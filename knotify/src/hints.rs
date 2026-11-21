//! Loss-tolerant path hints. The notify callback never blocks on stdout or BEAM.
use crate::{
    native,
    wire::{self, Value},
};
use notify::{
    Event, EventKind, RecursiveMode, Watcher,
    event::{MetadataKind, ModifyKind},
};
use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

const CAPACITY: usize = 4096;
const WINDOW: usize = 4;
enum Input {
    Credit(usize),
    Closed,
}
pub fn run(
    root: PathBuf,
    recursive: bool,
    debounce: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = std::fs::canonicalize(root)?;
    let (tx, rx) = mpsc::sync_channel(CAPACITY);
    let overflow = Arc::new(AtomicBool::new(false));
    let lost = Arc::clone(&overflow);
    let mut watcher = notify::RecommendedWatcher::new(
        move |event: notify::Result<Event>| {
            if event.as_ref().is_ok_and(access_only) {
                return;
            }
            if tx.try_send(event).is_err() {
                lost.store(true, Ordering::Release);
            }
        },
        notify::Config::default().with_follow_symlinks(false),
    )?;
    let mut output = io::stdout().lock();
    if let Err(e) = watcher.watch(
        &root,
        if recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        },
    ) {
        wire::write(
            &mut output,
            &wire::message(
                "error",
                [
                    ("code", "registration_failed".into()),
                    ("message", e.to_string().into()),
                ],
            ),
        )?;
        return Err(e.into());
    }
    wire::write(
        &mut output,
        &wire::message("ready", [("service", "watcher".into())]),
    )?;
    let (input_tx, input_rx) = mpsc::sync_channel(WINDOW);
    thread::spawn(move || {
        let mut input = io::stdin().lock();
        loop {
            match wire::read(&mut input) {
                Ok(Some(v))
                    if matches!(v.get("version").and_then(Value::int), Ok(wire::VERSION))
                        && v.get("type")
                            .and_then(Value::bytes)
                            .is_ok_and(|t| t == b"credit") =>
                {
                    match v.get("count").and_then(Value::int) {
                        Ok(n) if n > 0 && n <= WINDOW as i64 => {
                            if input_tx.send(Input::Credit(n as usize)).is_err() {
                                return;
                            }
                        }
                        _ => {
                            let _ = input_tx.send(Input::Closed);
                            return;
                        }
                    }
                }
                _ => {
                    // EOF must stop even while stdout is blocked on a dead owner.
                    std::process::exit(0);
                }
            }
        }
    });
    let mut credits = 0;
    let mut paths = BTreeSet::new();
    // Directories that appeared under the root (yoonka/beamports#7). The backend
    // watches a new directory only after reporting it, so entries made in it
    // before then are never reported; a scoped rescan covers them.
    let mut new_dirs = BTreeSet::new();
    let mut new_dirs_watched = true;
    let mut rescan = false;
    let mut error: Option<String> = None;
    let mut error_paths = BTreeSet::new();
    let mut seq = 0i64;
    let mut last = Instant::now();
    loop {
        loop {
            match input_rx.try_recv() {
                Ok(Input::Credit(n)) => credits = (credits + n).min(WINDOW),
                Ok(Input::Closed) => return Err("invalid credit".into()),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }
        if overflow.swap(false, Ordering::AcqRel) {
            rescan = true;
            paths.clear();
        }
        for _ in 0..512 {
            match rx.try_recv() {
                Ok(Ok(event)) => {
                    if recursive && new_directories(&root, &event, &mut new_dirs) {
                        new_dirs_watched = false;
                    }
                    absorb(&root, event, &mut paths, &mut rescan);
                }
                Ok(Err(e)) => {
                    error = Some(e.to_string());
                    if e.paths.is_empty() {
                        rescan = true;
                    }
                    for path in e.paths {
                        match path
                            .strip_prefix(&root)
                            .ok()
                            .and_then(|p| native::relative(p).ok())
                        {
                            Some(path) => {
                                error_paths.insert(path);
                            }
                            None => rescan = true,
                        }
                    }
                    if error_paths.len() > CAPACITY {
                        error_paths.clear();
                        rescan = true;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err("watcher disconnected".into()),
            }
        }
        if new_dirs.len() > CAPACITY {
            new_dirs.clear();
            rescan = true;
        }
        if !new_dirs_watched {
            // The backend adds the watch of a new directory on its own thread,
            // after it delivers the directory's event. That thread answers a
            // request only between event batches, so once this returns the new
            // directories are watched and a scan of them misses nothing.
            let _ = watcher.watched_paths();
            new_dirs_watched = true;
        }
        if credits > 0
            && (rescan
                || !error_paths.is_empty()
                || paths.len() >= 128
                || new_dirs.len() >= 128
                || last.elapsed() >= Duration::from_millis(debounce.max(10)))
        {
            let value = if rescan {
                paths.clear();
                error_paths.clear();
                new_dirs.clear();
                rescan = false;
                Some(wire::message(
                    "rescan",
                    [
                        ("batch_id", seq.into()),
                        (
                            "reason",
                            error
                                .take()
                                .unwrap_or_else(|| "events_may_be_lost".into())
                                .into(),
                        ),
                    ],
                ))
            } else if !error_paths.is_empty() {
                let batch = take_batch(&mut error_paths);
                Some(wire::message(
                    "rescan",
                    [
                        ("batch_id", seq.into()),
                        (
                            "reason",
                            error.clone().unwrap_or_else(|| "watch_error".into()).into(),
                        ),
                        ("paths", Value::List(batch)),
                    ],
                ))
            } else if !new_dirs.is_empty() {
                let batch = take_batch(&mut new_dirs);
                Some(wire::message(
                    "rescan",
                    [
                        ("batch_id", seq.into()),
                        ("reason", "new_directory".into()),
                        ("paths", Value::List(batch)),
                    ],
                ))
            } else if !paths.is_empty() {
                let batch = take_batch(&mut paths);
                Some(wire::message(
                    "paths",
                    [("batch_id", seq.into()), ("paths", Value::List(batch))],
                ))
            } else {
                None
            };
            if let Some(value) = value {
                wire::write(&mut output, &value)?;
                credits -= 1;
                seq += 1;
            }
            last = Instant::now();
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Takes the paths of one message: at most 128 of them and 256 KiB.
fn take_batch(set: &mut BTreeSet<Vec<u8>>) -> Vec<Value> {
    let mut batch = vec![];
    let mut size = 0;
    while let Some(path) = set.pop_first() {
        if size + path.len() > 256 * 1024 && !batch.is_empty() {
            set.insert(path);
            break;
        }
        size += path.len();
        batch.push(Value::from(path));
        if batch.len() == 128 {
            break;
        }
    }
    batch
}

/// Records the directories that an event creates or moves under the root, and
/// returns whether it recorded any. A rename's old path no longer exists, so
/// only its new path can be a directory.
fn new_directories(root: &Path, event: &Event, new_dirs: &mut BTreeSet<Vec<u8>>) -> bool {
    if !matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(ModifyKind::Name(_))
    ) {
        return false;
    }
    let mut added = false;
    for path in &event.paths {
        if !path.symlink_metadata().is_ok_and(|m| m.is_dir()) {
            continue;
        }
        if let Some(dir) = path
            .strip_prefix(root)
            .ok()
            .and_then(|p| native::relative(p).ok())
            .filter(|dir| !dir.is_empty())
        {
            added |= insert_dir(new_dirs, dir);
        }
    }
    added
}

/// Adds a directory unless one of its ancestors is pending, whose rescan covers
/// it, and drops its pending descendants.
fn insert_dir(dirs: &mut BTreeSet<Vec<u8>>, dir: Vec<u8>) -> bool {
    let covered = dirs.contains(&dir)
        || dir
            .iter()
            .enumerate()
            .any(|(i, &b)| b == b'/' && dirs.contains(&dir[..i]));
    if covered {
        return false;
    }
    let mut prefix = dir.clone();
    prefix.push(b'/');
    let descendants: Vec<_> = dirs
        .range(prefix.clone()..)
        .take_while(|d| d.starts_with(&prefix))
        .cloned()
        .collect();
    for d in descendants {
        dirs.remove(&d);
    }
    dirs.insert(dir);
    true
}

fn access_only(event: &Event) -> bool {
    !event.need_rescan()
        && matches!(
            event.kind,
            EventKind::Access(_)
                | EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime))
        )
}

fn absorb(root: &std::path::Path, event: Event, paths: &mut BTreeSet<Vec<u8>>, rescan: &mut bool) {
    if event.need_rescan()
        || (event.paths.is_empty() && matches!(event.kind, EventKind::Any | EventKind::Other))
    {
        *rescan = true;
    }
    if matches!(
        event.kind,
        EventKind::Access(_) | EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime))
    ) {
        return;
    }
    for path in event.paths {
        match path
            .strip_prefix(root)
            .ok()
            .and_then(|p| native::relative(p).ok())
        {
            Some(path) => {
                paths.insert(path);
            }
            None => *rescan = true,
        }
    }
    if paths.len() > CAPACITY {
        paths.clear();
        *rescan = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, CreateKind, Flag, RemoveKind};
    #[test]
    fn unscoped_ambiguous_events_require_a_rescan() {
        let root = PathBuf::from("root");
        for kind in [EventKind::Any, EventKind::Other] {
            let mut paths = BTreeSet::new();
            let mut rescan = false;
            absorb(&root, Event::new(kind), &mut paths, &mut rescan);
            assert!(rescan);
            assert!(paths.is_empty());
        }
        let mut paths = BTreeSet::new();
        let mut rescan = false;
        absorb(
            &root,
            Event::new(EventKind::Any).add_path(root.join("known")),
            &mut paths,
            &mut rescan,
        );
        assert!(!rescan);
        assert!(paths.contains(b"known".as_slice()));
    }
    #[test]
    fn recreate_is_retained_and_access_rescan_is_never_filtered() {
        let root = PathBuf::from("root");
        let mut paths = BTreeSet::new();
        let mut rescan = false;
        absorb(
            &root,
            Event::new(EventKind::Remove(RemoveKind::File)).add_path(root.join("a")),
            &mut paths,
            &mut rescan,
        );
        absorb(
            &root,
            Event::new(EventKind::Create(CreateKind::File)).add_path(root.join("a")),
            &mut paths,
            &mut rescan,
        );
        assert!(paths.contains(b"a".as_slice()));
        absorb(
            &root,
            Event::new(EventKind::Access(AccessKind::Any)).set_flag(Flag::Rescan),
            &mut paths,
            &mut rescan,
        );
        assert!(rescan);
    }
    #[test]
    fn coalescing_overflow_requires_root_reconciliation() {
        let root = PathBuf::from("root");
        let mut paths = BTreeSet::new();
        let mut rescan = false;
        for i in 0..=CAPACITY {
            absorb(
                &root,
                Event::new(EventKind::Create(CreateKind::File)).add_path(root.join(i.to_string())),
                &mut paths,
                &mut rescan,
            );
        }
        assert!(rescan);
        assert!(paths.len() <= CAPACITY);
    }
    #[test]
    fn new_directories_are_rescanned_once_per_subtree() {
        let mut dirs = BTreeSet::new();
        assert!(insert_dir(&mut dirs, b"a/b".to_vec()));
        assert!(insert_dir(&mut dirs, b"a/bc".to_vec()));
        assert!(insert_dir(&mut dirs, b"a".to_vec()));
        assert_eq!(dirs, BTreeSet::from([b"a".to_vec()]));
        assert!(!insert_dir(&mut dirs, b"a/b/c".to_vec()));
        assert!(!insert_dir(&mut dirs, b"a".to_vec()));
        assert!(insert_dir(&mut dirs, b"ab".to_vec()));
        assert_eq!(dirs.len(), 2);
    }
    #[test]
    fn created_and_moved_in_directories_need_a_rescan_but_files_do_not() {
        use notify::event::{ModifyKind, RenameMode};
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        std::fs::create_dir(root.join("made")).unwrap();
        std::fs::create_dir(root.join("moved")).unwrap();
        std::fs::write(root.join("file"), "1").unwrap();
        let mut dirs = BTreeSet::new();
        let created = Event::new(EventKind::Create(CreateKind::Any)).add_path(root.join("made"));
        assert!(new_directories(root, &created, &mut dirs));
        let renamed = Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(root.join("gone"))
            .add_path(root.join("moved"));
        assert!(new_directories(root, &renamed, &mut dirs));
        let file = Event::new(EventKind::Create(CreateKind::Any)).add_path(root.join("file"));
        assert!(!new_directories(root, &file, &mut dirs));
        let modified = Event::new(EventKind::Modify(ModifyKind::Any)).add_path(root.join("made"));
        assert!(!new_directories(root, &modified, &mut dirs));
        assert_eq!(dirs, BTreeSet::from([b"made".to_vec(), b"moved".to_vec()]));
    }
    #[test]
    fn access_time_noise_is_filtered_before_buffering_but_rescan_is_kept() {
        let event = Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::AccessTime,
        )));
        assert!(access_only(&event));
        assert!(!access_only(&event.set_flag(Flag::Rescan)));
        assert!(!access_only(&Event::new(EventKind::Modify(
            ModifyKind::Metadata(MetadataKind::WriteTime)
        ))));
    }
}

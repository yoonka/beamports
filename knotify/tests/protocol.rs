#[path = "../src/wire.rs"]
#[allow(dead_code)]
mod wire;
use std::{
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
#[test]
fn hint_service_reports_readiness_and_exits_when_its_owner_closes() {
    let root = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_knotify"))
        .args(["--protocol", "etf", "--recursive", "--path"])
        .arg(root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let ready = wire::read(child.stdout.as_mut().unwrap()).unwrap().unwrap();
        assert_eq!(ready.get("type").unwrap().bytes().unwrap(), b"ready");
        child.stdin.take();
        let started = Instant::now();
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "watcher did not exit on EOF"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }));
    let _ = child.kill();
    let _ = child.wait();
    guarded.unwrap();
}

/// Writes a directory and entries in it at once, as `cp -r` or `mkdir -p`
/// followed by writes do: the backend watches a new directory only after it
/// reports it, so these entries are made before their directory is watched
/// (yoonka/beamports#7).
fn make_tree(root: &std::path::Path) {
    let dir = root.join("dir");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("y.bin"), "2").unwrap();
    std::fs::create_dir(dir.join("sub")).unwrap();
    std::fs::write(dir.join("sub").join("z.bin"), "3").unwrap();
}

const TREE: [&[u8]; 2] = [b"dir/y.bin", b"dir/sub/z.bin"];

#[test]
fn hint_service_covers_entries_made_before_their_directory_is_watched() {
    for _ in 0..10 {
        let root = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_knotify"))
            .args(["--protocol", "etf", "--recursive", "--path"])
            .arg(root.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut stdout = child.stdout.take().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            thread::spawn(move || {
                while let Ok(Some(message)) = wire::read(&mut stdout) {
                    if tx.send(message).is_err() {
                        break;
                    }
                }
            });
            let ready = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(ready.get("type").unwrap().bytes().unwrap(), b"ready");
            let mut stdin = child.stdin.take().unwrap();
            let credit = |stdin: &mut std::process::ChildStdin, count: i64| {
                let message = wire::message("credit", [("count", count.into())]);
                wire::write(stdin, &message).unwrap();
            };
            credit(&mut stdin, 4);
            make_tree(root.path());
            // An entry is covered by its own path, by a rescan of the whole
            // root, or by a rescan of a directory that contains it.
            let mut covered = [false; TREE.len()];
            let started = Instant::now();
            while !covered.iter().all(|c| *c) {
                let left = Duration::from_secs(5).saturating_sub(started.elapsed());
                let Ok(message) = rx.recv_timeout(left) else {
                    break;
                };
                let kind = message.get("type").unwrap().bytes().unwrap().to_vec();
                let paths: Vec<Vec<u8>> = match message.get("paths") {
                    Ok(wire::Value::List(paths)) => {
                        paths.iter().map(|p| p.bytes().unwrap().to_vec()).collect()
                    }
                    _ => vec![],
                };
                for (entry, covered) in TREE.iter().zip(covered.iter_mut()) {
                    *covered |= match kind.as_slice() {
                        b"paths" => paths.iter().any(|p| p == entry),
                        b"rescan" => {
                            paths.is_empty()
                                || paths.iter().any(|p| {
                                    entry.starts_with(p) && entry.get(p.len()) == Some(&b'/')
                                })
                        }
                        _ => false,
                    };
                }
                credit(&mut stdin, 1);
            }
            assert!(
                covered.iter().all(|c| *c),
                "entries not covered: {:?}",
                TREE.iter()
                    .zip(covered)
                    .filter(|(_, c)| !c)
                    .map(|(e, _)| String::from_utf8_lossy(e))
                    .collect::<Vec<_>>()
            );
        }));
        let _ = child.kill();
        let _ = child.wait();
        guarded.unwrap();
    }
}

#[test]
fn json_events_cover_entries_made_before_their_directory_is_watched() {
    use std::io::BufRead;
    for _ in 0..10 {
        let root = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_knotify"))
            .args(["--recursive", "--path"])
            .arg(root.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let stdout = std::io::BufReader::new(child.stdout.take().unwrap());
            let (tx, rx) = std::sync::mpsc::channel();
            thread::spawn(move || {
                for line in stdout.lines() {
                    let Ok(line) = line else { break };
                    let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
                        continue;
                    };
                    if tx.send(event).is_err() {
                        break;
                    }
                }
            });
            // JSON mode has no ready message: wait until a probe is reported.
            let reported = |event: &serde_json::Value, suffix: &str| {
                event["paths"].as_array().is_some_and(|paths| {
                    paths.iter().any(|p| {
                        p.as_str()
                            .is_some_and(|p| p.replace('\\', "/").ends_with(suffix))
                    })
                })
            };
            let started = Instant::now();
            'ready: loop {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "watcher never reported the probe"
                );
                std::fs::write(root.path().join("probe"), "1").unwrap();
                while let Ok(event) = rx.recv_timeout(Duration::from_millis(100)) {
                    if reported(&event, "/probe") {
                        break 'ready;
                    }
                }
            }
            make_tree(root.path());
            let mut created = [false; TREE.len()];
            let started = Instant::now();
            while !created.iter().all(|c| *c) {
                let left = Duration::from_secs(5).saturating_sub(started.elapsed());
                let Ok(event) = rx.recv_timeout(left) else {
                    break;
                };
                if event["kind"] != "CREATE_FILE" && event["kind"] != "CREATE" {
                    continue;
                }
                for (entry, created) in TREE.iter().zip(created.iter_mut()) {
                    let suffix = format!("/{}", String::from_utf8_lossy(entry));
                    *created |= reported(&event, &suffix);
                }
            }
            assert!(
                created.iter().all(|c| *c),
                "entries not reported: {:?}",
                TREE.iter()
                    .zip(created)
                    .filter(|(_, c)| !c)
                    .map(|(e, _)| String::from_utf8_lossy(e))
                    .collect::<Vec<_>>()
            );
        }));
        let _ = child.kill();
        let _ = child.wait();
        guarded.unwrap();
    }
}

//! Headless CLI for the BOINC-quota consensus network.
//!
//! Same engine as the desktop app (NetworkActor + AppCommand/AppEvent), no GUI.
//! Primary use: scripted self-testing of the coordinator/worker consensus, and
//! running nodes on servers where the desktop app can't run.
//!
//! Each local instance MUST use a distinct identity, else they share
//! ~/.boinc-quota/identity.json -> same key -> same account -> the validator set
//! collapses and nodes never form a real multi-node network. Pass --identity.
//!
//! Usage:
//!   cli coordinator --listen 127.0.0.1:7878 [--peers a,b] [--name N] [--balance N] [--identity PATH]
//!   cli worker --coord 127.0.0.1:7878 [--name N] [--balance N] [--exec] [--identity PATH]
//!
//! Then type commands on stdin (one per line):
//!   project <name>            create a project (owner = this node)
//!   fund <pid> <amount>       move own balance into a project's quota
//!   donate <pid> <amount>     donate balance into a project's quota
//!   task <pid> <reward> <code...>   submit a python task
//!   send <to_addr> <amount>   transfer tokens
//!   exec start | exec stop    start/stop the executor
//!   state                     print balances / projects / tasks
//!   sleep <ms>                pause the script (lets consensus settle)
//!   quit                      disconnect and exit

use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Must match the executor's decoder (URL-safe, no padding), see executor.rs.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine as _};
use tokio::sync::mpsc;

use boinc_quota_rs::actor::NetworkActor;
use boinc_quota_rs::protocol::{AppCommand, AppEvent, NetworkSnapshot};

fn arg_val(args: &[String], key: &str) -> Option<String> {
    args.iter().position(|a| a == key).and_then(|i| args.get(i + 1)).cloned()
}

fn has_flag(args: &[String], key: &str) -> bool {
    args.iter().any(|a| a == key)
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  cli coordinator --listen ADDR [--peers a,b] [--name N] [--balance N] [--identity PATH]\n  cli worker --coord ADDR [--name N] [--balance N] [--exec] [--identity PATH]\n\nstdin commands: project NAME | fund PID AMT | donate PID AMT |\n  task PID REWARD CODE... | send TO AMT | exec start|stop | state | sleep MS | quit"
    );
    std::process::exit(2);
}

fn print_state(snap: &NetworkSnapshot) {
    println!("--- state: blocks={} valid={} ---", snap.block_count, snap.blockchain_valid);
    for p in &snap.participants {
        println!("  participant #{:<20} {:<8} balance={} rep={}", p.id, p.name, p.balance, p.reputation);
    }
    for pr in &snap.projects {
        println!(
            "  project   #{} {:<10} quota avail={} locked={}",
            pr.id, pr.name, pr.quota_available, pr.quota_locked
        );
    }
    for t in &snap.tasks {
        println!("  task      #{} project={} reward={} [{}]", t.id, t.project_id, t.reward, t.status_label);
    }
    println!("------------------------------------");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let role = args.first().cloned().unwrap_or_default();

    if let Some(path) = arg_val(&args, "--identity") {
        std::env::set_var("BOINC_IDENTITY", path);
    }
    let name = arg_val(&args, "--name").unwrap_or_else(|| "cli".to_string());
    let balance: u64 = arg_val(&args, "--balance").and_then(|s| s.parse().ok()).unwrap_or(100);

    let connect = match role.as_str() {
        "coordinator" => AppCommand::ConnectCoordinator {
            listen_addr: arg_val(&args, "--listen").unwrap_or_else(|| "127.0.0.1:7878".to_string()),
            name: name.clone(),
            balance,
            quorum: 2,
            peer_coordinators: arg_val(&args, "--peers")
                .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
                .unwrap_or_default(),
        },
        "worker" => AppCommand::ConnectWorker {
            coord_addr: arg_val(&args, "--coord").unwrap_or_else(|| "127.0.0.1:7878".to_string()),
            name: name.clone(),
            balance,
        },
        _ => usage(),
    };

    // Point the embedded interpreter at the bundled stdlib before any Python use.
    boinc_quota_rs::pyenv::prepare_embedded_python();

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    let (cmd_tx, cmd_rx) = mpsc::channel::<AppCommand>(64);
    let (evt_tx, mut evt_rx) = mpsc::channel::<AppEvent>(512);
    rt.spawn(NetworkActor::new(cmd_rx, evt_tx).run());

    // Latest replicated snapshot, updated from events; printed on `state`.
    let snapshot = Arc::new(Mutex::new(NetworkSnapshot::default()));
    {
        let snap = Arc::clone(&snapshot);
        rt.spawn(async move {
            while let Some(evt) = evt_rx.recv().await {
                match evt {
                    AppEvent::Log(m) => println!("[log] {m}"),
                    AppEvent::Error(m) => println!("[err] {m}"),
                    AppEvent::Connected => println!("[evt] connected"),
                    AppEvent::Disconnected { reason } => println!("[evt] disconnected: {reason}"),
                    AppEvent::Registered { participant_id } => println!("[evt] registered as #{participant_id}"),
                    AppEvent::Identity { signing_short, .. } => println!("[evt] identity {signing_short}"),
                    AppEvent::ExecutorStarted => println!("[evt] executor started"),
                    AppEvent::ExecutorStopped => println!("[evt] executor stopped"),
                    AppEvent::StateUpdate(s) => *snap.lock().unwrap() = s,
                    AppEvent::P2pUpdate(_) => {}
                }
            }
        });
    }

    let _ = cmd_tx.blocking_send(connect);
    if has_flag(&args, "--exec") {
        let _ = cmd_tx.blocking_send(AppCommand::StartExecutor {
            reliability: 95,
            compute_ticks: 2,
            allowed_packages: boinc_quota_rs::sandbox::default_allowed_packages(),
        });
    }

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        match p[0] {
            "project" => {
                let nm = p.get(1).copied().unwrap_or("P").to_string();
                let _ = cmd_tx.blocking_send(AppCommand::CreateProject {
                    name: nm,
                    owner_encryption_pubkey: None,
                });
            }
            "fund" => {
                if let (Some(pid), Some(amt)) = (parse(&p, 1), parse(&p, 2)) {
                    let _ = cmd_tx.blocking_send(AppCommand::FundProject { project_id: pid, amount: amt });
                }
            }
            "donate" => {
                if let (Some(pid), Some(amt)) = (parse(&p, 1), parse(&p, 2)) {
                    let _ = cmd_tx.blocking_send(AppCommand::DonateToProject { project_id: pid, amount: amt });
                }
            }
            "task" => {
                if let (Some(pid), Some(reward)) = (parse(&p, 1), parse(&p, 2)) {
                    let code = p.get(3).copied().unwrap_or("print('hello')");
                    let payload = format!("python:{}", BASE64.encode(code));
                    let _ = cmd_tx.blocking_send(AppCommand::SubmitTask { project_id: pid, reward, payload });
                }
            }
            "send" => {
                if let (Some(to), Some(amt)) = (parse(&p, 1), parse(&p, 2)) {
                    let _ = cmd_tx.blocking_send(AppCommand::SendTokens { to, amount: amt });
                }
            }
            "exec" => match p.get(1).copied() {
                Some("start") => {
                    let _ = cmd_tx.blocking_send(AppCommand::StartExecutor {
                        reliability: 95,
                        compute_ticks: 2,
                        allowed_packages: boinc_quota_rs::sandbox::default_allowed_packages(),
                    });
                }
                Some("stop") => {
                    let _ = cmd_tx.blocking_send(AppCommand::StopExecutor);
                }
                _ => println!("usage: exec start|stop"),
            },
            "state" => print_state(&snapshot.lock().unwrap()),
            "sleep" => {
                if let Some(ms) = parse::<u64>(&p, 1) {
                    std::thread::sleep(Duration::from_millis(ms));
                }
            }
            "quit" | "exit" => {
                let _ = cmd_tx.blocking_send(AppCommand::Disconnect);
                break;
            }
            other => println!("unknown command: {other}"),
        }
    }

    std::thread::sleep(Duration::from_millis(200));
}

fn parse<T: std::str::FromStr>(p: &[&str], i: usize) -> Option<T> {
    p.get(i).and_then(|s| s.parse().ok())
}

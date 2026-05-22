//! Consensus smoke test.
//!
//! Spins up N in-process P2P consensus nodes over loopback TCP, lets them form
//! a gossip mesh, submits a token transfer, and checks that every node
//! converges on an identical, valid chain with the expected balances.
//!
//! Run:  cargo run --bin consensus_smoke [N]   (default N = 4)
//! Exit: 0 = converged (PASS), 1 = did not converge (FAIL).

use std::time::Duration;

use tokio::sync::mpsc;

use boinc_quota_rs::actor::{node_address, p2p_node_with_identity};
use boinc_quota_rs::identity::Identity;
use boinc_quota_rs::protocol::{AppCommand, AppEvent, P2pSnapshot};

const INITIAL_BALANCE: u64 = 100;
const TRANSFER: u64 = 30;
const BASE_PORT: u16 = 18000;

struct Node {
    cmd: mpsc::Sender<AppCommand>,
    evt: mpsc::Receiver<AppEvent>,
    addr: u64,
}

/// Drain pending events, returning the most recent P2pUpdate snapshot.
fn latest(rx: &mut mpsc::Receiver<AppEvent>) -> Option<P2pSnapshot> {
    let mut last = None;
    while let Ok(evt) = rx.try_recv() {
        if let AppEvent::P2pUpdate(snap) = evt {
            last = Some(snap);
        }
    }
    last
}

fn head_hash(snap: &P2pSnapshot) -> &str {
    snap.blocks.last().map(|b| b.hash.as_str()).unwrap_or("-")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
        .max(2);

    println!("Spawning {n} consensus nodes on 127.0.0.1:{BASE_PORT}..{}", BASE_PORT as usize + n - 1);

    let mut nodes: Vec<Node> = Vec::new();
    for i in 0..n {
        let identity = Identity::generate();
        let addr = node_address(&identity.public_key_bytes());
        let listen = format!("127.0.0.1:{}", BASE_PORT + i as u16);
        // First node has no bootstrap; the rest dial the first node, then the
        // mesh fills in via peer-list gossip.
        let bootstrap = if i == 0 {
            vec![]
        } else {
            vec![format!("127.0.0.1:{BASE_PORT}")]
        };
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<AppCommand>(32);
        let (evt_tx, evt_rx) = mpsc::channel::<AppEvent>(512);
        let name = format!("node{i}");
        tokio::spawn(async move {
            p2p_node_with_identity(
                &mut cmd_rx,
                &evt_tx,
                identity,
                listen,
                bootstrap,
                name,
                INITIAL_BALANCE,
            )
            .await;
        });
        nodes.push(Node {
            cmd: cmd_tx,
            evt: evt_rx,
            addr,
        });
    }

    println!("Waiting for mesh + initial mints (3s)...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let to = nodes[1].addr;
    let _ = nodes[0]
        .cmd
        .send(AppCommand::SendTokens { to, amount: TRANSFER })
        .await;
    println!("Submitted transfer: node0 → node1  ({TRANSFER} tokens)\n");

    // Expected final balances: node0 = 70, node1 = 130, everyone else = 100.
    let mut snaps: Vec<P2pSnapshot> = vec![P2pSnapshot::default(); n];
    let mut converged = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        for (i, node) in nodes.iter_mut().enumerate() {
            if let Some(s) = latest(&mut node.evt) {
                snaps[i] = s;
            }
        }
        let h0 = snaps[0].block_count;
        let head0 = head_hash(&snaps[0]).to_string();
        let same_chain = h0 >= 2
            && snaps
                .iter()
                .all(|s| s.block_count == h0 && head_hash(s) == head0 && s.blockchain_valid);
        let validators_ok = snaps.iter().all(|s| s.validators == n);
        let balances_ok = snaps[0].my_balance == INITIAL_BALANCE - TRANSFER
            && snaps[1].my_balance == INITIAL_BALANCE + TRANSFER
            && snaps[2..].iter().all(|s| s.my_balance == INITIAL_BALANCE);
        if same_chain && validators_ok && balances_ok {
            converged = true;
            break;
        }
    }

    println!(
        "{:<8} {:>6} {:>6} {:>7} {:>8} {:>10} {:>6}  head",
        "node", "peers", "valid.", "blocks", "mempool", "balance", "ok?"
    );
    for (i, s) in snaps.iter().enumerate() {
        println!(
            "{:<8} {:>6} {:>6} {:>7} {:>8} {:>10} {:>6}  {}",
            format!("node{i}"),
            s.peers,
            s.validators,
            s.block_count,
            s.mempool,
            s.my_balance,
            if s.blockchain_valid { "✓" } else { "✗" },
            &head_hash(s)[..head_hash(s).len().min(16)],
        );
    }

    if converged {
        println!("\nSMOKE TEST PASSED: all nodes share one valid chain, balances correct.");
    } else {
        eprintln!(
            "\nSMOKE TEST FAILED: nodes did not converge (expected node0={}, node1={}, rest={}).",
            INITIAL_BALANCE - TRANSFER,
            INITIAL_BALANCE + TRANSFER,
            INITIAL_BALANCE
        );
        std::process::exit(1);
    }
}

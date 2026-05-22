use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use crate::blockchain::{NodeKey, Transaction};
use crate::consensus::{ConsensusEngine, ConsensusMsg};
use crate::executor::Executor;
use crate::identity::Identity;
use crate::model::ParticipantId;
use crate::network::Network;
use crate::ops::LedgerOp;
use crate::protocol::*;

type TcpWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, LinesCodec>;
type SharedWriter = Arc<Mutex<TcpWriter>>;

pub struct NetworkActor {
    cmd_rx: mpsc::Receiver<AppCommand>,
    evt_tx: mpsc::Sender<AppEvent>,
}

impl NetworkActor {
    pub fn new(cmd_rx: mpsc::Receiver<AppCommand>, evt_tx: mpsc::Sender<AppEvent>) -> Self {
        Self { cmd_rx, evt_tx }
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                AppCommand::ConnectCoordinator {
                    listen_addr,
                    name,
                    balance,
                    peer_coordinators,
                    ..
                } => {
                    // Coordinator: listens for workers/peer-coordinators and
                    // relays consensus gossip. Bootstrap = other coordinators.
                    let identity = resolve_identity("coordinator", &listen_addr, &name);
                    run_role_node(
                        NodeRole::Coordinator,
                        &mut self.cmd_rx,
                        &self.evt_tx,
                        identity,
                        Some(listen_addr),
                        peer_coordinators,
                        name,
                        balance,
                    )
                    .await;
                }
                AppCommand::ConnectWorker {
                    coord_addr,
                    name,
                    balance,
                } => {
                    // Worker: dials its coordinator; no listener (hub topology).
                    let identity = resolve_identity("worker", &coord_addr, &name);
                    run_role_node(
                        NodeRole::Worker,
                        &mut self.cmd_rx,
                        &self.evt_tx,
                        identity,
                        None,
                        vec![coord_addr],
                        name,
                        balance,
                    )
                    .await;
                }
                AppCommand::JoinP2P {
                    listen_addr,
                    bootstrap_peers,
                    name,
                    balance,
                } => {
                    run_p2p_node(
                        &mut self.cmd_rx,
                        &self.evt_tx,
                        listen_addr,
                        bootstrap_peers,
                        name,
                        balance,
                    )
                    .await;
                }
                _ => {}
            }
        }
    }
}


/// Auto-install missing allowlist packages into the venv before the executor
/// runs, logging each install/failure. Stdlib and already-present modules are
/// silently skipped.
async fn ensure_packages(packages: &[String], evt_tx: &mpsc::Sender<AppEvent>) {
    // First-run bootstrap: locate system Python, create the venv, install
    // smolagents. Cached, so this is cheap after the first executor start.
    let ready = tokio::task::spawn_blocking(crate::pyenv::ensure_ready)
        .await
        .unwrap_or_else(|e| Err(e.to_string()));
    if let Err(e) = ready {
        let _ = evt_tx
            .send(AppEvent::Log(format!("Python environment setup failed: {e}")))
            .await;
        return;
    }

    if packages.is_empty() {
        return;
    }
    let pkgs = packages.to_vec();
    let results = tokio::task::spawn_blocking(move || {
        crate::sandbox::ensure_packages_installed(&pkgs)
    })
    .await
    .unwrap_or_default();

    for (name, outcome) in results {
        let msg = match outcome {
            crate::sandbox::InstallOutcome::AlreadyPresent => continue,
            crate::sandbox::InstallOutcome::Installed => {
                format!("Installed package '{name}'")
            }
            crate::sandbox::InstallOutcome::Failed(e) => {
                format!("Failed to install '{name}': {e}")
            }
        };
        let _ = evt_tx.send(AppEvent::Log(msg)).await;
    }
}

pub fn build_snapshot(net: &Network) -> NetworkSnapshot {
    let mut participants: Vec<ParticipantView> = net
        .all_participants()
        .iter()
        .map(|p| ParticipantView {
            id: p.id,
            name: p.name.clone(),
            balance: net.balance_of(p.id),
            reputation: p.reputation,
        })
        .collect();
    participants.sort_by_key(|p| p.id);

    let mut projects: Vec<ProjectView> = net
        .all_projects()
        .iter()
        .map(|p| ProjectView {
            id: p.id,
            owner_id: p.owner_id,
            name: p.name.clone(),
            quota_available: p.quota_available,
            quota_locked: p.quota_locked,
        })
        .collect();
    projects.sort_by_key(|p| p.id);

    let mut tasks: Vec<TaskView> = net
        .all_tasks()
        .iter()
        .map(|t| {
            let (status_label, assigned_worker_id) = match &t.status {
                crate::model::TaskStatus::Pending => ("Pending".to_string(), None),
                crate::model::TaskStatus::Assigned { worker_id, .. } => {
                    (format!("Assigned({worker_id})"), Some(*worker_id))
                }
                crate::model::TaskStatus::Completed { .. } => ("Done".to_string(), None),
                crate::model::TaskStatus::QuotaExhausted { .. } => {
                    ("Quota exhausted".to_string(), None)
                }
                crate::model::TaskStatus::Rejected => ("Rejected".to_string(), None),
            };
            TaskView {
                id: t.id,
                project_id: t.project_id,
                reward: t.reward,
                payload: t.payload.clone(),
                status_label,
                assigned_worker_id,
                reported_worker_ids: t.reports.iter().map(|report| report.worker_id).collect(),
                has_encrypted_result: t.encrypted_result.is_some(),
                encrypted_result: t.encrypted_result.clone(),
            }
        })
        .collect();
    tasks.sort_by_key(|t| t.id);

    let blocks: Vec<BlockView> = net
        .blockchain()
        .blocks()
        .iter()
        .map(|b| BlockView {
            index: b.index,
            tick: b.tick,
            hash: hex::encode(b.hash),
            prev_hash: hex::encode(b.prev_hash),
            transactions: b.transactions.iter().map(transaction_view).collect(),
        })
        .collect();

    NetworkSnapshot {
        participants,
        projects,
        tasks,
        blocks,
        block_count: net.blockchain().block_count(),
        blockchain_valid: net.blockchain().verify_integrity(),
    }
}

fn transaction_view(tx: &Transaction) -> TransactionView {
    match tx {
        Transaction::Mint { to, amount } => TransactionView {
            kind: "mint".to_string(),
            from: None,
            to: Some(*to),
            amount: *amount,
            memo: None,
        },
        Transaction::Transfer {
            from,
            to,
            amount,
            memo,
        } => TransactionView {
            kind: "transfer".to_string(),
            from: Some(*from),
            to: Some(*to),
            amount: *amount,
            memo: Some(memo.clone()),
        },
        Transaction::Burn { from, amount } => TransactionView {
            kind: "burn".to_string(),
            from: Some(*from),
            to: None,
            amount: *amount,
            memo: None,
        },
    }
}

// ── P2P consensus mesh ──────────────────────────────────────────────────────────

/// Live writers to connected peers, keyed by their advertised listen address.
type PeerMap = Arc<Mutex<HashMap<String, SharedWriter>>>;
/// Hashes of consensus messages already processed, to break gossip loops.
type SeenSet = Arc<Mutex<HashSet<u64>>>;

/// Derive a u64 ledger account address from a node's Ed25519 public key.
pub fn node_address(key: &NodeKey) -> ParticipantId {
    u64::from_le_bytes(key[..8].try_into().unwrap())
}

fn msg_hash(msg: &ConsensusMsg) -> u64 {
    let mut h = DefaultHasher::new();
    serde_json::to_string(msg).unwrap_or_default().hash(&mut h);
    h.finish()
}

async fn send_p2p(writer: &SharedWriter, msg: &P2pMessage) {
    if let Ok(line) = serde_json::to_string(msg) {
        let mut w = writer.lock().await;
        let _ = w.send(line).await;
    }
}

/// Send one envelope to every peer except `exclude` (the sender we relay from).
async fn flood_envelope(peers: &PeerMap, exclude: Option<&str>, env: &P2pMessage) {
    let line = match serde_json::to_string(env) {
        Ok(l) => l,
        Err(_) => return,
    };
    let map = peers.lock().await;
    for (addr, writer) in map.iter() {
        if Some(addr.as_str()) == exclude {
            continue;
        }
        let mut w = writer.lock().await;
        let _ = w.send(line.clone()).await;
    }
}

/// Flood consensus messages this node originated; record their hashes as seen so
/// they are ignored when peers relay them back.
async fn flood_consensus(
    peers: &PeerMap,
    seen: &SeenSet,
    exclude: Option<&str>,
    msgs: Vec<ConsensusMsg>,
) {
    for msg in msgs {
        seen.lock().await.insert(msg_hash(&msg));
        flood_envelope(peers, exclude, &P2pMessage::Consensus(msg)).await;
    }
}

fn block_to_view(b: &crate::blockchain::Block) -> BlockView {
    BlockView {
        index: b.index,
        tick: b.tick,
        hash: hex::encode(b.hash),
        prev_hash: hex::encode(b.prev_hash),
        transactions: b.transactions.iter().map(transaction_view).collect(),
    }
}

async fn build_p2p_snapshot(
    engine: &Arc<Mutex<ConsensusEngine>>,
    peers: &PeerMap,
    my_addr: ParticipantId,
    node_key_short: &str,
) -> P2pSnapshot {
    let peer_count = peers.lock().await.len();
    let e = engine.lock().await;
    // Render the replica's internal economic ledger (token transactions) for the
    // UI; the consensus op-log drives it but the economy view stays the same.
    let econ = e.state().blockchain();
    P2pSnapshot {
        node_key_short: node_key_short.to_string(),
        peers: peer_count,
        validators: e.validator_count(),
        mempool: e.mempool().len(),
        block_count: e.block_count(),
        blockchain_valid: econ.verify_integrity(),
        my_balance: econ.balance_of(my_addr),
        blocks: econ.blocks().iter().map(block_to_view).collect(),
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_dial(
    addr: String,
    my_key: NodeKey,
    my_listen: String,
    my_name: String,
    engine: Arc<Mutex<ConsensusEngine>>,
    peers: PeerMap,
    seen: SeenSet,
    evt_tx: mpsc::Sender<AppEvent>,
) {
    tokio::spawn(async move {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                let _ = evt_tx
                    .send(AppEvent::Log(format!("Dialed peer {addr}")))
                    .await;
                p2p_connection(stream, my_key, my_listen, my_name, engine, peers, seen, evt_tx)
                    .await;
            }
            Err(e) => {
                let _ = evt_tx
                    .send(AppEvent::Log(format!("Dial {addr} failed: {e}")))
                    .await;
            }
        }
    });
}

/// Drive one peer connection (inbound-accepted or outbound-dialed, symmetric).
/// Peers are keyed by node pubkey (unique), so multiple workers that don't run
/// a listener never collide. There is no peer auto-discovery: the topology is
/// exactly the configured links (workers→coordinator, coordinator↔coordinator),
/// and the coordinator relays gossip between its links — the hub model.
#[allow(clippy::too_many_arguments)]
async fn p2p_connection(
    stream: TcpStream,
    my_key: NodeKey,
    my_listen: String,
    my_name: String,
    engine: Arc<Mutex<ConsensusEngine>>,
    peers: PeerMap,
    seen: SeenSet,
    evt_tx: mpsc::Sender<AppEvent>,
) {
    let (read_half, write_half) = stream.into_split();
    let mut reader = FramedRead::new(read_half, LinesCodec::new());
    let writer = Arc::new(Mutex::new(FramedWrite::new(write_half, LinesCodec::new())));

    // Announce ourselves immediately.
    send_p2p(
        &writer,
        &P2pMessage::Hello {
            node_key: my_key,
            listen_addr: my_listen.clone(),
            name: my_name.clone(),
        },
    )
    .await;

    let mut peer_key: Option<String> = None;

    while let Some(line) = reader.next().await {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let msg: P2pMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(_) => continue,
        };

        match msg {
            P2pMessage::Hello { node_key, .. } => {
                let key = hex::encode(node_key);
                peer_key = Some(key.clone());
                peers.lock().await.insert(key, Arc::clone(&writer));

                // Learn the new peer, and tell it every validator we already know
                // so it converges on the full set without needing direct links to
                // everyone (hub topology). Then propagate the new peer's key to
                // our other links so they learn it too.
                let known = {
                    let mut e = engine.lock().await;
                    e.add_validator(node_key);
                    e.validators()
                };
                for v in known {
                    send_p2p(&writer, &P2pMessage::Validator(v)).await;
                }
                flood_envelope(&peers, peer_key.as_deref(), &P2pMessage::Validator(node_key)).await;

                // Bring this peer up to date, and hand over our pending ops so
                // every node's mempool converges (a height's fixed proposer must
                // hold the ops to make progress).
                let (from, pending) = {
                    let e = engine.lock().await;
                    (e.next_index(), e.mempool().to_vec())
                };
                send_p2p(
                    &writer,
                    &P2pMessage::Consensus(ConsensusMsg::SyncRequest { from_height: from }),
                )
                .await;
                for op in pending {
                    send_p2p(&writer, &P2pMessage::Consensus(ConsensusMsg::Op(op))).await;
                }
            }
            P2pMessage::Validator(key) => {
                // Dedup membership gossip by the key bytes, then relay onward.
                let mut h = DefaultHasher::new();
                key.hash(&mut h);
                let fresh = seen.lock().await.insert(h.finish());
                if fresh {
                    engine.lock().await.add_validator(key);
                    flood_envelope(&peers, peer_key.as_deref(), &P2pMessage::Validator(key)).await;
                }
            }
            P2pMessage::Peers(_) => { /* no auto-discovery: explicit links only */ }
            P2pMessage::Consensus(cmsg) => {
                let dedup = matches!(
                    cmsg,
                    ConsensusMsg::Op(_)
                        | ConsensusMsg::Propose(_)
                        | ConsensusMsg::Vote { .. }
                        | ConsensusMsg::Committed(_)
                );
                if dedup {
                    let fresh = seen.lock().await.insert(msg_hash(&cmsg));
                    if !fresh {
                        continue;
                    }
                    // Relay onward (flood) to our other links, skipping the sender.
                    flood_envelope(
                        &peers,
                        peer_key.as_deref(),
                        &P2pMessage::Consensus(cmsg.clone()),
                    )
                    .await;
                }

                let produced = engine.lock().await.on_message(cmsg);
                for m in produced {
                    match m {
                        ConsensusMsg::SyncResponse { .. } => {
                            send_p2p(&writer, &P2pMessage::Consensus(m)).await;
                        }
                        other => {
                            flood_consensus(&peers, &seen, None, vec![other]).await;
                        }
                    }
                }
            }
        }
    }

    if let Some(key) = peer_key {
        peers.lock().await.remove(&key);
    }
    let _ = evt_tx
        .send(AppEvent::Log("Peer connection closed".to_string()))
        .await;
}

async fn run_p2p_node(
    cmd_rx: &mut mpsc::Receiver<AppCommand>,
    evt_tx: &mpsc::Sender<AppEvent>,
    listen_addr: String,
    bootstrap_peers: Vec<String>,
    name: String,
    balance: u64,
) {
    let identity = Identity::load_or_generate(&Identity::default_path());
    p2p_node_with_identity(
        cmd_rx,
        evt_tx,
        identity,
        listen_addr,
        bootstrap_peers,
        name,
        balance,
    )
    .await;
}

/// Run a P2P consensus node with an explicit identity. Used by the dispatcher
/// (which supplies the on-disk identity) and by multi-node smoke tests / the
/// `consensus_smoke` binary (which inject distinct generated identities).
#[allow(clippy::too_many_arguments)]
pub async fn p2p_node_with_identity(
    cmd_rx: &mut mpsc::Receiver<AppCommand>,
    evt_tx: &mpsc::Sender<AppEvent>,
    identity: Identity,
    listen_addr: String,
    bootstrap_peers: Vec<String>,
    name: String,
    balance: u64,
) {
    let my_key = identity.public_key_bytes();
    let my_short = identity.public_key_short();
    let my_addr = node_address(&my_key);

    let listener = match TcpListener::bind(&listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            let _ = evt_tx
                .send(AppEvent::Error(format!("Bind {listen_addr}: {e}")))
                .await;
            return;
        }
    };
    let _ = evt_tx
        .send(AppEvent::Log(format!("P2P node listening on {listen_addr}")))
        .await;
    let _ = evt_tx
        .send(AppEvent::Log(format!("Identity: {my_short}")))
        .await;

    let engine = Arc::new(Mutex::new(ConsensusEngine::new(identity, [])));
    let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
    let seen: SeenSet = Arc::new(Mutex::new(HashSet::new()));

    let _ = evt_tx.send(AppEvent::Connected).await;
    let _ = evt_tx
        .send(AppEvent::Registered {
            participant_id: my_addr,
        })
        .await;

    // Register ourselves (and mint our initial balance) into the replicated
    // ledger via a consensus operation.
    {
        let out = engine
            .lock()
            .await
            .submit_local_op(LedgerOp::RegisterParticipant {
                id: my_addr,
                name: name.clone(),
                public_key: my_key,
                initial_balance: balance,
            });
        flood_consensus(&peers, &seen, None, out).await;
    }

    // Dial bootstrap peers to bootstrap the mesh.
    for addr in bootstrap_peers {
        if addr.trim().is_empty() {
            continue;
        }
        spawn_dial(
            addr.trim().to_string(),
            my_key,
            listen_addr.clone(),
            name.clone(),
            Arc::clone(&engine),
            Arc::clone(&peers),
            Arc::clone(&seen),
            evt_tx.clone(),
        );
    }

    let mut tick: u64 = 0;
    let mut round = tokio::time::interval(Duration::from_millis(500));
    let mut sync_timer = tokio::time::interval(Duration::from_secs(3));
    // Grace period before we start proposing: lets the mesh form and every node
    // converge on the same validator set, so the deterministic round-robin
    // proposer is agreed by all. Proposing earlier risks two nodes (with
    // partial validator views) sealing competing blocks at the same height.
    let started = std::time::Instant::now();
    const PROPOSE_GRACE: Duration = Duration::from_millis(2500);

    loop {
        tokio::select! {
            _ = round.tick() => {
                tick += 1;
                let _ = tick;
                let out = {
                    let mut e = engine.lock().await;
                    if started.elapsed() >= PROPOSE_GRACE {
                        e.try_propose()
                    } else {
                        vec![]
                    }
                };
                flood_consensus(&peers, &seen, None, out).await;
                let snap = build_p2p_snapshot(&engine, &peers, my_addr, &my_short).await;
                let _ = evt_tx.send(AppEvent::P2pUpdate(snap)).await;
            }
            _ = sync_timer.tick() => {
                let (from, pending) = {
                    let e = engine.lock().await;
                    (e.next_index(), e.mempool().to_vec())
                };
                let mut msgs = vec![ConsensusMsg::SyncRequest { from_height: from }];
                msgs.extend(pending.into_iter().map(ConsensusMsg::Op));
                // Re-flood mempool + sync request so nodes that missed earlier
                // gossip (e.g. mints sent before the mesh formed) still converge.
                flood_consensus(&peers, &seen, None, msgs).await;
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    AppCommand::Disconnect => break,
                    AppCommand::SendTokens { to, amount } => {
                        let out = engine.lock().await.submit_local_op(LedgerOp::Transfer {
                            from: my_addr,
                            to,
                            amount,
                            memo: "p2p-transfer".to_string(),
                        });
                        flood_consensus(&peers, &seen, None, out).await;
                        let _ = evt_tx.send(AppEvent::Log(
                            format!("Queued transfer {amount} → #{to}")
                        )).await;
                    }
                    _ => {}
                }
            }
            Ok((stream, peer_addr)) = listener.accept() => {
                let _ = evt_tx.send(AppEvent::Log(
                    format!("Peer connected: {peer_addr}")
                )).await;
                tokio::spawn(p2p_connection(
                    stream, my_key, listen_addr.clone(), name.clone(),
                    Arc::clone(&engine), Arc::clone(&peers), Arc::clone(&seen), evt_tx.clone(),
                ));
            }
        }
    }

    let _ = evt_tx
        .send(AppEvent::Disconnected {
            reason: "left mesh".to_string(),
        })
        .await;
}

/// Pick the identity for a node. An explicit `BOINC_IDENTITY` env var always
/// wins (used by the CLI and for stable single-node setups). Otherwise the
/// identity is derived from the connection params (role + address + name) so
/// several nodes launched locally each get a distinct, stable key automatically
/// — no need to juggle `BOINC_IDENTITY` per instance. Different connection →
/// different identity.
fn resolve_identity(role: &str, addr: &str, name: &str) -> Identity {
    if std::env::var("BOINC_IDENTITY").is_ok() {
        return Identity::load_or_generate(&Identity::default_path());
    }
    let mut h = DefaultHasher::new();
    (role, addr, name).hash(&mut h);
    let key = format!("{:016x}", h.finish());
    let path: PathBuf = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".boinc-quota")
        .join(format!("identity-{key}.json"));
    Identity::load_or_generate(&path)
}

// ── Coordinator / Worker consensus nodes ────────────────────────────────────────

/// Role of a node in the shared-ledger consensus mesh. Both roles are equal
/// validators; the role only governs task behaviour and transport shape.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Listens for workers/peer-coordinators and relays gossip; drives the clock.
    Coordinator,
    /// Dials a coordinator; runs the executor that claims and computes tasks.
    Worker,
}

async fn submit_and_flood(
    engine: &Arc<Mutex<ConsensusEngine>>,
    peers: &PeerMap,
    seen: &SeenSet,
    op: LedgerOp,
) {
    let out = engine.lock().await.submit_local_op(op);
    flood_consensus(peers, seen, None, out).await;
}

/// Run a coordinator or worker node. Every node holds a `ConsensusEngine` with a
/// `Network` replica; all economic and task actions become [`LedgerOp`]s that are
/// gossiped, BFT-voted, and applied identically everywhere. Coordinators listen
/// and relay; workers dial a coordinator. The coordinator emits `Tick` ops to
/// advance the shared clock (lease expiry).
#[allow(clippy::too_many_arguments)]
async fn run_role_node(
    role: NodeRole,
    cmd_rx: &mut mpsc::Receiver<AppCommand>,
    evt_tx: &mpsc::Sender<AppEvent>,
    identity: Identity,
    listen_addr: Option<String>,
    bootstrap: Vec<String>,
    name: String,
    balance: u64,
) {
    let my_key = identity.public_key_bytes();
    let my_short = identity.public_key_short();
    let my_enc_short = identity.encryption_pubkey_short();
    let my_addr = node_address(&my_key);
    let _ = evt_tx
        .send(AppEvent::Identity {
            signing_short: my_short.clone(),
            encryption_short: my_enc_short,
        })
        .await;

    let listener = match &listen_addr {
        Some(addr) => match TcpListener::bind(addr).await {
            Ok(l) => {
                let _ = evt_tx
                    .send(AppEvent::Log(format!("Listening on {addr}")))
                    .await;
                Some(l)
            }
            Err(e) => {
                let _ = evt_tx
                    .send(AppEvent::Error(format!("Bind {addr}: {e}")))
                    .await;
                return;
            }
        },
        None => None,
    };
    let my_listen = listen_addr.unwrap_or_default();

    let _ = evt_tx
        .send(AppEvent::Log(format!("Identity: {my_short}")))
        .await;

    let engine = Arc::new(Mutex::new(ConsensusEngine::new(identity, [])));
    let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
    let seen: SeenSet = Arc::new(Mutex::new(HashSet::new()));

    let _ = evt_tx.send(AppEvent::Connected).await;
    let _ = evt_tx
        .send(AppEvent::Registered {
            participant_id: my_addr,
        })
        .await;

    // Register ourselves (and mint our balance) into the shared ledger.
    submit_and_flood(
        &engine,
        &peers,
        &seen,
        LedgerOp::RegisterParticipant {
            id: my_addr,
            name: name.clone(),
            public_key: my_key,
            initial_balance: balance,
        },
    )
    .await;

    for addr in bootstrap {
        let addr = addr.trim().to_string();
        if addr.is_empty() {
            continue;
        }
        spawn_dial(
            addr,
            my_key,
            my_listen.clone(),
            name.clone(),
            Arc::clone(&engine),
            Arc::clone(&peers),
            Arc::clone(&seen),
            evt_tx.clone(),
        );
    }

    let mut executor_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut round = tokio::time::interval(Duration::from_millis(500));
    let mut sync_timer = tokio::time::interval(Duration::from_secs(3));
    let mut tick_timer = tokio::time::interval(Duration::from_secs(1));
    let started = std::time::Instant::now();
    const PROPOSE_GRACE: Duration = Duration::from_millis(2500);

    loop {
        tokio::select! {
            _ = round.tick() => {
                let out = {
                    let mut e = engine.lock().await;
                    if started.elapsed() >= PROPOSE_GRACE { e.try_propose() } else { vec![] }
                };
                flood_consensus(&peers, &seen, None, out).await;
                // Push the replicated state to the local UI.
                let snap = {
                    let e = engine.lock().await;
                    build_snapshot(e.state())
                };
                let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
            }
            _ = tick_timer.tick(), if role == NodeRole::Coordinator => {
                // Coordinator advances the shared clock so leases expire.
                submit_and_flood(&engine, &peers, &seen, LedgerOp::Tick { n: 1 }).await;
            }
            _ = sync_timer.tick() => {
                let (from, pending) = {
                    let e = engine.lock().await;
                    (e.next_index(), e.mempool().to_vec())
                };
                let mut msgs = vec![ConsensusMsg::SyncRequest { from_height: from }];
                msgs.extend(pending.into_iter().map(ConsensusMsg::Op));
                flood_consensus(&peers, &seen, None, msgs).await;
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    AppCommand::Disconnect => break,
                    AppCommand::CreateProject { name: pname, owner_encryption_pubkey } => {
                        submit_and_flood(&engine, &peers, &seen, LedgerOp::CreateProject {
                            owner_id: my_addr, name: pname, owner_encryption_pubkey,
                        }).await;
                    }
                    AppCommand::FundProject { project_id, amount } => {
                        submit_and_flood(&engine, &peers, &seen, LedgerOp::FundProject {
                            owner_id: my_addr, project_id, amount,
                        }).await;
                    }
                    AppCommand::DonateToProject { project_id, amount } => {
                        submit_and_flood(&engine, &peers, &seen, LedgerOp::DonateToProject {
                            supporter_id: my_addr, project_id, amount,
                        }).await;
                    }
                    AppCommand::SubmitTask { project_id, reward, payload } => {
                        submit_and_flood(&engine, &peers, &seen, LedgerOp::SubmitTask {
                            owner_id: my_addr, project_id, reward, payload,
                        }).await;
                    }
                    AppCommand::SendTokens { to, amount } => {
                        submit_and_flood(&engine, &peers, &seen, LedgerOp::Transfer {
                            from: my_addr, to, amount, memo: "transfer".to_string(),
                        }).await;
                    }
                    AppCommand::StartExecutor { reliability, compute_ticks, allowed_packages }
                        if executor_handle.is_none() =>
                    {
                        let h = tokio::spawn(ops_executor_loop(
                            my_addr, reliability, compute_ticks, allowed_packages,
                            Arc::clone(&engine), Arc::clone(&peers), Arc::clone(&seen), evt_tx.clone(),
                        ));
                        executor_handle = Some(h);
                        let _ = evt_tx.send(AppEvent::ExecutorStarted).await;
                        let _ = evt_tx.send(AppEvent::Log("Executor started".to_string())).await;
                    }
                    AppCommand::StopExecutor => {
                        if let Some(h) = executor_handle.take() {
                            h.abort();
                            let _ = evt_tx.send(AppEvent::ExecutorStopped).await;
                            let _ = evt_tx.send(AppEvent::Log("Executor stopped".to_string())).await;
                        }
                    }
                    _ => {}
                }
            }
            accept = async {
                match &listener {
                    Some(l) => l.accept().await,
                    None => std::future::pending::<std::io::Result<(TcpStream, std::net::SocketAddr)>>().await,
                }
            } => {
                if let Ok((stream, addr)) = accept {
                    let _ = evt_tx.send(AppEvent::Log(format!("Peer connected: {addr}"))).await;
                    tokio::spawn(p2p_connection(
                        stream, my_key, my_listen.clone(), name.clone(),
                        Arc::clone(&engine), Arc::clone(&peers), Arc::clone(&seen), evt_tx.clone(),
                    ));
                }
            }
        }
    }

    if let Some(h) = executor_handle {
        h.abort();
    }
    let _ = evt_tx
        .send(AppEvent::Disconnected {
            reason: "stopped".to_string(),
        })
        .await;
}

/// Worker-side executor driven by the replicated state: claim assigned tasks via
/// `RequestTask` ops, compute them, and report results via `SubmitResult` ops.
#[allow(clippy::too_many_arguments)]
async fn ops_executor_loop(
    my_addr: ParticipantId,
    reliability: u8,
    compute_ticks: u64,
    allowed_packages: Vec<String>,
    engine: Arc<Mutex<ConsensusEngine>>,
    peers: PeerMap,
    seen: SeenSet,
    evt_tx: mpsc::Sender<AppEvent>,
) {
    ensure_packages(&allowed_packages, &evt_tx).await;
    let executor = Executor::new(
        my_addr,
        "rsm-executor",
        reliability,
        compute_ticks.max(1),
        1,
        allowed_packages,
    );
    let mut processed: HashSet<u64> = HashSet::new();
    let mut nonce: u64 = 0;

    loop {
        // A task assigned to me in the replica that I have not yet computed.
        let job = {
            let e = engine.lock().await;
            let st = e.state();
            st.all_tasks().into_iter().find_map(|t| {
                let mine = matches!(
                    &t.status,
                    crate::model::TaskStatus::Assigned { worker_id, .. } if *worker_id == my_addr
                );
                if mine && !processed.contains(&t.id) {
                    let enc = st.project(t.project_id).and_then(|p| p.owner_encryption_pubkey);
                    Some((t.id, t.reward, t.payload.clone(), enc))
                } else {
                    None
                }
            })
        };

        match job {
            Some((task_id, reward, payload, owner_enc)) => {
                processed.insert(task_id);
                let exec = executor.clone();
                let computed = tokio::task::spawn_blocking(move || {
                    exec.execute_task_payload(task_id, &payload, reward)
                })
                .await;
                let Ok((computed, actual_cost)) = computed else {
                    continue;
                };
                let encrypted_result =
                    owner_enc.map(|pk| Identity::encrypt_for(&pk, computed.stdout.as_bytes()));
                let _ = evt_tx
                    .send(AppEvent::Log(format!(
                        "Computed task #{task_id} → {}",
                        computed.digest
                    )))
                    .await;
                submit_and_flood(
                    &engine,
                    &peers,
                    &seen,
                    LedgerOp::SubmitResult {
                        worker_id: my_addr,
                        task_id,
                        result_digest: computed.digest,
                        actual_cost,
                        encrypted_result,
                    },
                )
                .await;
            }
            None => {
                let pending = { engine.lock().await.state().pending_count() > 0 };
                if pending {
                    nonce += 1;
                    submit_and_flood(
                        &engine,
                        &peers,
                        &seen,
                        LedgerOp::RequestTask {
                            worker_id: my_addr,
                            nonce,
                        },
                    )
                    .await;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

#[cfg(test)]
mod p2p_tests {
    use super::*;

    /// Spawn a P2P node task with its own generated identity. Returns the
    /// command sender, event receiver, and the node's ledger address.
    fn spawn_node(
        listen: &str,
        bootstrap: Vec<String>,
        balance: u64,
    ) -> (mpsc::Sender<AppCommand>, mpsc::Receiver<AppEvent>, ParticipantId) {
        let identity = Identity::generate();
        let addr = node_address(&identity.public_key_bytes());
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<AppCommand>(32);
        let (evt_tx, evt_rx) = mpsc::channel::<AppEvent>(512);
        let listen = listen.to_string();
        tokio::spawn(async move {
            p2p_node_with_identity(
                &mut cmd_rx,
                &evt_tx,
                identity,
                listen,
                bootstrap,
                "node".to_string(),
                balance,
            )
            .await;
        });
        (cmd_tx, evt_rx, addr)
    }

    /// Drain pending events, returning the most recent P2pUpdate snapshot.
    fn latest_snapshot(rx: &mut mpsc::Receiver<AppEvent>) -> Option<P2pSnapshot> {
        let mut last = None;
        while let Ok(evt) = rx.try_recv() {
            if let AppEvent::P2pUpdate(snap) = evt {
                last = Some(snap);
            }
        }
        last
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_nodes_form_mesh_and_converge_on_a_transfer() {
        let (_ca, mut ea, _addr_a) = spawn_node("127.0.0.1:17901", vec![], 100);
        let (_cb, mut eb, addr_b) =
            spawn_node("127.0.0.1:17902", vec!["127.0.0.1:17901".to_string()], 100);
        let (_cc, mut ec, _addr_c) =
            spawn_node("127.0.0.1:17903", vec!["127.0.0.1:17901".to_string()], 100);

        // Let the mesh form and the three initial mints commit.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Node A (first node) transfers 30 to node B.
        // We don't have A's sender wired for sends here, so use B sending to itself
        // is meaningless; instead drive the transfer from B to A's address would
        // need addr_a — use the A command channel.
        let _ = _ca
            .send(AppCommand::SendTokens {
                to: addr_b,
                amount: 30,
            })
            .await;

        // Poll up to ~12s for convergence.
        let mut converged = false;
        let mut snap_a = P2pSnapshot::default();
        let mut snap_b = P2pSnapshot::default();
        let mut snap_c = P2pSnapshot::default();
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(300)).await;
            if let Some(s) = latest_snapshot(&mut ea) {
                snap_a = s;
            }
            if let Some(s) = latest_snapshot(&mut eb) {
                snap_b = s;
            }
            if let Some(s) = latest_snapshot(&mut ec) {
                snap_c = s;
            }
            let equal_height = snap_a.block_count == snap_b.block_count
                && snap_b.block_count == snap_c.block_count;
            if equal_height
                && snap_a.block_count >= 3
                && snap_a.validators == 3
                && snap_a.my_balance == 70
                && snap_b.my_balance == 130
            {
                converged = true;
                break;
            }
        }

        assert!(
            converged,
            "nodes did not converge: A(h={},val={},bal={}) B(h={},bal={}) C(h={},bal={})",
            snap_a.block_count,
            snap_a.validators,
            snap_a.my_balance,
            snap_b.block_count,
            snap_b.my_balance,
            snap_c.block_count,
            snap_c.my_balance,
        );
        assert!(snap_a.blockchain_valid && snap_b.blockchain_valid && snap_c.blockchain_valid);
    }

    /// Spawn a coordinator/worker role node with a generated identity.
    fn spawn_role(
        role: NodeRole,
        listen: Option<&str>,
        bootstrap: Vec<String>,
        balance: u64,
    ) -> (mpsc::Sender<AppCommand>, mpsc::Receiver<AppEvent>) {
        let identity = Identity::generate();
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<AppCommand>(32);
        let (evt_tx, evt_rx) = mpsc::channel::<AppEvent>(512);
        let listen = listen.map(|s| s.to_string());
        tokio::spawn(async move {
            run_role_node(
                role,
                &mut cmd_rx,
                &evt_tx,
                identity,
                listen,
                bootstrap,
                "node".to_string(),
                balance,
            )
            .await;
        });
        (cmd_tx, evt_rx)
    }

    fn latest_state(rx: &mut mpsc::Receiver<AppEvent>) -> Option<NetworkSnapshot> {
        let mut last = None;
        while let Ok(evt) = rx.try_recv() {
            if let AppEvent::StateUpdate(snap) = evt {
                last = Some(snap);
            }
        }
        last
    }

    /// A coordinator and two workers, all consensus validators over one shared
    /// ledger: the coordinator creates/funds a project and submits a task; every
    /// node must converge on the identical project + task state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coordinator_and_workers_converge_on_project_and_task() {
        let (cc, mut ec) = spawn_role(NodeRole::Coordinator, Some("127.0.0.1:17911"), vec![], 100);
        let (_w1, mut e1) = spawn_role(
            NodeRole::Worker,
            None,
            vec!["127.0.0.1:17911".to_string()],
            0,
        );
        let (_w2, mut e2) = spawn_role(
            NodeRole::Worker,
            None,
            vec!["127.0.0.1:17911".to_string()],
            0,
        );

        // Let the mesh form, validators propagate, and registrations commit.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let _ = cc
            .send(AppCommand::CreateProject {
                name: "P".to_string(),
                owner_encryption_pubkey: None,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(900)).await;
        let _ = cc
            .send(AppCommand::FundProject {
                project_id: 1,
                amount: 50,
            })
            .await;
        tokio::time::sleep(Duration::from_millis(900)).await;
        let _ = cc
            .send(AppCommand::SubmitTask {
                project_id: 1,
                reward: 10,
                payload: "x".to_string(),
            })
            .await;

        let mut sc = NetworkSnapshot::default();
        let mut s1 = NetworkSnapshot::default();
        let mut s2 = NetworkSnapshot::default();
        let mut converged = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(300)).await;
            if let Some(s) = latest_state(&mut ec) {
                sc = s;
            }
            if let Some(s) = latest_state(&mut e1) {
                s1 = s;
            }
            if let Some(s) = latest_state(&mut e2) {
                s2 = s;
            }
            let ok = |s: &NetworkSnapshot| {
                s.projects.len() == 1
                    && s.tasks.len() == 1
                    && s.projects[0].quota_available == 40
                    && s.projects[0].quota_locked == 10
            };
            if ok(&sc) && ok(&s1) && ok(&s2) {
                converged = true;
                break;
            }
        }

        assert!(
            converged,
            "did not converge: coord(p={},t={}) w1(p={},t={}) w2(p={},t={})",
            sc.projects.len(),
            sc.tasks.len(),
            s1.projects.len(),
            s1.tasks.len(),
            s2.projects.len(),
            s2.tasks.len(),
        );
        // All three replicas agree on the funded project's quota split.
        assert_eq!(s1.projects[0].quota_locked, 10);
        assert_eq!(s2.projects[0].quota_available, 40);
    }
}

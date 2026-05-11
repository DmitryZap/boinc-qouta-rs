use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use crate::blockchain::Transaction;
use crate::executor::Executor;
use crate::model::ParticipantId;
use crate::network::{Network, NetworkConfig, NetworkError};
use crate::protocol::*;

type TcpReader = FramedRead<tokio::net::tcp::OwnedReadHalf, LinesCodec>;
type TcpWriter = FramedWrite<tokio::net::tcp::OwnedWriteHalf, LinesCodec>;
type SharedReader = Arc<Mutex<TcpReader>>;
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
                    quorum,
                } => {
                    run_coordinator(
                        &mut self.cmd_rx,
                        &self.evt_tx,
                        listen_addr,
                        name,
                        balance,
                        quorum,
                    )
                    .await;
                }
                AppCommand::ConnectWorker {
                    coord_addr,
                    name,
                    balance,
                } => {
                    run_worker(&mut self.cmd_rx, &self.evt_tx, coord_addr, name, balance).await;
                }
                _ => {}
            }
        }
    }
}

// ── Coordinator mode ──────────────────────────────────────────────────────────

async fn run_coordinator(
    cmd_rx: &mut mpsc::Receiver<AppCommand>,
    evt_tx: &mpsc::Sender<AppEvent>,
    listen_addr: String,
    name: String,
    balance: u64,
    quorum: u64,
) {
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
        .send(AppEvent::Log(format!("Listening on {listen_addr}")))
        .await;

    let config = NetworkConfig {
        consensus_quorum: quorum.max(1),
        ..NetworkConfig::default()
    };
    let mut initial_network =
        load_coordinator_state().unwrap_or_else(|| Network::with_config(config));
    initial_network.set_config(config);
    let network = Arc::new(Mutex::new(initial_network));
    let (broadcast_tx, _) = broadcast::channel::<String>(256);

    let coordinator_identity =
        crate::identity::Identity::load_or_generate(&crate::identity::Identity::default_path());
    let coord_pubkey = coordinator_identity.public_key_bytes();
    let coord_enc_pubkey = coordinator_identity.encryption_pubkey_bytes();
    let my_id = {
        let mut net = network.lock().await;
        net.find_participant_by_pubkey(&coord_pubkey)
            .unwrap_or_else(|| net.register_participant(name, balance, Some(coord_pubkey)))
    };
    persist_coordinator_state(&network, evt_tx).await;
    let _ = evt_tx
        .send(AppEvent::Log(format!(
            "Identity: {}",
            coordinator_identity.public_key_short()
        )))
        .await;
    let _ = evt_tx.send(AppEvent::Connected).await;
    let _ = evt_tx
        .send(AppEvent::Registered {
            participant_id: my_id,
        })
        .await;
    send_state_update(evt_tx, &network).await;

    let mut executor_stop: Option<tokio::task::JoinHandle<()>> = None;
    let mut tick_timer = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tick_timer.tick() => {
                // Advance network clock so lease timeouts expire and abandoned
                // tasks return to the Pending queue for new workers to pick up.
                let prev_pending;
                let new_pending;
                {
                    let mut net = network.lock().await;
                    prev_pending = net.pending_count();
                    net.tick(1);
                    new_pending = net.pending_count();
                }
                if new_pending > prev_pending {
                    let _ = evt_tx.send(AppEvent::Log(
                        format!("Lease(s) expired: {} task(s) returned to queue", new_pending - prev_pending)
                    )).await;
                    broadcast_and_notify(&network, &broadcast_tx, evt_tx).await;
                }
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    AppCommand::Disconnect => break,
                    AppCommand::CreateProject { name, owner_encryption_pubkey } => {
                        let pk = owner_encryption_pubkey.or(Some(coord_enc_pubkey));
                        apply_local_change(
                            &network,
                            &broadcast_tx,
                            evt_tx,
                            |net| net.create_project(my_id, name, pk),
                            |id| Some(AppEvent::Log(format!("Project #{id} created"))),
                        ).await;
                    }
                    AppCommand::FundProject { project_id, amount } => {
                        apply_local_change(
                            &network,
                            &broadcast_tx,
                            evt_tx,
                            |net| net.fund_project_from_owner(my_id, project_id, amount),
                            |_| None,
                        ).await;
                    }
                    AppCommand::DonateToProject { project_id, amount } => {
                        apply_local_change(
                            &network,
                            &broadcast_tx,
                            evt_tx,
                            |net| net.donate_to_project(my_id, project_id, amount),
                            |_| None,
                        ).await;
                    }
                    AppCommand::SubmitTask { project_id, reward, payload } => {
                        apply_local_change(
                            &network,
                            &broadcast_tx,
                            evt_tx,
                            |net| net.submit_task(my_id, project_id, reward, payload),
                            |task_id| Some(AppEvent::Log(format!("Task #{task_id} submitted"))),
                        ).await;
                    }
                    AppCommand::StartExecutor { reliability, compute_ticks }
                        if executor_stop.is_none() =>
                    {
                        let net_clone = Arc::clone(&network);
                        let bcast = broadcast_tx.clone();
                        let evt = evt_tx.clone();
                        let handle = tokio::spawn(coordinator_executor_loop(
                            my_id, reliability, compute_ticks, net_clone, bcast, evt,
                        ));
                        executor_stop = Some(handle);
                        let _ = evt_tx.send(AppEvent::ExecutorStarted).await;
                        let _ = evt_tx.send(AppEvent::Log("Executor started".to_string())).await;
                    }
                    AppCommand::StopExecutor => {
                        if let Some(handle) = executor_stop.take() {
                            handle.abort();
                            let _ = evt_tx.send(AppEvent::ExecutorStopped).await;
                            let _ = evt_tx.send(AppEvent::Log("Executor stopped".to_string())).await;
                        }
                    }
                    _ => {}
                }
            }
            Ok((stream, peer_addr)) = listener.accept() => {
                let _ = evt_tx.send(AppEvent::Log(format!("Peer connected: {peer_addr}"))).await;
                let net_clone = Arc::clone(&network);
                let bcast = broadcast_tx.clone();
                let evt = evt_tx.clone();
                tokio::spawn(handle_peer(stream, net_clone, bcast, evt));
            }
        }
    }

    if let Some(handle) = executor_stop {
        handle.abort();
    }
    let _ = evt_tx
        .send(AppEvent::Disconnected {
            reason: "stopped".to_string(),
        })
        .await;
}

async fn handle_peer(
    stream: TcpStream,
    network: Arc<Mutex<Network>>,
    broadcast_tx: broadcast::Sender<String>,
    evt_tx: mpsc::Sender<AppEvent>,
) {
    let (read_half, write_half) = stream.into_split();
    let mut reader = FramedRead::new(read_half, LinesCodec::new());
    let writer = Arc::new(Mutex::new(FramedWrite::new(write_half, LinesCodec::new())));

    // Forward broadcast state updates to this peer
    let writer_bcast = Arc::clone(&writer);
    let mut bcast_rx = broadcast_tx.subscribe();
    tokio::spawn(async move {
        loop {
            match bcast_rx.recv().await {
                Ok(msg) => {
                    let mut w = writer_bcast.lock().await;
                    let _ = w.send(msg).await;
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    });

    let mut peer_id: Option<ParticipantId> = None;
    let mut peer_nonce: Option<[u8; 32]> = None;

    while let Some(line) = reader.next().await {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let request: PeerRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = PeerResponse::Error {
                    msg: format!("parse error: {e}"),
                };
                send_response(&writer, resp).await;
                continue;
            }
        };

        let response = handle_peer_request(
            request,
            &network,
            &broadcast_tx,
            &evt_tx,
            &mut peer_id,
            &mut peer_nonce,
        )
        .await;
        send_response(&writer, response).await;
    }

    let _ = evt_tx
        .send(AppEvent::Log("Peer disconnected".to_string()))
        .await;
}

async fn handle_peer_request(
    req: PeerRequest,
    network: &Arc<Mutex<Network>>,
    broadcast_tx: &broadcast::Sender<String>,
    evt_tx: &mpsc::Sender<AppEvent>,
    peer_id: &mut Option<ParticipantId>,
    peer_nonce: &mut Option<[u8; 32]>,
) -> PeerResponse {
    let mut net = network.lock().await;
    match req {
        PeerRequest::Hello {
            name,
            public_key,
            initial_balance,
        } => {
            let participant_id = net
                .find_participant_by_pubkey(&public_key)
                .unwrap_or_else(|| {
                    net.register_participant(name, initial_balance, Some(public_key))
                });
            *peer_id = Some(participant_id);

            let mut nonce = [0u8; 32];
            use rand::RngCore;
            rand::rngs::OsRng.fill_bytes(&mut nonce);
            *peer_nonce = Some(nonce);

            PeerResponse::Challenge {
                participant_id,
                nonce,
            }
        }
        PeerRequest::Auth {
            public_key,
            nonce_signature,
        } => {
            let Some(nonce) = peer_nonce.take() else {
                return PeerResponse::Denied {
                    reason: "no pending challenge".to_string(),
                };
            };
            let Some(id) = *peer_id else {
                return PeerResponse::Denied {
                    reason: "hello not sent".to_string(),
                };
            };

            if !crate::identity::Identity::verify(&public_key, &nonce, &nonce_signature) {
                *peer_id = None;
                return PeerResponse::Denied {
                    reason: "invalid signature".to_string(),
                };
            }

            let snap = build_snapshot(&net);
            drop(net);
            broadcast_snapshot(broadcast_tx, &snap);
            PeerResponse::Welcome { participant_id: id }
        }
        PeerRequest::CreateProject {
            name,
            owner_encryption_pubkey,
        } => {
            let id = match registered_peer_id(peer_id) {
                Ok(id) => id,
                Err(resp) => return resp,
            };
            match net.create_project(id, name, owner_encryption_pubkey) {
                Ok(project_id) => {
                    publish_peer_state(&net, broadcast_tx, evt_tx);
                    let _ = evt_tx
                        .try_send(AppEvent::Log(format!("Peer created project #{project_id}")));
                    PeerResponse::ProjectCreated { project_id }
                }
                Err(e) => PeerResponse::Error {
                    msg: format!("{e:?}"),
                },
            }
        }
        PeerRequest::FundProject { project_id, amount } => {
            let id = match registered_peer_id(peer_id) {
                Ok(id) => id,
                Err(resp) => return resp,
            };
            match net.fund_project_from_owner(id, project_id, amount) {
                Ok(()) => {
                    publish_peer_state(&net, broadcast_tx, evt_tx);
                    PeerResponse::Ok
                }
                Err(e) => PeerResponse::Error {
                    msg: format!("{e:?}"),
                },
            }
        }
        PeerRequest::DonateToProject { project_id, amount } => {
            let id = match registered_peer_id(peer_id) {
                Ok(id) => id,
                Err(resp) => return resp,
            };
            match net.donate_to_project(id, project_id, amount) {
                Ok(()) => {
                    publish_peer_state(&net, broadcast_tx, evt_tx);
                    PeerResponse::Ok
                }
                Err(e) => PeerResponse::Error {
                    msg: format!("{e:?}"),
                },
            }
        }
        PeerRequest::SubmitTask {
            project_id,
            reward,
            payload,
        } => {
            let id = match registered_peer_id(peer_id) {
                Ok(id) => id,
                Err(resp) => return resp,
            };
            match net.submit_task(id, project_id, reward, payload) {
                Ok(task_id) => {
                    publish_peer_state(&net, broadcast_tx, evt_tx);
                    PeerResponse::TaskSubmitted { task_id }
                }
                Err(e) => PeerResponse::Error {
                    msg: format!("{e:?}"),
                },
            }
        }
        PeerRequest::RequestTask { worker_id } => match net.request_task(worker_id) {
            Ok(task) => {
                let owner_encryption_pubkey = net
                    .project(task.project_id)
                    .and_then(|p| p.owner_encryption_pubkey);
                publish_peer_state(&net, broadcast_tx, evt_tx);
                PeerResponse::TaskAssigned {
                    task_id: task.id,
                    project_id: task.project_id,
                    reward: task.reward,
                    payload: task.payload,
                    owner_encryption_pubkey,
                }
            }
            Err(NetworkError::NoPendingTasks) => PeerResponse::NoPendingTasks,
            Err(e) => PeerResponse::Error {
                msg: format!("{e:?}"),
            },
        },
        PeerRequest::SubmitResult {
            worker_id,
            task_id,
            result_digest,
            actual_cost,
            encrypted_result,
        } => match net.submit_result(
            worker_id,
            task_id,
            result_digest,
            actual_cost,
            encrypted_result,
        ) {
            Ok(consensus) => {
                publish_peer_state(&net, broadcast_tx, evt_tx);
                PeerResponse::ResultAck { consensus }
            }
            Err(e) => PeerResponse::Error {
                msg: format!("{e:?}"),
            },
        },
        PeerRequest::Heartbeat { worker_id, task_id } => match net.heartbeat(worker_id, task_id) {
            Ok(()) => PeerResponse::Ok,
            Err(e) => PeerResponse::Error {
                msg: format!("{e:?}"),
            },
        },
        PeerRequest::GetState => {
            let snap = build_snapshot(&net);
            PeerResponse::StateUpdate(snap)
        }
    }
}

async fn coordinator_executor_loop(
    worker_id: ParticipantId,
    reliability: u8,
    compute_ticks: u64,
    network: Arc<Mutex<Network>>,
    broadcast_tx: broadcast::Sender<String>,
    evt_tx: mpsc::Sender<AppEvent>,
) {
    let executor = Executor::new(
        worker_id,
        "local-executor",
        reliability,
        compute_ticks.max(1),
        1,
    );
    loop {
        let result = {
            let net_clone = Arc::clone(&network);
            let exec_clone = executor.clone();
            tokio::task::spawn_blocking(move || {
                let mut net = net_clone.blocking_lock();
                exec_clone.process_next_task(&mut net)
            })
            .await
        };

        match result {
            Ok(Ok(crate::executor::ExecutorEvent::Idle)) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(Ok(crate::executor::ExecutorEvent::ProcessedTask {
                task_id,
                consensus_reached,
                reward,
                ..
            })) => {
                let msg = format!("Task #{task_id}: reward={reward} consensus={consensus_reached}");
                let _ = evt_tx.send(AppEvent::Log(msg)).await;
                let snap = build_snapshot(&*network.lock().await);
                persist_coordinator_state(&network, &evt_tx).await;
                broadcast_snapshot(&broadcast_tx, &snap);
                let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
            }
            Ok(Err(e)) => {
                let _ = evt_tx
                    .send(AppEvent::Error(format!("Executor: {e:?}")))
                    .await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => break,
        }
    }
}

// ── Worker mode ───────────────────────────────────────────────────────────────

async fn run_worker(
    cmd_rx: &mut mpsc::Receiver<AppCommand>,
    evt_tx: &mpsc::Sender<AppEvent>,
    coord_addr: String,
    name: String,
    balance: u64,
) {
    let stream = match TcpStream::connect(&coord_addr).await {
        Ok(s) => s,
        Err(e) => {
            let _ = evt_tx
                .send(AppEvent::Error(format!("Connect {coord_addr}: {e}")))
                .await;
            return;
        }
    };
    let _ = evt_tx
        .send(AppEvent::Log(format!("Connected to {coord_addr}")))
        .await;

    let (read_half, write_half) = stream.into_split();
    let reader = Arc::new(Mutex::new(FramedRead::new(read_half, LinesCodec::new())));
    let writer = Arc::new(Mutex::new(FramedWrite::new(write_half, LinesCodec::new())));

    // Load identity and perform Hello/Auth handshake
    let identity =
        crate::identity::Identity::load_or_generate(&crate::identity::Identity::default_path());
    let public_key = identity.public_key_bytes();
    let my_encryption_pubkey = identity.encryption_pubkey_bytes();
    let _ = evt_tx
        .send(AppEvent::Log(format!(
            "Identity: {}",
            identity.public_key_short()
        )))
        .await;

    // Send Hello
    let req = PeerRequest::Hello {
        name,
        public_key,
        initial_balance: balance,
    };
    if let Err(e) = send_request(&writer, &req).await {
        let _ = evt_tx
            .send(AppEvent::Error(format!("Hello send: {e}")))
            .await;
        return;
    }

    // Receive Challenge
    let (participant_id, nonce) = match recv_response(&reader).await {
        Some(PeerResponse::Challenge {
            participant_id,
            nonce,
        }) => (participant_id, nonce),
        Some(PeerResponse::Error { msg }) => {
            let _ = evt_tx.send(AppEvent::Error(msg)).await;
            return;
        }
        _ => {
            let _ = evt_tx
                .send(AppEvent::Error("unexpected hello response".to_string()))
                .await;
            return;
        }
    };

    // Sign and send Auth
    let signature = identity.sign_nonce(&nonce);
    let req = PeerRequest::Auth {
        public_key,
        nonce_signature: signature,
    };
    if let Err(e) = send_request(&writer, &req).await {
        let _ = evt_tx
            .send(AppEvent::Error(format!("Auth send: {e}")))
            .await;
        return;
    }

    // Receive Welcome
    let my_id = match recv_response(&reader).await {
        Some(PeerResponse::Welcome { participant_id: id }) => {
            // Confirm the server assigned us the same id as announced in Challenge
            let _ = id; // use whichever the server confirmed
            let _ = evt_tx.send(AppEvent::Connected).await;
            let _ = evt_tx.send(AppEvent::Registered { participant_id }).await;
            participant_id
        }
        Some(PeerResponse::Denied { reason }) => {
            let _ = evt_tx
                .send(AppEvent::Error(format!("Auth denied: {reason}")))
                .await;
            return;
        }
        Some(PeerResponse::Error { msg }) => {
            let _ = evt_tx.send(AppEvent::Error(msg)).await;
            return;
        }
        _ => {
            let _ = evt_tx
                .send(AppEvent::Error("unexpected auth response".to_string()))
                .await;
            return;
        }
    };

    let _ = send_request(&writer, &PeerRequest::GetState).await;
    if let Some(PeerResponse::StateUpdate(snap)) = recv_response(&reader).await {
        let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
    }

    let mut executor_handle: Option<tokio::task::JoinHandle<()>> = {
        let w = Arc::clone(&writer);
        let r = Arc::clone(&reader);
        let e = evt_tx.clone();
        let handle = tokio::spawn(worker_executor_loop(my_id, 95, 2, w, r, e));
        let _ = evt_tx.send(AppEvent::ExecutorStarted).await;
        let _ = evt_tx
            .send(AppEvent::Log("Worker executor started".to_string()))
            .await;
        Some(handle)
    };

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    AppCommand::Disconnect => break,
                    AppCommand::CreateProject { name, owner_encryption_pubkey } => {
                        let pk = owner_encryption_pubkey.or(Some(my_encryption_pubkey));
                        worker_send_recv(&writer, &reader, evt_tx,
                            PeerRequest::CreateProject { name, owner_encryption_pubkey: pk },
                        ).await;
                        request_state_update(&writer, &reader, evt_tx).await;
                    }
                    AppCommand::FundProject { project_id, amount } => {
                        worker_send_recv(&writer, &reader, evt_tx,
                            PeerRequest::FundProject { project_id, amount },
                        ).await;
                        request_state_update(&writer, &reader, evt_tx).await;
                    }
                    AppCommand::DonateToProject { project_id, amount } => {
                        worker_send_recv(&writer, &reader, evt_tx,
                            PeerRequest::DonateToProject { project_id, amount },
                        ).await;
                        request_state_update(&writer, &reader, evt_tx).await;
                    }
                    AppCommand::SubmitTask { project_id, reward, payload } => {
                        worker_send_recv(&writer, &reader, evt_tx,
                            PeerRequest::SubmitTask { project_id, reward, payload },
                        ).await;
                        request_state_update(&writer, &reader, evt_tx).await;
                    }
                    AppCommand::StartExecutor { reliability, compute_ticks }
                        if executor_handle.is_none() =>
                    {
                        let w = Arc::clone(&writer);
                        let r = Arc::clone(&reader);
                        let e = evt_tx.clone();
                        let handle = tokio::spawn(worker_executor_loop(
                            my_id, reliability, compute_ticks, w, r, e,
                        ));
                        executor_handle = Some(handle);
                        let _ = evt_tx.send(AppEvent::ExecutorStarted).await;
                        let _ = evt_tx.send(AppEvent::Log("Worker executor started".to_string())).await;
                    }
                    AppCommand::StopExecutor => {
                        if let Some(h) = executor_handle.take() {
                            h.abort();
                            let _ = evt_tx.send(AppEvent::ExecutorStopped).await;
                            let _ = evt_tx.send(AppEvent::Log("Worker executor stopped".to_string())).await;
                        }
                    }
                    _ => {}
                }
            }
            // Poll incoming messages (state updates pushed by coordinator)
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if let Ok(mut r) = reader.try_lock() {
                    while let Ok(Some(line)) = tokio::time::timeout(
                        Duration::from_millis(10), r.next()
                    ).await {
                        if let Ok(line) = line {
                            if let Ok(PeerResponse::StateUpdate(snap)) = serde_json::from_str(&line) {
                                let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(h) = executor_handle {
        h.abort();
    }
    let _ = evt_tx.send(AppEvent::ExecutorStopped).await;
    let _ = evt_tx
        .send(AppEvent::Disconnected {
            reason: "disconnected".to_string(),
        })
        .await;
}

async fn worker_executor_loop(
    worker_id: ParticipantId,
    reliability: u8,
    compute_ticks: u64,
    writer: SharedWriter,
    reader: SharedReader,
    evt_tx: mpsc::Sender<AppEvent>,
) {
    let executor = Executor::new(worker_id, "worker-executor", reliability, 1, 1);
    loop {
        // Request a task
        let _ = send_request(&writer, &PeerRequest::RequestTask { worker_id }).await;
        let task = match recv_response(&reader).await {
            Some(PeerResponse::TaskAssigned {
                task_id,
                project_id,
                reward,
                payload,
                owner_encryption_pubkey,
            }) => (
                task_id,
                project_id,
                reward,
                payload,
                owner_encryption_pubkey,
            ),
            Some(PeerResponse::NoPendingTasks) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        let (task_id, _project_id, reward, payload, owner_enc_pk) = task;

        // Simulate compute time
        let delay = compute_ticks.max(1) * 100;
        tokio::time::sleep(Duration::from_millis(delay)).await;

        // Compute digest + stdout, then encrypt stdout for project owner (if pubkey provided).
        let (computed, actual_cost) = executor.execute_task_payload(task_id, &payload, reward);
        let result_digest = computed.digest.clone();
        let encrypted_result = owner_enc_pk
            .map(|pk| crate::identity::Identity::encrypt_for(&pk, computed.stdout.as_bytes()));
        let _ = send_request(
            &writer,
            &PeerRequest::SubmitResult {
                worker_id,
                task_id,
                result_digest: result_digest.clone(),
                actual_cost,
                encrypted_result,
            },
        )
        .await;

        match recv_response(&reader).await {
            Some(PeerResponse::ResultAck { consensus }) => {
                let _ = evt_tx
                    .send(AppEvent::Log(format!(
                        "Task #{task_id} result={result_digest} consensus={consensus}"
                    )))
                    .await;
                // Request fresh state so worker UI balance updates immediately
                let _ = send_request(&writer, &PeerRequest::GetState).await;
                if let Some(PeerResponse::StateUpdate(snap)) = recv_response(&reader).await {
                    let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
                }
            }
            Some(PeerResponse::StateUpdate(snap)) => {
                let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
            }
            _ => {}
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn registered_peer_id(peer_id: &Option<ParticipantId>) -> Result<ParticipantId, PeerResponse> {
    (*peer_id).ok_or_else(|| PeerResponse::Error {
        msg: "not registered".to_string(),
    })
}

fn publish_peer_state(
    network: &Network,
    broadcast_tx: &broadcast::Sender<String>,
    evt_tx: &mpsc::Sender<AppEvent>,
) -> NetworkSnapshot {
    save_coordinator_state_ref(network);
    let snap = build_snapshot(network);
    broadcast_snapshot(broadcast_tx, &snap);
    let _ = evt_tx.try_send(AppEvent::StateUpdate(snap.clone()));
    snap
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

async fn send_state_update(evt_tx: &mpsc::Sender<AppEvent>, network: &Arc<Mutex<Network>>) {
    let snap = build_snapshot(&*network.lock().await);
    let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
}

async fn apply_local_change<T>(
    network: &Arc<Mutex<Network>>,
    broadcast_tx: &broadcast::Sender<String>,
    evt_tx: &mpsc::Sender<AppEvent>,
    operation: impl FnOnce(&mut Network) -> Result<T, NetworkError>,
    success_event: impl FnOnce(T) -> Option<AppEvent>,
) {
    let result = {
        let mut net = network.lock().await;
        operation(&mut net)
    };

    match result {
        Ok(value) => {
            if let Some(event) = success_event(value) {
                let _ = evt_tx.send(event).await;
            }
            broadcast_and_notify(network, broadcast_tx, evt_tx).await;
        }
        Err(err) => {
            let _ = evt_tx.send(AppEvent::Error(format!("{err:?}"))).await;
        }
    }
}

fn coordinator_state_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("boinc-quota")
        .join("coordinator-state.json")
}

fn load_coordinator_state() -> Option<Network> {
    let path = coordinator_state_path();
    let bytes = std::fs::read(&path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_coordinator_state_ref(network: &Network) {
    let path = coordinator_state_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(bytes) = serde_json::to_vec_pretty(network) {
        let _ = std::fs::write(path, bytes);
    }
}

async fn persist_coordinator_state(network: &Arc<Mutex<Network>>, evt_tx: &mpsc::Sender<AppEvent>) {
    let net = network.lock().await;
    let path = coordinator_state_path();
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            let _ = evt_tx
                .send(AppEvent::Error(format!("Persist state: {e}")))
                .await;
            return;
        }
    }
    match serde_json::to_vec_pretty(&*net) {
        Ok(bytes) => {
            if let Err(e) = std::fs::write(&path, bytes) {
                let _ = evt_tx
                    .send(AppEvent::Error(format!("Persist state: {e}")))
                    .await;
            }
        }
        Err(e) => {
            let _ = evt_tx
                .send(AppEvent::Error(format!("Persist state: {e}")))
                .await;
        }
    }
}

fn broadcast_snapshot(broadcast_tx: &broadcast::Sender<String>, snap: &NetworkSnapshot) {
    if let Ok(msg) = serde_json::to_string(&PeerResponse::StateUpdate(snap.clone())) {
        let _ = broadcast_tx.send(msg);
    }
}

async fn broadcast_and_notify(
    network: &Arc<Mutex<Network>>,
    broadcast_tx: &broadcast::Sender<String>,
    evt_tx: &mpsc::Sender<AppEvent>,
) {
    persist_coordinator_state(network, evt_tx).await;
    let snap = build_snapshot(&*network.lock().await);
    broadcast_snapshot(broadcast_tx, &snap);
    let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
}

async fn send_response(writer: &SharedWriter, resp: PeerResponse) {
    if let Ok(msg) = serde_json::to_string(&resp) {
        let mut w = writer.lock().await;
        let _ = w.send(msg).await;
    }
}

async fn send_request(writer: &SharedWriter, req: &PeerRequest) -> Result<(), String> {
    let msg = serde_json::to_string(req).map_err(|e| e.to_string())?;
    let mut w = writer.lock().await;
    w.send(msg).await.map_err(|e| e.to_string())
}

async fn recv_response(reader: &SharedReader) -> Option<PeerResponse> {
    let timeout = Duration::from_secs(5);
    let mut r = reader.lock().await;
    match tokio::time::timeout(timeout, r.next()).await {
        Ok(Some(Ok(line))) => serde_json::from_str(&line).ok(),
        _ => None,
    }
}

async fn worker_send_recv(
    writer: &SharedWriter,
    reader: &SharedReader,
    evt_tx: &mpsc::Sender<AppEvent>,
    req: PeerRequest,
) {
    if send_request(writer, &req).await.is_err() {
        return;
    }
    match recv_response(reader).await {
        Some(PeerResponse::Error { msg }) => {
            let _ = evt_tx.send(AppEvent::Error(msg)).await;
        }
        Some(PeerResponse::StateUpdate(snap)) => {
            let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
        }
        _ => {}
    }
}

async fn request_state_update(
    writer: &SharedWriter,
    reader: &SharedReader,
    evt_tx: &mpsc::Sender<AppEvent>,
) {
    let _ = send_request(writer, &PeerRequest::GetState).await;
    if let Some(PeerResponse::StateUpdate(snap)) = recv_response(reader).await {
        let _ = evt_tx.send(AppEvent::StateUpdate(snap)).await;
    }
}

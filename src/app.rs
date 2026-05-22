use std::collections::VecDeque;
use std::time::Duration;

use eframe::egui::{self, Button, Color32, CornerRadius, Margin, RichText, Stroke, Vec2};
use tokio::sync::mpsc;

use crate::model::ParticipantId;
use crate::protocol::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine as _};

mod payload_templates;
mod style;

use payload_templates::*;
use style::*;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PayloadMode {
    Python,
    GpuPython,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tab {
    Connect,
    Projects,
    Tasks,
    Executor,
    Packages,
    Network,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskFilter {
    All,
    Pending,
    Assigned,
    Completed,
    QuotaExhausted,
    Rejected,
}

pub struct BoincApp {
    cmd_tx: mpsc::Sender<AppCommand>,
    evt_rx: mpsc::Receiver<AppEvent>,

    // Network state
    connected: bool,
    my_id: Option<ParticipantId>,
    snapshot: NetworkSnapshot,
    log: VecDeque<String>,
    executor_running: bool,

    // UI state
    active_tab: Tab,
    log_expanded: bool,
    selected_task: Option<u64>,

    // Connect tab
    node_mode: NodeMode,
    listen_addr: String,
    coord_addr: String,
    /// User-managed list of known coordinator addresses (Worker mode picks one).
    coordinators: Vec<String>,
    new_coordinator_input: String,
    /// Other coordinators this coordinator peers with (consensus mesh).
    peer_coordinators: Vec<String>,
    new_peer_coordinator_input: String,
    my_name: String,
    my_balance_input: String,

    // Projects tab
    new_project_name: String,
    fund_project_id: String,
    fund_amount: String,
    donate_project_id: String,
    donate_amount: String,

    // Tasks tab
    task_project_id: String,
    task_project_selected: Option<u64>,
    task_reward: String,
    task_payload_mode: PayloadMode,
    task_code: String,
    task_filter: TaskFilter,

    // Batch submit
    batch_range_start: String,
    batch_range_end: String,
    batch_chunk_size: String,

    // Executor tab
    exec_reliability: u8,
    exec_compute_ticks: u64,

    // Packages tab — user-managed import allowlist for the isolated interpreter
    exec_allowed_packages: Vec<String>,
    exec_new_package_input: String,

    quick_demo_done: bool,
    quorum_input: String,

    // P2P consensus mode
    bootstrap_input: String,
    send_to_input: String,
    send_amount_input: String,
    p2p: Option<P2pSnapshot>,

    // Identity
    my_public_key_short: String,
    my_encryption_pubkey_short: String,
    my_encryption_pubkey: Option<[u8; 32]>,

    // Decrypt UI state
    decrypted_content: Option<String>,
    decrypt_error: Option<String>,
    decrypt_cache: std::collections::HashMap<u64, Result<String, String>>,
}

impl BoincApp {
    pub fn new(cmd_tx: mpsc::Sender<AppCommand>, evt_rx: mpsc::Receiver<AppEvent>) -> Self {
        let identity =
            crate::identity::Identity::load_or_generate(&crate::identity::Identity::default_path());
        let my_public_key_short = identity.public_key_short();
        let my_encryption_pubkey_short = identity.encryption_pubkey_short();
        let my_encryption_pubkey = Some(identity.encryption_pubkey_bytes());

        Self {
            cmd_tx,
            evt_rx,
            connected: false,
            my_id: None,
            snapshot: NetworkSnapshot::default(),
            log: VecDeque::new(),
            executor_running: false,
            active_tab: Tab::Connect,
            log_expanded: false,
            selected_task: None,
            node_mode: NodeMode::Coordinator,
            listen_addr: "0.0.0.0:7878".to_string(),
            coord_addr: "127.0.0.1:7878".to_string(),
            coordinators: vec![
                "127.0.0.1:7878".to_string(),
                "127.0.0.1:7879".to_string(),
                "127.0.0.1:7880".to_string(),
                "192.168.1.10:7878".to_string(),
            ],
            new_coordinator_input: String::new(),
            peer_coordinators: Vec::new(),
            new_peer_coordinator_input: String::new(),
            my_name: "Alice".to_string(),
            my_balance_input: "100".to_string(),
            new_project_name: String::new(),
            fund_project_id: String::new(),
            fund_amount: String::new(),
            donate_project_id: String::new(),
            donate_amount: String::new(),
            task_project_id: String::new(),
            task_project_selected: None,
            task_reward: "10".to_string(),
            task_payload_mode: PayloadMode::Python,
            task_code: "print('hello from BOINC')".to_string(),
            task_filter: TaskFilter::All,
            exec_reliability: 95,
            exec_compute_ticks: 2,
            exec_allowed_packages: crate::sandbox::default_allowed_packages(),
            exec_new_package_input: String::new(),
            quick_demo_done: false,
            quorum_input: "2".to_string(),
            bootstrap_input: "127.0.0.1:7878".to_string(),
            send_to_input: String::new(),
            send_amount_input: "10".to_string(),
            p2p: None,
            batch_range_start: "2".to_string(),
            batch_range_end: "10000".to_string(),
            batch_chunk_size: "1000".to_string(),
            my_public_key_short,
            my_encryption_pubkey_short,
            my_encryption_pubkey,
            decrypted_content: None,
            decrypt_error: None,
            decrypt_cache: std::collections::HashMap::new(),
        }
    }

    fn send(&self, cmd: AppCommand) {
        let _ = self.cmd_tx.try_send(cmd);
    }

    fn task_payload_for_submit(&self) -> String {
        match self.task_payload_mode {
            PayloadMode::Python => format!("python:{}", BASE64.encode(&self.task_code)),
            PayloadMode::GpuPython => format!("python-gpu:{}", BASE64.encode(&self.task_code)),
        }
    }

    fn selected_task_project_id(&self) -> Result<u64, &'static str> {
        if let Some(id) = self.task_project_selected {
            return Ok(id);
        }
        if self.task_project_id.trim().is_empty() {
            return Err("ERROR: select a project before submitting a task");
        }
        self.task_project_id
            .parse::<u64>()
            .map_err(|_| "ERROR: selected project id is invalid")
    }

    fn drain_events(&mut self) {
        while let Ok(evt) = self.evt_rx.try_recv() {
            match evt {
                AppEvent::Connected => {
                    self.connected = true;
                    self.log.push_back("Connected".to_string());
                }
                AppEvent::Disconnected { reason } => {
                    self.connected = false;
                    self.executor_running = false;
                    self.my_id = None;
                    self.log.push_back(format!("Disconnected: {reason}"));
                }
                AppEvent::Registered { participant_id } => {
                    self.my_id = Some(participant_id);
                    self.log
                        .push_back(format!("Registered as participant #{participant_id}"));
                }
                AppEvent::Identity {
                    signing_short,
                    encryption_short,
                } => {
                    // Reflect the identity the node actually runs under (derived
                    // from the connection), not the on-disk default.
                    self.my_public_key_short = signing_short;
                    self.my_encryption_pubkey_short = encryption_short;
                }
                AppEvent::StateUpdate(snap) => {
                    self.snapshot = snap;
                    self.decrypt_cache.clear();
                }
                AppEvent::P2pUpdate(snap) => {
                    // Feed the replicated chain into the shared snapshot so the
                    // Network tab renders it, and expose our own balance.
                    self.snapshot.blocks = snap.blocks.clone();
                    self.snapshot.block_count = snap.block_count;
                    self.snapshot.blockchain_valid = snap.blockchain_valid;
                    if let Some(id) = self.my_id {
                        self.snapshot.participants = vec![ParticipantView {
                            id,
                            name: self.my_name.clone(),
                            balance: snap.my_balance,
                            reputation: 1,
                        }];
                    }
                    self.p2p = Some(snap);
                }
                AppEvent::ExecutorStarted => {
                    self.executor_running = true;
                }
                AppEvent::ExecutorStopped => {
                    self.executor_running = false;
                }
                AppEvent::Log(msg) => {
                    self.log.push_back(msg);
                    if self.log.len() > 500 {
                        for _ in 0..100 {
                            self.log.pop_front();
                        }
                    }
                }
                AppEvent::Error(msg) => {
                    self.log.push_back(format!("ERROR: {msg}"));
                }
            }
        }
    }

    fn my_participant(&self) -> Option<&ParticipantView> {
        self.my_id
            .and_then(|id| self.snapshot.participants.iter().find(|p| p.id == id))
    }

    fn my_balance(&self) -> u64 {
        self.my_participant().map(|p| p.balance).unwrap_or(0)
    }

    fn my_reputation(&self) -> u64 {
        self.my_participant().map(|p| p.reputation).unwrap_or(0)
    }

    fn decrypt_task_result(&mut self, task: &TaskView) -> Option<String> {
        if let Some(cached) = self.decrypt_cache.get(&task.id) {
            return match cached {
                Ok(s) => Some(s.clone()),
                Err(_) => None,
            };
        }
        let blob = task.encrypted_result.as_ref()?;
        // Only decrypt if current user is project owner
        let project = self
            .snapshot
            .projects
            .iter()
            .find(|p| p.id == task.project_id)?;
        let my_id = self.my_id?;
        if project.owner_id != my_id {
            return None;
        }
        let identity =
            crate::identity::Identity::load_or_generate(&crate::identity::Identity::default_path());
        match identity.decrypt(blob) {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes).to_string();
                self.decrypt_cache.insert(task.id, Ok(text.clone()));
                Some(text)
            }
            Err(e) => {
                self.decrypt_cache.insert(task.id, Err(format!("{:?}", e)));
                None
            }
        }
    }
}

fn payload_preview(payload: &str, max_chars: usize) -> String {
    let mut chars = payload.chars();
    let preview: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{preview}…")
    } else {
        preview
    }
}

/// Bordered, scrollable list box of addresses (like an HTML `<select>`). When
/// `highlight` is set, the row equal to `selected` is highlighted and clicking a
/// row writes it into `selected`.
fn address_listbox(ui: &mut egui::Ui, list: &[String], selected: &mut String, highlight: bool) {
    egui::Frame::new()
        .fill(BG_APP)
        .stroke(Stroke::new(1.0, BG_CARD))
        .inner_margin(Margin::same(2i8))
        .show(ui, |ui| {
            egui::ScrollArea::vertical()
                .max_height(110.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    if list.is_empty() {
                        ui.add_space(6.0);
                        ui.label(RichText::new("— none —").size(11.0).color(C_MUTED));
                        ui.add_space(6.0);
                    }
                    for addr in list {
                        let sel = highlight && selected == addr;
                        if ui
                            .add_sized(
                                [ui.available_width(), 24.0],
                                Button::selectable(
                                    sel,
                                    RichText::new(addr)
                                        .monospace()
                                        .color(if sel { ACCENT } else { TEXT_PRI }),
                                ),
                            )
                            .clicked()
                        {
                            *selected = addr.clone();
                        }
                    }
                });
        });
}

/// Input + "Add" button that appends a trimmed, unique address to `list`. If
/// `select_into` is given, the added address becomes the selection.
fn add_address_row(
    ui: &mut egui::Ui,
    list: &mut Vec<String>,
    input: &mut String,
    select_into: Option<&mut String>,
    hint: &str,
) {
    ui.horizontal(|ui| {
        ui.add_sized(
            [ui.available_width() - 60.0, 28.0],
            egui::TextEdit::singleline(input).hint_text(hint),
        );
        if ui
            .add_sized(
                [56.0, 28.0],
                Button::new(RichText::new("Add").color(BG_APP)).fill(ACCENT),
            )
            .clicked()
        {
            let a = input.trim().to_string();
            if !a.is_empty() && !list.contains(&a) {
                list.push(a.clone());
                if let Some(s) = select_into {
                    *s = a;
                }
                input.clear();
            }
        }
    });
}

// ── eframe::App ───────────────────────────────────────────────────────────────
impl eframe::App for BoincApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_events();

        let ctx = ui.ctx().clone();
        ctx.request_repaint_after(Duration::from_millis(100));

        // Global visuals — applied every frame
        ctx.set_visuals({
            let mut v = egui::Visuals::dark();
            v.panel_fill = BG_APP;
            v.extreme_bg_color = BG_INPUT;
            v.faint_bg_color = BG_SURFACE;
            v.widgets.noninteractive.bg_fill = BG_SURFACE;
            v.widgets.noninteractive.fg_stroke = Stroke::new(1.0, TEXT_SEC);
            v.widgets.inactive.bg_fill = BG_CARD;
            v.widgets.inactive.fg_stroke = Stroke::new(1.0, TEXT_SEC);
            v.widgets.hovered.bg_fill = BG_CARD;
            v.widgets.hovered.fg_stroke = Stroke::new(1.0, ACCENT);
            v.widgets.active.bg_fill = ACCENT_DIM;
            v.widgets.active.fg_stroke = Stroke::new(1.0, TEXT_PRI);
            v.selection.bg_fill = Color32::from_rgba_unmultiplied(88, 166, 255, 40);
            v.selection.stroke = Stroke::new(1.0, ACCENT);
            v.widgets.noninteractive.corner_radius = CornerRadius::same(6u8);
            v.widgets.inactive.corner_radius = CornerRadius::same(6u8);
            v.widgets.hovered.corner_radius = CornerRadius::same(6u8);
            v.widgets.active.corner_radius = CornerRadius::same(6u8);
            v
        });
        ctx.global_style_mut(|s| {
            s.spacing.item_spacing = Vec2::new(8.0, 6.0);
            s.spacing.button_padding = Vec2::new(12.0, 6.0);
        });

        // ── Header (52 px) ────────────────────────────────────────────────────
        egui::Panel::top("header")
            .min_size(52.0)
            .frame(
                egui::Frame::new()
                    .fill(BG_SURFACE)
                    .inner_margin(Margin::symmetric(16i8, 8i8)),
            )
            .show_inside(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.label(
                        RichText::new("BOINC Quota")
                            .size(16.0)
                            .color(TEXT_PRI)
                            .strong(),
                    );
                    if self.connected {
                        ui.add_space(6.0);
                        match self.node_mode {
                            NodeMode::Coordinator => status_badge(
                                ui,
                                "Coordinator",
                                Color32::from_rgba_unmultiplied(63, 185, 80, 40),
                                C_SUCCESS,
                            ),
                            NodeMode::Worker => status_badge(
                                ui,
                                "Worker",
                                Color32::from_rgba_unmultiplied(210, 153, 34, 40),
                                C_WARNING,
                            ),
                            NodeMode::Peer => status_badge(
                                ui,
                                "P2P Peer",
                                Color32::from_rgba_unmultiplied(88, 166, 255, 40),
                                ACCENT,
                            ),
                        }
                    }
                    if let Some(id) = self.my_id {
                        let id_str = id.to_string();
                        let short = if id_str.len() > 8 {
                            format!("{}…", &id_str[..8])
                        } else {
                            id_str
                        };
                        ui.with_layout(
                            egui::Layout::centered_and_justified(egui::Direction::LeftToRight),
                            |ui| {
                                ui.label(
                                    RichText::new(format!("ID: {short}"))
                                        .size(12.0)
                                        .color(TEXT_SEC)
                                        .monospace(),
                                );
                            },
                        );
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if self.connected {
                            ui.label(RichText::new("● Connected").size(12.0).color(C_SUCCESS));
                            ui.add_space(8.0);
                            ui.label(
                                RichText::new(format!("★ {}", self.my_reputation()))
                                    .size(13.0)
                                    .color(TEXT_PRI),
                            );
                            ui.add_space(4.0);
                            ui.label(
                                RichText::new(format!("⚖ {}", self.my_balance()))
                                    .size(13.0)
                                    .color(ACCENT),
                            );
                        } else {
                            ui.label(RichText::new("● Disconnected").size(12.0).color(C_DANGER));
                        }
                    });
                });
            });

        // ── Redirect to Connect if active_tab isn't allowed in current mode ───
        let allowed: &[Tab] = match self.node_mode {
            NodeMode::Coordinator => &[Tab::Connect, Tab::Projects, Tab::Tasks, Tab::Network],
            NodeMode::Worker => &[Tab::Connect, Tab::Tasks, Tab::Executor, Tab::Network],
            NodeMode::Peer => &[Tab::Connect, Tab::Network],
        };
        if !allowed.contains(&self.active_tab) {
            self.active_tab = Tab::Connect;
        }

        // ── Tab bar (36 px) ───────────────────────────────────────────────────
        egui::Panel::top("tabs")
            .min_size(36.0)
            .frame(
                egui::Frame::new()
                    .fill(BG_SURFACE)
                    .inner_margin(Margin::symmetric(16i8, 4i8)),
            )
            .show_inside(ui, |ui| {
                let tabs: Vec<(Tab, &str)> = match self.node_mode {
                    NodeMode::Coordinator => vec![
                        (Tab::Connect, "Connect"),
                        (Tab::Projects, "Projects"),
                        (Tab::Tasks, "Tasks"),
                        (Tab::Network, "Network"),
                    ],
                    NodeMode::Worker => vec![
                        (Tab::Connect, "Connect"),
                        (Tab::Tasks, "Tasks"),
                        (Tab::Executor, "Executor"),
                        (Tab::Packages, "Packages"),
                        (Tab::Network, "Network"),
                    ],
                    NodeMode::Peer => vec![
                        (Tab::Connect, "Connect"),
                        (Tab::Network, "Ledger"),
                    ],
                };
                ui.horizontal(|ui| {
                    for (tab, name) in tabs {
                        let active = self.active_tab == tab;
                        let label = RichText::new(name).size(13.0).color(if active {
                            ACCENT
                        } else {
                            TEXT_SEC
                        });
                        if ui.selectable_label(active, label).clicked() {
                            self.active_tab = tab;
                        }
                    }
                });
                ui.separator();
            });

        // ── Log panel (bottom, collapsible) ───────────────────────────────────
        let log_count = self.log.len();
        let log_display: Vec<String> = {
            let start = log_count.saturating_sub(50);
            self.log.iter().skip(start).cloned().collect()
        };
        let log_height = if self.log_expanded { 140.0 } else { 28.0 };
        egui::Panel::bottom("log_panel")
            .min_size(log_height)
            .max_size(log_height)
            .frame(
                egui::Frame::new()
                    .fill(BG_SURFACE)
                    .inner_margin(Margin::symmetric(12i8, 4i8)),
            )
            .show_inside(ui, |ui| {
                ui.horizontal(|ui| {
                    let toggle_label = if self.log_expanded {
                        format!("▾ Log ({})", log_count)
                    } else {
                        format!("▸ Log ({})", log_count)
                    };
                    if ui
                        .small_button(RichText::new(toggle_label).size(11.0).color(TEXT_SEC))
                        .clicked()
                    {
                        self.log_expanded = !self.log_expanded;
                    }
                    if !self.log_expanded {
                        if let Some(last) = log_display.last() {
                            ui.label(
                                RichText::new(last)
                                    .size(11.0)
                                    .color(log_line_color(last))
                                    .monospace(),
                            );
                        }
                    }
                });
                if self.log_expanded {
                    egui::ScrollArea::vertical()
                        .id_salt("log_panel_scroll")
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            for line in &log_display {
                                ui.label(
                                    RichText::new(line)
                                        .size(11.0)
                                        .color(log_line_color(line))
                                        .monospace(),
                                );
                            }
                        });
                }
            });

        // ── Central panel ─────────────────────────────────────────────────────
        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(BG_APP)
                    .inner_margin(Margin::same(16i8)),
            )
            .show_inside(ui, |ui| match self.active_tab {
                Tab::Connect => {
                    egui::ScrollArea::vertical()
                        .id_salt("connect_scroll")
                        .auto_shrink([false, false])
                        .show(ui, |ui| self.draw_connect_tab(ui));
                }
                Tab::Projects => self.draw_projects_tab(ui),
                Tab::Tasks => self.draw_tasks_tab(ui),
                Tab::Executor => self.draw_executor_tab(ui),
                Tab::Packages => self.draw_packages_tab(ui),
                Tab::Network => self.draw_network_tab(ui),
            });
    }
}

// ── Tab implementations ───────────────────────────────────────────────────────
impl BoincApp {
    // ── Connect tab ──────────────────────────────────────────────────────────
    /// P2P-mode controls shown inside the Connection card: mesh stats, this
    /// node's ledger address, and a token-transfer form.
    fn draw_p2p_controls(&mut self, ui: &mut egui::Ui) {
        if let Some(p) = self.p2p.clone() {
            ui.horizontal(|ui| {
                ui.label(RichText::new(format!("Peers: {}", p.peers)).size(12.0).color(TEXT_SEC));
                ui.label(RichText::new(format!("Validators: {}", p.validators)).size(12.0).color(TEXT_SEC));
            });
            ui.horizontal(|ui| {
                ui.label(RichText::new(format!("Mempool: {}", p.mempool)).size(12.0).color(TEXT_SEC));
                ui.label(RichText::new(format!("Blocks: {}", p.block_count)).size(12.0).color(TEXT_SEC));
                let (txt, col) = if p.blockchain_valid {
                    ("✓ chain valid", C_SUCCESS)
                } else {
                    ("✗ chain invalid", C_DANGER)
                };
                ui.label(RichText::new(txt).size(12.0).color(col));
            });
        }
        if let Some(id) = self.my_id {
            ui.horizontal(|ui| {
                ui.label(RichText::new("Your address:").size(11.0).color(TEXT_SEC));
                ui.label(RichText::new(id.to_string()).size(11.0).color(ACCENT).monospace());
            });
        }
        ui.add_space(8.0);
        ui.label(RichText::new("Send tokens").size(12.0).color(TEXT_PRI).strong());
        ui.horizontal(|ui| {
            ui.label(RichText::new("To (addr):").size(11.0).color(TEXT_SEC));
            ui.add_sized([180.0, 26.0], egui::TextEdit::singleline(&mut self.send_to_input));
        });
        ui.horizontal(|ui| {
            ui.label(RichText::new("Amount:").size(11.0).color(TEXT_SEC));
            ui.add_sized([100.0, 26.0], egui::TextEdit::singleline(&mut self.send_amount_input));
        });
        if ui.add_sized(
            [ui.available_width(), 30.0],
            Button::new(RichText::new("Submit transfer").color(BG_APP)).fill(ACCENT),
        ).clicked() {
            if let (Ok(to), Ok(amount)) = (
                self.send_to_input.trim().parse::<u64>(),
                self.send_amount_input.trim().parse::<u64>(),
            ) {
                self.send(AppCommand::SendTokens { to, amount });
            }
        }
        ui.add_space(8.0);
    }

    fn draw_connect_tab(&mut self, ui: &mut egui::Ui) {
        // Identity info always visible at top
        card_frame().show(ui, |ui| {
            ui.label(
                RichText::new("Your Identity (Ed25519 signing key):")
                    .size(11.0)
                    .color(TEXT_SEC),
            );
            ui.horizontal(|ui| {
                ui.monospace(RichText::new(&self.my_public_key_short).color(ACCENT));
                ui.label(
                    RichText::new("— stored in ~/.boinc-quota/identity.json")
                        .size(11.0)
                        .color(C_MUTED),
                );
            });
            ui.label(
                RichText::new("X25519 encryption key:")
                    .size(11.0)
                    .color(TEXT_SEC),
            );
            ui.horizontal(|ui| {
                ui.monospace(RichText::new(&self.my_encryption_pubkey_short).color(ACCENT));
                ui.label(
                    RichText::new("— workers encrypt task results to this key")
                        .size(11.0)
                        .color(C_MUTED),
                );
            });
        });
        ui.add_space(8.0);

        ui.columns(2, |cols| {
            card_frame().show(&mut cols[0], |ui| {
                section_header(ui, "Connection");
                if self.connected {
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Status:").color(TEXT_SEC).size(12.0));
                        match self.node_mode {
                            NodeMode::Coordinator => status_badge(
                                ui, "Coordinator",
                                Color32::from_rgba_unmultiplied(63, 185, 80, 40), C_SUCCESS,
                            ),
                            NodeMode::Worker => status_badge(
                                ui, "Worker",
                                Color32::from_rgba_unmultiplied(210, 153, 34, 40), C_WARNING,
                            ),
                            NodeMode::Peer => status_badge(
                                ui, "P2P Peer",
                                Color32::from_rgba_unmultiplied(88, 166, 255, 40), ACCENT,
                            ),
                        }
                    });
                    ui.add_space(8.0);
                    if self.node_mode == NodeMode::Peer {
                        self.draw_p2p_controls(ui);
                    }
                    if self.node_mode != NodeMode::Peer && !self.quick_demo_done {
                        if ui.add_sized(
                            [ui.available_width(), 32.0],
                            Button::new(RichText::new("⚡ Quick Demo Setup").color(BG_APP)).fill(C_WARNING),
                        ).clicked() {
                            self.send(AppCommand::CreateProject { name: "Demo".to_string(), owner_encryption_pubkey: self.my_encryption_pubkey });
                            self.send(AppCommand::FundProject { project_id: 1, amount: 50 });
                            let payload = format!("python:{}", BASE64.encode("print('hello from BOINC')"));
                            self.send(AppCommand::SubmitTask {
                                project_id: 1, reward: 20, payload,
                            });
                            self.quick_demo_done = true;
                        }
                        ui.label(RichText::new("Creates project #1, funds 50, submits Python task reward=20").size(11.0).color(TEXT_SEC));
                        ui.add_space(8.0);
                    }
                    if ui.add_sized(
                        [ui.available_width(), 36.0],
                        Button::new(RichText::new("Disconnect").color(TEXT_PRI)).fill(C_DANGER),
                    ).clicked() {
                        self.send(AppCommand::Disconnect);
                    }
                } else {
                    ui.horizontal(|ui| {
                        let coord = self.node_mode == NodeMode::Coordinator;
                        if ui.add_sized([90.0, 30.0], Button::new(
                            RichText::new("Coordinator").color(if coord { BG_APP } else { TEXT_SEC }),
                        ).fill(if coord { ACCENT } else { BG_CARD })).clicked() {
                            self.node_mode = NodeMode::Coordinator;
                        }
                        let worker = self.node_mode == NodeMode::Worker;
                        if ui.add_sized([70.0, 30.0], Button::new(
                            RichText::new("Worker").color(if worker { BG_APP } else { TEXT_SEC }),
                        ).fill(if worker { ACCENT } else { BG_CARD })).clicked() {
                            self.node_mode = NodeMode::Worker;
                        }
                    });
                    ui.add_space(8.0);
                    match self.node_mode {
                        NodeMode::Coordinator => {
                            ui.label(RichText::new("Listen address").size(11.0).color(TEXT_SEC));
                            ui.add_sized([ui.available_width(), 28.0], egui::TextEdit::singleline(&mut self.listen_addr));
                            ui.add_space(6.0);
                            ui.label(RichText::new("Peer coordinators").size(11.0).color(TEXT_SEC));
                            ui.add_space(2.0);
                            let mut scratch = String::new();
                            address_listbox(ui, &self.peer_coordinators, &mut scratch, false);
                        }
                        NodeMode::Worker => {
                            ui.label(RichText::new("Coordinators").size(11.0).color(TEXT_SEC));
                            ui.add_space(2.0);
                            address_listbox(ui, &self.coordinators, &mut self.coord_addr, true);
                        }
                        NodeMode::Peer => {
                            ui.label(RichText::new("Listen address").size(11.0).color(TEXT_SEC));
                            ui.add_sized([ui.available_width(), 28.0], egui::TextEdit::singleline(&mut self.listen_addr));
                            ui.add_space(4.0);
                            ui.label(RichText::new("Bootstrap peers (comma-separated, blank for first node)").size(11.0).color(TEXT_SEC));
                            ui.add_sized([ui.available_width(), 28.0], egui::TextEdit::singleline(&mut self.bootstrap_input));
                        }
                    }
                    ui.add_space(4.0);
                    ui.label(RichText::new("Your name").size(11.0).color(TEXT_SEC));
                    ui.add_sized([ui.available_width(), 28.0], egui::TextEdit::singleline(&mut self.my_name));
                    ui.add_space(4.0);
                    ui.label(RichText::new("Initial balance").size(11.0).color(TEXT_SEC));
                    ui.add_sized([ui.available_width(), 28.0], egui::TextEdit::singleline(&mut self.my_balance_input));
                    match self.node_mode {
                        NodeMode::Worker => {
                            ui.add_space(8.0);
                            add_address_row(
                                ui,
                                &mut self.coordinators,
                                &mut self.new_coordinator_input,
                                Some(&mut self.coord_addr),
                                "add coordinator host:port",
                            );
                        }
                        NodeMode::Coordinator => {
                            ui.add_space(8.0);
                            add_address_row(
                                ui,
                                &mut self.peer_coordinators,
                                &mut self.new_peer_coordinator_input,
                                None,
                                "add peer coordinator host:port",
                            );
                        }
                        NodeMode::Peer => {}
                    }
                    ui.add_space(10.0);
                    if ui.add_sized(
                        [ui.available_width(), 36.0],
                        Button::new(RichText::new("Connect").color(BG_APP)).fill(ACCENT),
                    ).clicked() {
                        let balance = self.my_balance_input.parse::<u64>().unwrap_or(100);
                        let quorum = self.quorum_input.parse::<u64>().unwrap_or(2);
                        match self.node_mode {
                            NodeMode::Coordinator => self.send(AppCommand::ConnectCoordinator {
                                listen_addr: self.listen_addr.clone(),
                                name: self.my_name.clone(), balance, quorum,
                                peer_coordinators: self.peer_coordinators.clone(),
                            }),
                            NodeMode::Worker => self.send(AppCommand::ConnectWorker {
                                coord_addr: self.coord_addr.clone(),
                                name: self.my_name.clone(), balance,
                            }),
                            NodeMode::Peer => {
                                let bootstrap_peers = self.bootstrap_input
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect();
                                self.send(AppCommand::JoinP2P {
                                    listen_addr: self.listen_addr.clone(),
                                    bootstrap_peers,
                                    name: self.my_name.clone(),
                                    balance,
                                });
                            }
                        }
                    }
                }
            });

            if self.connected {
                card_frame().show(&mut cols[1], |ui| {
                    section_header(ui, "Node Info");
                    let addr = match self.node_mode {
                        NodeMode::Coordinator => self.listen_addr.clone(),
                        NodeMode::Worker => self.coord_addr.clone(),
                        NodeMode::Peer => self.listen_addr.clone(),
                    };
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Address:").color(TEXT_SEC).size(12.0));
                        ui.label(RichText::new(&addr).color(TEXT_PRI).size(12.0).monospace());
                    });
                    if let Some(p) = self.my_participant().cloned() {
                        ui.horizontal(|ui| {
                            ui.label(RichText::new("Name:").color(TEXT_SEC).size(12.0));
                            ui.label(RichText::new(&p.name).color(TEXT_PRI).size(12.0));
                        });
                        ui.horizontal(|ui| {
                            ui.label(RichText::new("Balance:").color(TEXT_SEC).size(12.0));
                            ui.label(RichText::new(p.balance.to_string()).color(ACCENT).size(12.0));
                        });
                        ui.horizontal(|ui| {
                            ui.label(RichText::new("Reputation:").color(TEXT_SEC).size(12.0));
                            ui.label(RichText::new(format!("★ {}", p.reputation)).color(TEXT_PRI).size(12.0));
                        });
                    }
                });
            } else {
                card_frame().show(&mut cols[1], |ui| {
                    section_header(ui, "About Modes");
                    ui.label(RichText::new("Coordinator").color(C_SUCCESS).size(12.0).strong());
                    ui.label(RichText::new("Runs the server, manages the blockchain and task queue. Start this first.").color(TEXT_SEC).size(12.0));
                    ui.add_space(8.0);
                    ui.label(RichText::new("Worker").color(C_WARNING).size(12.0).strong());
                    ui.label(RichText::new("Connects to a coordinator, picks up and executes tasks, earns rewards.").color(TEXT_SEC).size(12.0));
                });
            }
        });
    }

    // ── Projects tab ─────────────────────────────────────────────────────────
    fn draw_projects_tab(&mut self, ui: &mut egui::Ui) {
        if !self.connected {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("Not connected.").color(C_MUTED).size(14.0));
            });
            return;
        }
        ui.columns(2, |cols| {
            egui::ScrollArea::vertical()
                .id_salt("projects_forms_scroll")
                .show(&mut cols[0], |ui| {
                    card_frame().show(ui, |ui| {
                        section_header(ui, "New Project");
                        ui.label(RichText::new("Project name").size(11.0).color(TEXT_SEC));
                        ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.new_project_name),
                        );
                        ui.add_space(6.0);
                        if ui
                            .add_sized(
                                [ui.available_width(), 32.0],
                                Button::new(RichText::new("Create Project").color(BG_APP))
                                    .fill(ACCENT),
                            )
                            .clicked()
                            && !self.new_project_name.is_empty()
                        {
                            self.send(AppCommand::CreateProject {
                                name: self.new_project_name.clone(),
                                owner_encryption_pubkey: self.my_encryption_pubkey,
                            });
                            self.new_project_name.clear();
                        }
                    });
                    ui.add_space(10.0);
                    card_frame().show(ui, |ui| {
                        section_header(ui, "Fund Project");
                        ui.label(RichText::new("Project ID").size(11.0).color(TEXT_SEC));
                        ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.fund_project_id),
                        );
                        ui.add_space(4.0);
                        ui.label(RichText::new("Amount").size(11.0).color(TEXT_SEC));
                        ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.fund_amount),
                        );
                        ui.add_space(6.0);
                        if ui
                            .add_sized(
                                [ui.available_width(), 32.0],
                                Button::new(RichText::new("Fund").color(BG_APP)).fill(ACCENT),
                            )
                            .clicked()
                        {
                            if let (Ok(pid), Ok(amt)) = (
                                self.fund_project_id.parse::<u64>(),
                                self.fund_amount.parse::<u64>(),
                            ) {
                                self.send(AppCommand::FundProject {
                                    project_id: pid,
                                    amount: amt,
                                });
                            }
                        }
                    });
                    ui.add_space(10.0);
                    card_frame().show(ui, |ui| {
                        section_header(ui, "Donate Quota");
                        ui.label(RichText::new("Project ID").size(11.0).color(TEXT_SEC));
                        ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.donate_project_id),
                        );
                        ui.add_space(4.0);
                        ui.label(RichText::new("Amount").size(11.0).color(TEXT_SEC));
                        ui.add_sized(
                            [ui.available_width(), 28.0],
                            egui::TextEdit::singleline(&mut self.donate_amount),
                        );
                        ui.add_space(6.0);
                        if ui
                            .add_sized(
                                [ui.available_width(), 32.0],
                                Button::new(RichText::new("Donate").color(BG_APP)).fill(ACCENT),
                            )
                            .clicked()
                        {
                            if let (Ok(pid), Ok(amt)) = (
                                self.donate_project_id.parse::<u64>(),
                                self.donate_amount.parse::<u64>(),
                            ) {
                                self.send(AppCommand::DonateToProject {
                                    project_id: pid,
                                    amount: amt,
                                });
                            }
                        }
                    });
                });

            let projects = self.snapshot.projects.clone();
            let hdr = format!("Projects ({})", projects.len());
            section_header(&mut cols[1], &hdr);
            egui::ScrollArea::vertical()
                .id_salt("projects_scroll")
                .show(&mut cols[1], |ui| {
                    if projects.is_empty() {
                        ui.label(RichText::new("No projects yet.").color(C_MUTED).size(13.0));
                    } else {
                        for p in &projects {
                            card_frame().show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label(
                                        RichText::new(&p.name).color(TEXT_PRI).size(13.0).strong(),
                                    );
                                    ui.label(
                                        RichText::new(format!("#{}", p.id))
                                            .color(C_MUTED)
                                            .size(11.0)
                                            .monospace(),
                                    );
                                });
                                ui.add_space(4.0);
                                let total = p.quota_available + p.quota_locked;
                                ui.horizontal(|ui| {
                                    ui.label(
                                        RichText::new("Available:").size(11.0).color(TEXT_SEC),
                                    );
                                    let frac = if total > 0 {
                                        p.quota_available as f32 / total as f32
                                    } else {
                                        0.0
                                    };
                                    ui.add(
                                        egui::ProgressBar::new(frac)
                                            .desired_width(100.0)
                                            .fill(C_SUCCESS),
                                    );
                                    ui.label(
                                        RichText::new(p.quota_available.to_string())
                                            .size(11.0)
                                            .color(C_SUCCESS),
                                    );
                                });
                                ui.horizontal(|ui| {
                                    ui.label(RichText::new("Locked:  ").size(11.0).color(TEXT_SEC));
                                    let frac = if total > 0 {
                                        p.quota_locked as f32 / total as f32
                                    } else {
                                        0.0
                                    };
                                    ui.add(
                                        egui::ProgressBar::new(frac)
                                            .desired_width(100.0)
                                            .fill(C_WARNING),
                                    );
                                    ui.label(
                                        RichText::new(p.quota_locked.to_string())
                                            .size(11.0)
                                            .color(C_WARNING),
                                    );
                                });
                            });
                            ui.add_space(6.0);
                        }
                    }
                });
        });
    }

    // ── Tasks tab ────────────────────────────────────────────────────────────
    fn draw_tasks_tab(&mut self, ui: &mut egui::Ui) {
        if !self.connected {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("Not connected.").color(C_MUTED).size(14.0));
            });
            return;
        }

        if self.node_mode == NodeMode::Worker {
            self.draw_worker_tasks_tab(ui);
            return;
        }

        // Filter bar
        ui.horizontal(|ui| {
            for (filter, label) in [
                (TaskFilter::All, "All"),
                (TaskFilter::Pending, "Pending"),
                (TaskFilter::Assigned, "Assigned"),
                (TaskFilter::Completed, "Completed"),
                (TaskFilter::QuotaExhausted, "Quota"),
                (TaskFilter::Rejected, "Rejected"),
            ] {
                let active = self.task_filter == filter;
                if ui
                    .add(
                        Button::new(RichText::new(label).size(12.0).color(if active {
                            BG_APP
                        } else {
                            TEXT_SEC
                        }))
                        .fill(if active { ACCENT } else { BG_CARD }),
                    )
                    .clicked()
                {
                    self.task_filter = filter;
                }
            }
        });
        ui.add_space(8.0);

        ui.columns(2, |cols| {
            egui::ScrollArea::vertical().id_salt("task_form_scroll").show(&mut cols[0], |ui| {
                card_frame().show(ui, |ui| {
                    section_header(ui, "Submit Task");

                    let projects = self.snapshot.projects.clone();
                    let selected_label = self.task_project_selected
                        .and_then(|id| projects.iter().find(|p| p.id == id))
                        .map(|p| format!("#{} {}", p.id, p.name))
                        .unwrap_or_else(|| {
                            if self.task_project_id.is_empty() { "— select —".to_string() }
                            else { format!("#{}", self.task_project_id) }
                        });

                    ui.label(RichText::new("Project").size(11.0).color(TEXT_SEC));
                    egui::ComboBox::from_id_salt("task_project_combo")
                        .width(ui.available_width())
                        .selected_text(selected_label)
                        .show_ui(ui, |ui| {
                            for p in &projects {
                                let lbl = format!("#{} {}", p.id, p.name);
                                let sel = self.task_project_selected == Some(p.id);
                                if ui.selectable_label(sel, &lbl).clicked() {
                                    self.task_project_selected = Some(p.id);
                                    self.task_project_id = p.id.to_string();
                                }
                            }
                        });

                    ui.add_space(4.0);
                    ui.label(RichText::new("Payload type").size(11.0).color(TEXT_SEC));
                    ui.horizontal(|ui| {
                        let py_on = self.task_payload_mode == PayloadMode::Python;
                        if ui.add_sized([80.0, 28.0], Button::new(
                            RichText::new("Python").color(if py_on { BG_APP } else { TEXT_SEC }),
                        ).fill(if py_on { ACCENT } else { BG_CARD })).clicked() {
                            self.task_payload_mode = PayloadMode::Python;
                        }
                        let gpu_on = self.task_payload_mode == PayloadMode::GpuPython;
                        if ui.add_sized([80.0, 28.0], Button::new(
                            RichText::new("GPU").color(if gpu_on { BG_APP } else { TEXT_SEC }),
                        ).fill(if gpu_on { ACCENT } else { BG_CARD })).clicked() {
                            self.task_payload_mode = PayloadMode::GpuPython;
                        }
                    });

                    ui.add_space(4.0);
                    {
                        let is_gpu = self.task_payload_mode == PayloadMode::GpuPython;
                        if is_gpu {
                            ui.label(RichText::new("GPU mode: runs in pytorch Docker with --gpus all").size(11.0).color(Color32::from_rgb(120, 200, 255)));
                        }
                        ui.label(RichText::new("Python code").size(11.0).color(TEXT_SEC));
                        egui::ScrollArea::vertical()
                            .id_salt("code_editor_scroll")
                            .max_height(120.0)
                            .show(ui, |ui| {
                                ui.add(egui::TextEdit::multiline(&mut self.task_code)
                                    .font(egui::TextStyle::Monospace)
                                    .desired_width(f32::INFINITY)
                                    .desired_rows(6));
                            });
                        ui.horizontal(|ui| {
                            if !is_gpu {
                                if ui.small_button("Prime check").clicked() { self.task_code = PRIME_CHECK_TEMPLATE.to_string(); }
                                if ui.small_button("Prime chunk").clicked() { self.task_code = PRIME_CHUNK_TEMPLATE.to_string(); }
                            } else {
                                if ui.small_button("PyTorch matmul").clicked() { self.task_code = GPU_PYTORCH_TEMPLATE.to_string(); }
                            }
                        });
                        ui.label(RichText::new(format!("Will encode {} bytes with base64", self.task_code.len())).size(10.0).color(C_MUTED));
                    }

                    ui.add_space(4.0);
                    ui.label(RichText::new("Reward").size(11.0).color(TEXT_SEC));
                    ui.add_sized([ui.available_width(), 28.0], egui::TextEdit::singleline(&mut self.task_reward));
                    ui.add_space(8.0);

                    if ui.add_sized([ui.available_width(), 36.0],
                        Button::new(RichText::new("Submit Task").color(BG_APP)).fill(ACCENT),
                    ).clicked() {
                        match (self.selected_task_project_id(), self.task_reward.parse::<u64>()) {
                            (Ok(pid), Ok(reward)) => {
                                let payload = self.task_payload_for_submit();
                                self.send(AppCommand::SubmitTask { project_id: pid, reward, payload });
                            }
                            (Err(msg), _) => self.log.push_back(msg.to_string()),
                            (_, Err(_)) => self.log.push_back("ERROR: task reward must be a number".to_string()),
                        }
                    }
                });

                ui.add_space(10.0);
                ui.collapsing(RichText::new("Batch Submit (Prime Sieve)").size(12.0).color(TEXT_SEC), |ui| {
                    card_frame().show(ui, |ui| {
                        ui.label(RichText::new("Splits range into chunks, one task per chunk.").size(11.0).color(TEXT_SEC));
                        ui.add_space(4.0);
                        ui.label(RichText::new("Start").size(11.0).color(TEXT_SEC));
                        ui.add_sized([ui.available_width(), 26.0], egui::TextEdit::singleline(&mut self.batch_range_start));
                        ui.label(RichText::new("End").size(11.0).color(TEXT_SEC));
                        ui.add_sized([ui.available_width(), 26.0], egui::TextEdit::singleline(&mut self.batch_range_end));
                        ui.label(RichText::new("Chunk size").size(11.0).color(TEXT_SEC));
                        ui.add_sized([ui.available_width(), 26.0], egui::TextEdit::singleline(&mut self.batch_chunk_size));
                        ui.add_space(6.0);
                        if ui.add_sized([ui.available_width(), 32.0],
                            Button::new(RichText::new("Batch Submit").color(BG_APP)).fill(ACCENT_DIM),
                        ).clicked() {
                            if let (Ok(start), Ok(end), Ok(chunk), Ok(reward)) = (
                                self.batch_range_start.parse::<u64>(),
                                self.batch_range_end.parse::<u64>(),
                                self.batch_chunk_size.parse::<u64>(),
                                self.task_reward.parse::<u64>(),
                            ) {
                                match self.selected_task_project_id() {
                                    Ok(pid) => {
                                    let mut n = start;
                                    let mut count = 0u32;
                                    while n < end {
                                        let chunk_end = (n + chunk).min(end);
                                        let code = format!(
                                            "start,end={},{}\nprimes=[]\nfor n in range(max(2,start),end):\n    ok=all(n%i!=0 for i in range(2,int(n**0.5)+1))\n    if ok:primes.append(n)\nprint(','.join(map(str,primes)) or 'none')\n",
                                            n, chunk_end
                                        );
                                        let payload = format!("python:{}", BASE64.encode(&code));
                                        self.send(AppCommand::SubmitTask { project_id: pid, reward, payload });
                                        n = chunk_end;
                                        count += 1;
                                    }
                                    self.log.push_back(format!("Batch: submitted {count} prime-check tasks"));
                                    }
                                    Err(msg) => self.log.push_back(msg.to_string()),
                                }
                            } else {
                                self.log.push_back("ERROR: batch start/end/chunk and reward must be numbers".to_string());
                            }
                        }
                    });
                });
            });

            // Right: filtered task list
            let tasks: Vec<_> = {
                let filter = &self.task_filter;
                self.snapshot.tasks.iter().filter(|t| match filter {
                    TaskFilter::All       => true,
                    TaskFilter::Pending   => t.status_label == "Pending",
                    TaskFilter::Assigned  => t.status_label.starts_with("Assigned"),
                    TaskFilter::Completed => t.status_label.starts_with("Done"),
                    TaskFilter::QuotaExhausted => t.status_label == "Quota exhausted",
                    TaskFilter::Rejected  => t.status_label == "Rejected",
                }).cloned().collect()
            };
            let list_hdr = format!("Tasks ({})", tasks.len());
            section_header(&mut cols[1], &list_hdr);
            egui::ScrollArea::vertical().id_salt("tasks_scroll").show(&mut cols[1], |ui| {
                if tasks.is_empty() {
                    ui.label(RichText::new("No tasks match filter.").color(C_MUTED).size(13.0));
                } else {
                    for t in &tasks {
                        let (badge_bg, badge_fg) = task_status_colors(&t.status_label);
                        let result_text: Option<String> =
                            if t.status_label.starts_with("Done") || t.status_label == "Quota exhausted" {
                            self.decrypt_task_result(t)
                                .map(|s| s.lines().next().unwrap_or("").chars().take(60).collect::<String>())
                        } else {
                            None
                        };
                        let response = card_frame()
                            .show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    status_badge(ui, &t.status_label, badge_bg, badge_fg);
                                    ui.label(RichText::new(format!("Task #{}", t.id)).color(TEXT_PRI).size(12.0).strong());
                                    ui.label(RichText::new(format!("→ Project #{}", t.project_id)).color(TEXT_SEC).size(12.0));
                                    if t.has_encrypted_result {
                                        ui.label(RichText::new("🔒").color(C_WARNING).size(12.0));
                                    }
                                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                        ui.label(RichText::new(format!("⚖ {}", t.reward)).color(ACCENT).size(12.0));
                                    });
                                });
                                let preview = if t.payload.len() > 60 { format!("{}…", &t.payload[..60]) } else { t.payload.clone() };
                                ui.label(RichText::new(preview).size(11.0)
                                    .color(Color32::from_rgba_unmultiplied(139, 148, 158, 180))
                                    .monospace());
                                // Result row
                                ui.horizontal(|ui| {
                                    ui.label(RichText::new("Result:").size(11.0).color(TEXT_SEC));
                                    match &result_text {
                                        Some(s) => {
                                            ui.label(RichText::new(s).size(11.0)
                                                .color(Color32::from_rgb(80, 200, 80))
                                                .monospace());
                                        }
                                        None if t.has_encrypted_result => {
                                            ui.label(RichText::new("🔒").size(11.0).color(C_MUTED));
                                        }
                                        None => {
                                            ui.label(RichText::new("—").size(11.0).color(C_MUTED));
                                        }
                                    }
                                });
                            })
                            .response
                            .interact(egui::Sense::click());
                        if response.clicked() {
                            self.selected_task = Some(t.id);
                            self.decrypted_content = None;
                            self.decrypt_error = None;
                        }
                        ui.add_space(4.0);
                    }
                }
            });
        });

        // Selected task detail panel — shown below the two columns.
        if let Some(sel_id) = self.selected_task {
            let task_clone = self.snapshot.tasks.iter().find(|t| t.id == sel_id).cloned();
            if let Some(t) = task_clone {
                ui.add_space(10.0);
                let project_owner_id = self
                    .snapshot
                    .projects
                    .iter()
                    .find(|p| p.id == t.project_id)
                    .map(|p| p.owner_id);
                let is_owner = project_owner_id.is_some() && project_owner_id == self.my_id;

                card_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            RichText::new(format!("Task #{} details", t.id))
                                .color(TEXT_PRI)
                                .size(14.0)
                                .strong(),
                        );
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.small_button("close").clicked() {
                                self.selected_task = None;
                                self.decrypted_content = None;
                                self.decrypt_error = None;
                            }
                        });
                    });
                    ui.label(
                        RichText::new(format!("Status: {}", t.status_label))
                            .size(12.0)
                            .color(TEXT_SEC),
                    );
                    ui.label(
                        RichText::new(format!("Project: #{}  Reward: {}", t.project_id, t.reward))
                            .size(12.0)
                            .color(TEXT_SEC),
                    );
                    ui.add_space(4.0);
                    ui.label(RichText::new("Payload:").size(11.0).color(TEXT_SEC));
                    let preview = if t.payload.len() > 200 {
                        format!("{}…", &t.payload[..200])
                    } else {
                        t.payload.clone()
                    };
                    ui.label(
                        RichText::new(preview)
                            .size(11.0)
                            .color(TEXT_PRI)
                            .monospace(),
                    );

                    ui.add_space(8.0);
                    if t.has_encrypted_result {
                        ui.label(
                            RichText::new("🔒 Encrypted result available")
                                .size(12.0)
                                .color(C_WARNING),
                        );
                        if is_owner {
                            if ui
                                .add(
                                    Button::new(RichText::new("Decrypt result").color(BG_APP))
                                        .fill(ACCENT),
                                )
                                .clicked()
                            {
                                let identity = crate::identity::Identity::load_or_generate(
                                    &crate::identity::Identity::default_path(),
                                );
                                if let Some(blob) = &t.encrypted_result {
                                    match identity.decrypt(blob) {
                                        Ok(bytes) => {
                                            self.decrypted_content =
                                                Some(String::from_utf8_lossy(&bytes).to_string());
                                            self.decrypt_error = None;
                                        }
                                        Err(e) => {
                                            self.decrypt_error = Some(format!("{e:?}"));
                                            self.decrypted_content = None;
                                        }
                                    }
                                } else {
                                    self.decrypt_error =
                                        Some("no encrypted blob attached".to_string());
                                }
                            }
                        } else {
                            ui.label(
                                RichText::new("Only the project owner can decrypt this result.")
                                    .size(11.0)
                                    .color(C_MUTED),
                            );
                        }

                        if let Some(err) = &self.decrypt_error {
                            ui.label(
                                RichText::new(format!("Decrypt error: {err}"))
                                    .size(11.0)
                                    .color(C_DANGER),
                            );
                        }
                        if let Some(content) = &self.decrypted_content {
                            ui.add_space(4.0);
                            ui.label(RichText::new("Plaintext:").size(11.0).color(TEXT_SEC));
                            let mut text = content.clone();
                            egui::ScrollArea::vertical()
                                .id_salt("decrypted_scroll")
                                .max_height(160.0)
                                .show(ui, |ui| {
                                    ui.add(
                                        egui::TextEdit::multiline(&mut text)
                                            .font(egui::TextStyle::Monospace)
                                            .desired_width(f32::INFINITY)
                                            .desired_rows(8)
                                            .interactive(false),
                                    );
                                });
                        }
                    } else {
                        ui.label(
                            RichText::new("No encrypted result attached.")
                                .size(11.0)
                                .color(C_MUTED),
                        );
                    }
                });
            }
        }
    }

    fn draw_worker_tasks_tab(&mut self, ui: &mut egui::Ui) {
        let my_id = self.my_id;
        let tasks: Vec<_> = self
            .snapshot
            .tasks
            .iter()
            .filter(|t| {
                t.assigned_worker_id == my_id
                    || my_id.is_some_and(|id| t.reported_worker_ids.contains(&id))
            })
            .cloned()
            .collect();

        let active_count = tasks
            .iter()
            .filter(|task| task.assigned_worker_id == my_id)
            .count();
        let header = format!(
            "Worker Tasks ({active_count} active / {} total)",
            tasks.len()
        );
        section_header(ui, &header);

        egui::ScrollArea::vertical()
            .id_salt("worker_tasks_scroll")
            .show(ui, |ui| {
                if tasks.is_empty() {
                    card_frame().show(ui, |ui| {
                        ui.label(
                            RichText::new("No tasks are currently assigned to this worker.")
                                .size(13.0)
                                .color(C_MUTED),
                        );
                        ui.label(
                            RichText::new(
                                "The executor picks up pending work automatically while it is running.",
                            )
                            .size(11.0)
                            .color(TEXT_SEC),
                        );
                    });
                    return;
                }

                for task in tasks {
                    let (badge_bg, badge_fg) = task_status_colors(&task.status_label);
                    card_frame().show(ui, |ui| {
                        ui.horizontal(|ui| {
                            status_badge(ui, &task.status_label, badge_bg, badge_fg);
                            ui.label(
                                RichText::new(format!("Task #{}", task.id))
                                    .color(TEXT_PRI)
                                    .size(13.0)
                                    .strong(),
                            );
                            ui.label(
                                RichText::new(format!("Project #{}", task.project_id))
                                    .color(TEXT_SEC)
                                    .size(12.0),
                            );
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                ui.label(
                                    RichText::new(format!("Reward {}", task.reward))
                                        .color(ACCENT)
                                        .size(12.0),
                                );
                            });
                        });
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new(payload_preview(&task.payload, 180))
                                .size(11.0)
                                .color(Color32::from_rgba_unmultiplied(139, 148, 158, 180))
                                .monospace(),
                        );
                    });
                    ui.add_space(6.0);
                }
            });
    }

    // ── Executor tab ─────────────────────────────────────────────────────────
    fn draw_executor_tab(&mut self, ui: &mut egui::Ui) {
        if !self.connected {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("Not connected.").color(C_MUTED).size(14.0));
            });
            return;
        }

        card_frame()
            .fill(Color32::from_rgba_unmultiplied(210, 153, 34, 20))
            .show(ui, |ui| {
                ui.label(RichText::new("ℹ Workers start executor automatically after connecting. Coordinators can still run one manually for local validation.")
                    .size(12.0).color(C_WARNING));
            });
        ui.add_space(10.0);

        card_frame().show(ui, |ui| {
            section_header(ui, "Execution Controls");
            ui.horizontal(|ui| {
                ui.label(RichText::new("Reliability:").size(12.0).color(TEXT_SEC));
                ui.add(egui::Slider::new(&mut self.exec_reliability, 0..=100).suffix("%"));
            });
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new("Compute ticks:").size(12.0).color(TEXT_SEC));
                let mut val = self.exec_compute_ticks as u32;
                ui.add(egui::Slider::new(&mut val, 1..=20));
                self.exec_compute_ticks = val as u64;
            });
            ui.add_space(12.0);
            if self.executor_running {
                let t = ui.ctx().input(|i| i.time);
                let alpha = ((t * 2.0).sin() * 0.3 + 0.7) as f32;
                ui.label(RichText::new("● Executor running").size(12.0).color(
                    Color32::from_rgba_unmultiplied(63, 185, 80, (alpha * 255.0) as u8),
                ));
                ui.add_space(8.0);
            }
            ui.horizontal(|ui| {
                let avail = ui.available_width();
                let pad = (avail - 200.0).max(0.0) / 2.0;
                ui.add_space(pad);
                if self.executor_running {
                    if ui
                        .add_sized(
                            [200.0, 40.0],
                            Button::new(RichText::new("■ Stop Executor").color(TEXT_PRI))
                                .fill(C_DANGER),
                        )
                        .clicked()
                    {
                        self.executor_running = false;
                        self.send(AppCommand::StopExecutor);
                    }
                } else if ui
                    .add_sized(
                        [200.0, 40.0],
                        Button::new(RichText::new("▶ Start Executor").color(BG_APP)).fill(ACCENT),
                    )
                    .clicked()
                {
                    self.executor_running = true;
                    self.send(AppCommand::StartExecutor {
                        reliability: self.exec_reliability,
                        compute_ticks: self.exec_compute_ticks,
                        allowed_packages: self.exec_allowed_packages.clone(),
                    });
                }
            });
        });

        ui.add_space(10.0);
        let exec_lines: Vec<String> = self
            .log
            .iter()
            .rev()
            .filter(|l| {
                let lo = l.to_lowercase();
                lo.contains("executor") || lo.contains("tick") || lo.contains("task")
            })
            .take(8)
            .cloned()
            .collect();
        card_frame().show(ui, |ui| {
            section_header(ui, "Status");
            if exec_lines.is_empty() {
                ui.label(
                    RichText::new("No executor activity yet.")
                        .size(12.0)
                        .color(C_MUTED),
                );
            } else {
                for line in exec_lines.iter().rev() {
                    ui.label(
                        RichText::new(line)
                            .size(11.0)
                            .color(log_line_color(line))
                            .monospace(),
                    );
                }
            }
        });
    }

    // ── Packages tab ─────────────────────────────────────────────────────────
    fn draw_packages_tab(&mut self, ui: &mut egui::Ui) {
        card_frame()
            .fill(Color32::from_rgba_unmultiplied(210, 153, 34, 20))
            .show(ui, |ui| {
                ui.label(RichText::new("ℹ These are additional imports allowed in the isolated interpreter, on top of a safe stdlib baseline (math, random, datetime, collections, re, …). Submodule access (e.g. random._os) is always blocked. Changes apply when you (re)start the executor.")
                    .size(12.0).color(C_WARNING));
            });
        ui.add_space(10.0);

        // ── Add a package ──────────────────────────────────────────────────
        card_frame().show(ui, |ui| {
            section_header(ui, "Add Package");
            ui.horizontal(|ui| {
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.exec_new_package_input)
                        .hint_text("module name, e.g. numpy")
                        .desired_width(220.0),
                );
                let submit = ui
                    .add_sized(
                        [80.0, 28.0],
                        Button::new(RichText::new("+ Add").color(BG_APP)).fill(ACCENT),
                    )
                    .clicked()
                    || (resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)));

                if submit {
                    let name = self.exec_new_package_input.trim().to_string();
                    if !name.is_empty() && !self.exec_allowed_packages.contains(&name) {
                        self.exec_allowed_packages.push(name);
                        self.exec_allowed_packages.sort();
                    }
                    self.exec_new_package_input.clear();
                }
            });
        });
        ui.add_space(10.0);

        // ── Current allowlist ──────────────────────────────────────────────
        card_frame().show(ui, |ui| {
            ui.horizontal(|ui| {
                section_header(ui, "Allowed Packages");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add(Button::new(
                            RichText::new("Reset to defaults").size(11.0).color(TEXT_SEC),
                        ))
                        .clicked()
                    {
                        self.exec_allowed_packages = crate::sandbox::default_allowed_packages();
                    }
                });
            });
            ui.add_space(4.0);

            if self.exec_allowed_packages.is_empty() {
                ui.label(
                    RichText::new("No additional packages — only the safe stdlib baseline is importable.")
                        .size(12.0)
                        .color(C_MUTED),
                );
                return;
            }

            let mut remove: Option<usize> = None;
            for (idx, pkg) in self.exec_allowed_packages.iter().enumerate() {
                ui.horizontal(|ui| {
                    if ui
                        .add(Button::new(RichText::new("✕").size(11.0).color(C_DANGER)))
                        .on_hover_text("Remove")
                        .clicked()
                    {
                        remove = Some(idx);
                    }
                    ui.label(RichText::new(pkg).size(12.0).color(TEXT_PRI).monospace());
                });
            }
            if let Some(idx) = remove {
                self.exec_allowed_packages.remove(idx);
            }
        });

        if self.executor_running {
            ui.add_space(8.0);
            ui.label(
                RichText::new("⚠ Executor is running — restart it to apply allowlist changes.")
                    .size(11.0)
                    .color(C_WARNING),
            );
        }
    }

    // ── Network tab ──────────────────────────────────────────────────────────
    fn draw_network_tab(&mut self, ui: &mut egui::Ui) {
        if !self.connected {
            ui.centered_and_justified(|ui| {
                ui.label(RichText::new("Not connected.").color(C_MUTED).size(14.0));
            });
            return;
        }

        let participants = self.snapshot.participants.clone();
        let total_balance: u64 = participants.iter().map(|p| p.balance).sum();
        let top_rep = participants.iter().max_by_key(|p| p.reputation).cloned();

        // Stats row — 4 metric cards
        ui.columns(4, |cols| {
            cols[0].scope(|ui| {
                card_frame().show(ui, |ui| {
                    ui.label(RichText::new("Participants").size(11.0).color(TEXT_SEC));
                    ui.label(
                        RichText::new(participants.len().to_string())
                            .size(22.0)
                            .color(TEXT_PRI)
                            .strong(),
                    );
                });
            });
            cols[1].scope(|ui| {
                card_frame().show(ui, |ui| {
                    ui.label(RichText::new("Total Balance").size(11.0).color(TEXT_SEC));
                    ui.label(
                        RichText::new(total_balance.to_string())
                            .size(22.0)
                            .color(ACCENT)
                            .strong(),
                    );
                });
            });
            cols[2].scope(|ui| {
                card_frame().show(ui, |ui| {
                    ui.label(RichText::new("Blockchain").size(11.0).color(TEXT_SEC));
                    ui.horizontal(|ui| {
                        let (icon, color) = if self.snapshot.blockchain_valid {
                            ("✓", C_SUCCESS)
                        } else {
                            ("✗", C_DANGER)
                        };
                        ui.label(RichText::new(icon).size(20.0).color(color).strong());
                        ui.label(
                            RichText::new(format!("{} blocks", self.snapshot.block_count))
                                .size(12.0)
                                .color(TEXT_SEC),
                        );
                    });
                });
            });
            cols[3].scope(|ui| {
                card_frame().show(ui, |ui| {
                    ui.label(RichText::new("Top Rep").size(11.0).color(TEXT_SEC));
                    if let Some(p) = &top_rep {
                        ui.label(RichText::new(&p.name).size(13.0).color(TEXT_PRI).strong());
                        ui.label(
                            RichText::new(format!("★ {}", p.reputation))
                                .size(12.0)
                                .color(C_WARNING),
                        );
                    } else {
                        ui.label(RichText::new("—").size(13.0).color(C_MUTED));
                    }
                });
            });
        });

        ui.add_space(12.0);
        let part_hdr = format!("Participants ({})", participants.len());
        section_header(ui, &part_hdr);
        card_frame().show(ui, |ui| {
            egui::ScrollArea::vertical()
                .id_salt("network_scroll")
                .show(ui, |ui| {
                    egui::Grid::new("participants_grid")
                        .num_columns(4)
                        .striped(true)
                        .min_col_width(80.0)
                        .show(ui, |ui| {
                            ui.label(RichText::new("Name").size(11.0).color(TEXT_SEC).strong());
                            ui.label(RichText::new("ID").size(11.0).color(TEXT_SEC).strong());
                            ui.label(RichText::new("Balance").size(11.0).color(TEXT_SEC).strong());
                            ui.label(RichText::new("Rep").size(11.0).color(TEXT_SEC).strong());
                            ui.end_row();
                            for p in &participants {
                                let is_me = self.my_id == Some(p.id);
                                let name_rt = if is_me {
                                    RichText::new(format!("{} (me)", p.name))
                                        .color(ACCENT)
                                        .size(12.0)
                                } else {
                                    RichText::new(&p.name).color(TEXT_PRI).size(12.0)
                                };
                                ui.label(name_rt);
                                let id_str = p.id.to_string();
                                let short = if id_str.len() > 8 {
                                    format!("{}…", &id_str[..8])
                                } else {
                                    id_str
                                };
                                ui.label(
                                    RichText::new(short).size(11.0).color(C_MUTED).monospace(),
                                );
                                ui.label(
                                    RichText::new(p.balance.to_string())
                                        .size(12.0)
                                        .color(ACCENT),
                                );
                                ui.label(
                                    RichText::new(format!("★ {}", p.reputation))
                                        .size(12.0)
                                        .color(TEXT_PRI),
                                );
                                ui.end_row();
                            }
                        });
                });
        });

        ui.add_space(12.0);
        let block_hdr = format!("Blocks ({})", self.snapshot.blocks.len());
        section_header(ui, &block_hdr);
        card_frame().show(ui, |ui| {
            egui::ScrollArea::vertical()
                .id_salt("blocks_scroll")
                .max_height(260.0)
                .show(ui, |ui| {
                    for block in self.snapshot.blocks.iter().rev() {
                        let short_hash = &block.hash[..block.hash.len().min(12)];
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new(format!("#{}", block.index))
                                    .size(12.0)
                                    .color(TEXT_PRI)
                                    .strong(),
                            );
                            ui.label(
                                RichText::new(format!("tick {}", block.tick))
                                    .size(11.0)
                                    .color(TEXT_SEC),
                            );
                            ui.label(
                                RichText::new(short_hash)
                                    .size(11.0)
                                    .color(ACCENT)
                                    .monospace(),
                            );
                            ui.label(
                                RichText::new(format!("{} tx", block.transactions.len()))
                                    .size(11.0)
                                    .color(C_MUTED),
                            );
                        });
                        for tx in &block.transactions {
                            let text = match (tx.from, tx.to, &tx.memo) {
                                (Some(from), Some(to), Some(memo)) => {
                                    format!(
                                        "{} {} -> {} amount={} {}",
                                        tx.kind, from, to, tx.amount, memo
                                    )
                                }
                                (Some(from), Some(to), None) => {
                                    format!("{} {} -> {} amount={}", tx.kind, from, to, tx.amount)
                                }
                                (None, Some(to), _) => {
                                    format!("{} -> {} amount={}", tx.kind, to, tx.amount)
                                }
                                (Some(from), None, _) => {
                                    format!("{} {} amount={}", tx.kind, from, tx.amount)
                                }
                                (None, None, _) => {
                                    format!("{} amount={}", tx.kind, tx.amount)
                                }
                            };
                            ui.label(RichText::new(text).size(11.0).color(TEXT_SEC).monospace());
                        }
                        ui.add_space(6.0);
                    }
                });
        });
    }
}

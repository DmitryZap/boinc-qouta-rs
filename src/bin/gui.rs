use tokio::sync::mpsc;

use boinc_quota_rs::actor::NetworkActor;
use boinc_quota_rs::app::BoincApp;
use boinc_quota_rs::protocol::{AppCommand, AppEvent};
use eframe::egui;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let (cmd_tx, cmd_rx) = mpsc::channel::<AppCommand>(64);
    let (evt_tx, evt_rx) = mpsc::channel::<AppEvent>(256);

    rt.spawn(NetworkActor::new(cmd_rx, evt_tx).run());

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([960.0, 640.0])
            .with_title("BOINC Quota"),
        ..Default::default()
    };

    eframe::run_native(
        "boinc-quota",
        native_options,
        Box::new(|_cc| Ok(Box::new(BoincApp::new(cmd_tx, evt_rx)))),
    )
    .expect("eframe error");
}

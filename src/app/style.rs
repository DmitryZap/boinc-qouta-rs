use eframe::egui::{self, Color32, CornerRadius, Margin, RichText};

pub(super) const BG_APP: Color32 = Color32::from_rgb(15, 17, 23);
pub(super) const BG_SURFACE: Color32 = Color32::from_rgb(22, 27, 34);
pub(super) const BG_CARD: Color32 = Color32::from_rgb(30, 36, 48);
pub(super) const BG_INPUT: Color32 = Color32::from_rgb(13, 17, 23);
pub(super) const ACCENT: Color32 = Color32::from_rgb(88, 166, 255);
pub(super) const ACCENT_DIM: Color32 = Color32::from_rgb(31, 111, 235);
pub(super) const C_SUCCESS: Color32 = Color32::from_rgb(63, 185, 80);
pub(super) const C_WARNING: Color32 = Color32::from_rgb(210, 153, 34);
pub(super) const C_DANGER: Color32 = Color32::from_rgb(248, 81, 73);
pub(super) const C_MUTED: Color32 = Color32::from_rgb(110, 118, 129);
pub(super) const TEXT_PRI: Color32 = Color32::from_rgb(230, 237, 243);
pub(super) const TEXT_SEC: Color32 = Color32::from_rgb(139, 148, 158);

pub(super) fn card_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(BG_CARD)
        .corner_radius(CornerRadius::same(8u8))
        .inner_margin(Margin::same(12i8))
}

pub(super) fn section_header(ui: &mut egui::Ui, title: &str) {
    ui.label(
        RichText::new(title.to_uppercase())
            .size(11.0)
            .color(TEXT_SEC)
            .strong(),
    );
    ui.add_space(4.0);
}

pub(super) fn status_badge(ui: &mut egui::Ui, label: &str, bg: Color32, fg: Color32) {
    egui::Frame::new()
        .fill(bg)
        .corner_radius(CornerRadius::same(10u8))
        .inner_margin(Margin::symmetric(8i8, 3i8))
        .show(ui, |ui| {
            ui.label(RichText::new(label).size(11.0).color(fg).strong());
        });
}

pub(super) fn log_line_color(line: &str) -> Color32 {
    if line.starts_with("ERROR") {
        C_DANGER
    } else if line.starts_with("WARN") {
        C_WARNING
    } else {
        TEXT_SEC
    }
}

pub(super) fn task_status_colors(status: &str) -> (Color32, Color32) {
    if status == "Pending" {
        (Color32::from_rgba_unmultiplied(210, 153, 34, 35), C_WARNING)
    } else if status.starts_with("Assigned") {
        (Color32::from_rgba_unmultiplied(88, 166, 255, 35), ACCENT)
    } else if status.starts_with("Done") {
        (Color32::from_rgba_unmultiplied(63, 185, 80, 35), C_SUCCESS)
    } else if status == "Quota exhausted" {
        (Color32::from_rgba_unmultiplied(210, 153, 34, 35), C_WARNING)
    } else {
        (Color32::from_rgba_unmultiplied(248, 81, 73, 35), C_DANGER)
    }
}

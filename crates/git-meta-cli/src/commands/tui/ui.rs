//! Rendering: pure `&App` → ratatui widgets, no logic beyond formatting.
//!
//! Colors mirror `git meta inspect`: yellow target types, cyan `type:` +
//! green value for targets, bold keys, dim previews and timestamps.

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState, Wrap};
use ratatui::Frame;

use git_meta_lib::types::{MetaValue, TargetType};

use super::data::{format_relative, format_timestamp, DetailData};
use super::state::{App, InputMode, View};
use crate::commands::inspect::format_value_oneline;

const TYPE_STYLE: Style = Style::new().fg(Color::Yellow);
const TYPE_PREFIX_STYLE: Style = Style::new().fg(Color::Cyan);
const VALUE_STYLE: Style = Style::new().fg(Color::Green);
const KEY_STYLE: Style = Style::new().add_modifier(Modifier::BOLD);
const DIM_STYLE: Style = Style::new().add_modifier(Modifier::DIM);
const SELECTED_STYLE: Style = Style::new().add_modifier(Modifier::REVERSED);

pub(super) fn draw(frame: &mut Frame, app: &App) {
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, header, app);
    match app.view() {
        View::Overview { selected } => draw_overview(frame, body, app, *selected),
        View::TargetList {
            target_type,
            selected,
            filter,
        } => draw_target_list(frame, body, app, target_type, *selected, filter),
        View::KeyList {
            target_type,
            target_value,
            selected,
            filter,
        } => draw_key_list(
            frame,
            body,
            app,
            target_type,
            target_value,
            *selected,
            filter,
        ),
        View::Detail { detail, scroll, .. } => {
            draw_detail(frame, body, app, detail, *scroll);
        }
    }
    draw_footer(frame, footer, app);
}

/// Breadcrumb of the view stack: `git meta tui › commit › abc123 › agent:model`.
fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let mut spans = vec![Span::styled("git meta tui", KEY_STYLE)];
    for view in app.stack() {
        match view {
            View::Overview { .. } => {}
            View::TargetList { target_type, .. } => {
                spans.push(Span::styled(" › ", DIM_STYLE));
                spans.push(Span::styled(target_type.to_string(), TYPE_STYLE));
            }
            View::KeyList {
                target_type,
                target_value,
                ..
            } => {
                spans.push(Span::styled(" › ", DIM_STYLE));
                if *target_type == TargetType::Project {
                    spans.push(Span::styled("project", TYPE_STYLE));
                } else {
                    spans.push(Span::styled(target_value.clone(), VALUE_STYLE));
                }
            }
            View::Detail { key, .. } => {
                spans.push(Span::styled(" › ", DIM_STYLE));
                spans.push(Span::styled(key.clone(), KEY_STYLE));
            }
        }
    }
    frame.render_widget(Line::from(spans), area);
}

fn draw_overview(frame: &mut Frame, area: Rect, app: &App, selected: usize) {
    let rows: Vec<Row> = app
        .snapshot
        .type_rows()
        .into_iter()
        .map(|row| {
            let targets = if row.target_type == TargetType::Project || row.target_count == 0 {
                String::new()
            } else {
                format!(" across {} targets", row.target_count)
            };
            let pending = if row.promised > 0 {
                format!("  ({} pending)", row.promised)
            } else {
                String::new()
            };
            Row::new(vec![
                Cell::from(Span::styled(row.target_type.to_string(), TYPE_STYLE)),
                Cell::from(Line::from(vec![
                    Span::raw(format!("{} keys{targets}", row.key_count)),
                    Span::styled(pending, DIM_STYLE),
                ])),
            ])
        })
        .collect();

    let table = Table::new(rows, [Constraint::Length(10), Constraint::Min(0)])
        .row_highlight_style(SELECTED_STYLE);
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_target_list(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    target_type: &TargetType,
    selected: usize,
    filter: &str,
) {
    let rows: Vec<Row> = app
        .snapshot
        .target_rows(target_type, filter)
        .into_iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(Line::from(vec![
                    Span::styled(format!("{target_type}:"), TYPE_PREFIX_STYLE),
                    Span::styled(row.target_value, VALUE_STYLE),
                ])),
                Cell::from(format!(
                    "{} key{}",
                    row.key_count,
                    if row.key_count == 1 { "" } else { "s" }
                )),
                Cell::from(Span::styled(
                    format_relative(row.last_timestamp, app.now_ms),
                    DIM_STYLE,
                )),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(8),
            Constraint::Length(12),
        ],
    )
    .row_highlight_style(SELECTED_STYLE);
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_key_list(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    target_type: &TargetType,
    target_value: &str,
    selected: usize,
    filter: &str,
) {
    let width = area.width as usize;
    let rows: Vec<Row> = app
        .snapshot
        .key_rows(target_type, target_value, filter)
        .into_iter()
        .map(|row| {
            let preview = format_value_oneline(&row.value, &row.value_type, width, row.key.len());
            Row::new(vec![
                Cell::from(Line::from(vec![
                    Span::styled(row.key, KEY_STYLE),
                    Span::raw("  "),
                    Span::styled(preview, DIM_STYLE),
                ])),
                Cell::from(Span::styled(
                    format_relative(row.last_timestamp, app.now_ms),
                    DIM_STYLE,
                )),
            ])
        })
        .collect();

    let table = Table::new(rows, [Constraint::Min(20), Constraint::Length(12)])
        .row_highlight_style(SELECTED_STYLE);
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_detail(frame: &mut Frame, area: Rect, app: &App, detail: &DetailData, scroll: usize) {
    let [info, content] = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);

    let mut info_spans = vec![Span::styled(
        format!(
            "type: {} · updated {}",
            detail.value.value_type(),
            format_timestamp(detail.last_timestamp)
        ),
        DIM_STYLE,
    )];
    if let Some(authorship) = &detail.authorship {
        info_spans.push(Span::styled(
            format!(" · by {}", authorship.email),
            DIM_STYLE,
        ));
    }
    frame.render_widget(Line::from(info_spans), info);

    let scroll_u16 = scroll.min(u16::MAX as usize) as u16;
    match &detail.value {
        MetaValue::String(s) => {
            let paragraph = Paragraph::new(s.as_str())
                .wrap(Wrap { trim: false })
                .scroll((scroll_u16, 0));
            frame.render_widget(paragraph, content);
        }
        MetaValue::List(entries) => {
            let rows: Vec<Row> = entries
                .iter()
                .enumerate()
                .map(|(i, entry)| {
                    Row::new(vec![
                        Cell::from(Span::styled(format!("[{i}]"), DIM_STYLE)),
                        Cell::from(entry.value.clone()),
                        Cell::from(Span::styled(
                            format_relative(entry.timestamp, app.now_ms),
                            DIM_STYLE,
                        )),
                    ])
                })
                .collect();
            let table = Table::new(
                rows,
                [
                    Constraint::Length(6),
                    Constraint::Min(20),
                    Constraint::Length(12),
                ],
            );
            let mut state = TableState::default().with_offset(scroll);
            frame.render_stateful_widget(table, content, &mut state);
        }
        MetaValue::Set(members) => {
            let text: Vec<Line> = members.iter().map(|m| Line::raw(m.as_str())).collect();
            let paragraph = Paragraph::new(text).scroll((scroll_u16, 0));
            frame.render_widget(paragraph, content);
        }
        _ => {
            frame.render_widget(Paragraph::new("[unsupported value type]"), content);
        }
    }
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &App) {
    if let Some(status) = &app.status {
        frame.render_widget(
            Line::from(Span::styled(status.clone(), Style::new().fg(Color::Red))),
            area,
        );
        return;
    }

    if matches!(app.input_mode, InputMode::Filter) {
        let filter = match app.view() {
            View::TargetList { filter, .. } | View::KeyList { filter, .. } => filter.as_str(),
            _ => "",
        };
        frame.render_widget(Line::raw(format!("/{filter}▌")), area);
        return;
    }

    let mut hints = String::new();
    match app.view() {
        View::Overview { .. } => hints.push_str("j/k move · enter open · q quit"),
        View::TargetList { filter, .. } | View::KeyList { filter, .. } => {
            if !filter.is_empty() {
                hints.push_str(&format!("filter: {filter} · "));
            }
            hints.push_str("j/k move · enter open · / filter · esc back · q quit");
        }
        View::Detail { .. } => hints.push_str("j/k scroll · g/G top/bottom · esc back · q quit"),
    }
    frame.render_widget(Line::from(Span::styled(hints, DIM_STYLE)), area);
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;

    use git_meta_lib::types::ValueType;

    use super::super::data::{test_entry, MetaSnapshot};
    use super::*;

    fn app() -> App {
        App::new(
            MetaSnapshot {
                entries: vec![
                    test_entry(
                        TargetType::Commit,
                        "aaa111",
                        "agent:model",
                        "\"claude\"",
                        ValueType::String,
                        1_000,
                    ),
                    test_entry(
                        TargetType::Commit,
                        "aaa111",
                        "review:status",
                        "\"approved\"",
                        ValueType::String,
                        2_000,
                    ),
                ],
                promised_counts: BTreeMap::from([(TargetType::Branch, 2)]),
            },
            10_000,
        )
    }

    fn rendered_text(app: &App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        let mut text = String::new();
        for y in 0..area.height {
            for x in 0..area.width {
                text.push_str(buffer[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn overview_shows_types_counts_and_pending() {
        let text = rendered_text(&app());
        assert!(text.contains("git meta tui"));
        assert!(text.contains("commit"));
        assert!(text.contains("2 keys across 1 targets"));
        assert!(text.contains("branch"));
        assert!(text.contains("(2 pending)"));
        assert!(text.contains("q quit"));
    }

    #[test]
    fn key_list_shows_keys_with_previews() {
        let mut app = app();
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let text = rendered_text(&app);
        assert!(text.contains("aaa111"));
        assert!(text.contains("agent:model"));
        assert!(text.contains("claude"));
        assert!(text.contains("/ filter"));
    }
}

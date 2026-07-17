//! Rendering: pure `&App` → ratatui widgets, no logic beyond formatting.
//!
//! Layout: a breadcrumb header, a body split into a left navigation pane
//! and a right value pane, and a footer with key hints. Colors mirror
//! `git meta inspect`: yellow target types, cyan `type:` + green value for
//! targets, bold keys, dim previews and timestamps.

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table, TableState, Wrap};
use ratatui::Frame;

use git_meta_lib::types::{MetaValue, TargetType};

use super::data::{format_relative, format_timestamp, join_prefix, DetailData, KeyTreeRow};
use super::state::{App, DetailRequest, InputMode, PaneFocus, View};
use crate::commands::inspect::format_value_oneline;

const TYPE_STYLE: Style = Style::new().fg(Color::Yellow);
const TYPE_PREFIX_STYLE: Style = Style::new().fg(Color::Cyan);
const VALUE_STYLE: Style = Style::new().fg(Color::Green);
const KEY_STYLE: Style = Style::new().add_modifier(Modifier::BOLD);
const DIM_STYLE: Style = Style::new().add_modifier(Modifier::DIM);
const SELECTED_STYLE: Style = Style::new().add_modifier(Modifier::REVERSED);
const FOCUSED_BORDER: Style = Style::new().fg(Color::Cyan);
const UNFOCUSED_BORDER: Style = Style::new().add_modifier(Modifier::DIM);

pub(super) fn draw(frame: &mut Frame, app: &App) {
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, header, app);

    let [nav_area, side_area] =
        Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)]).areas(body);
    draw_nav(frame, nav_area, app);
    draw_side(frame, side_area, app);

    draw_footer(frame, footer, app);
}

/// Breadcrumb of the view stack: `git meta tui › commit › abc123`.
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
                prefix,
                ..
            } => {
                spans.push(Span::styled(" › ", DIM_STYLE));
                // The root key level names the target; each deeper level
                // names its namespace segment.
                match prefix.rsplit(':').next() {
                    Some(segment) if !prefix.is_empty() => {
                        spans.push(Span::styled(segment.to_string(), KEY_STYLE));
                    }
                    _ if *target_type == TargetType::Project => {
                        spans.push(Span::styled("project", TYPE_STYLE));
                    }
                    _ => spans.push(Span::styled(target_value.clone(), VALUE_STYLE)),
                }
            }
            View::Search { .. } => {
                spans.push(Span::styled(" › ", DIM_STYLE));
                spans.push(Span::raw("search"));
            }
        }
    }
    frame.render_widget(Line::from(spans), area);
}

/// Left pane: the current navigation list.
fn draw_nav(frame: &mut Frame, area: Rect, app: &App) {
    let focused = matches!(app.focus, PaneFocus::Nav);
    let title = match app.view() {
        View::Overview { .. } => "target types".to_string(),
        View::TargetList { target_type, .. } => format!("{target_type} targets"),
        View::KeyList { prefix, .. } if !prefix.is_empty() => prefix.clone(),
        View::KeyList { .. } => "keys".to_string(),
        View::Search { .. } => "search".to_string(),
    };
    let block = Block::bordered().title(title).border_style(if focused {
        FOCUSED_BORDER
    } else {
        UNFOCUSED_BORDER
    });
    let inner = block.inner(area);
    frame.render_widget(block, area);

    match app.view() {
        View::Overview { selected } => draw_type_list(frame, inner, app, *selected),
        View::TargetList {
            target_type,
            selected,
            filter,
        } => draw_target_list(frame, inner, app, target_type, *selected, filter),
        View::KeyList {
            target_type,
            target_value,
            prefix,
            selected,
            filter,
        } => draw_key_list(
            frame,
            inner,
            app,
            target_type,
            target_value,
            prefix,
            *selected,
            filter,
        ),
        View::Search { query, selected } => draw_search(frame, inner, app, query, *selected),
    }
}

fn draw_type_list(frame: &mut Frame, area: Rect, app: &App, selected: usize) {
    let rows: Vec<Row> = app
        .snapshot
        .type_rows()
        .into_iter()
        .map(|row| {
            let pending = if row.promised > 0 {
                format!("  ({} pending)", row.promised)
            } else {
                String::new()
            };
            let targets = if row.target_type == TargetType::Project || row.target_count == 0 {
                String::new()
            } else {
                format!(" · {} targets", row.target_count)
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
                Cell::from(Span::styled(row.target_value, VALUE_STYLE)),
                Cell::from(Span::styled(
                    format_relative(row.last_timestamp, app.now_ms),
                    DIM_STYLE,
                )),
            ])
        })
        .collect();

    let table = Table::new(rows, [Constraint::Min(10), Constraint::Length(12)])
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
    prefix: &str,
    selected: usize,
    filter: &str,
) {
    let rows: Vec<Row> = app
        .snapshot
        .key_tree_rows(target_type, target_value, prefix, filter)
        .into_iter()
        .map(|row| match row {
            KeyTreeRow::Namespace {
                segment,
                key_count,
                last_timestamp,
            } => Row::new(vec![
                Cell::from(Line::from(vec![
                    Span::styled(segment, TYPE_PREFIX_STYLE),
                    Span::styled(format!(" ▸ {key_count}"), DIM_STYLE),
                ])),
                Cell::from(Span::styled(
                    format_relative(last_timestamp, app.now_ms),
                    DIM_STYLE,
                )),
            ]),
            KeyTreeRow::Leaf { segment, row } => Row::new(vec![
                Cell::from(Span::styled(segment, KEY_STYLE)),
                Cell::from(Span::styled(
                    format_relative(row.last_timestamp, app.now_ms),
                    DIM_STYLE,
                )),
            ]),
        })
        .collect();

    let table = Table::new(rows, [Constraint::Min(10), Constraint::Length(12)])
        .row_highlight_style(SELECTED_STYLE);
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_search(frame: &mut Frame, area: Rect, app: &App, query: &str, selected: usize) {
    let [input_area, results_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);

    frame.render_widget(
        Line::from(vec![
            Span::styled("search: ", DIM_STYLE),
            Span::raw(query.to_string()),
            Span::raw("▌"),
        ]),
        input_area,
    );

    let rows: Vec<Row> = app
        .snapshot
        .search_rows(query)
        .into_iter()
        .map(|row| Row::new(vec![Cell::from(row.path)]))
        .collect();

    let table = Table::new(rows, [Constraint::Min(10)]).row_highlight_style(SELECTED_STYLE);
    let mut state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, results_area, &mut state);
}

/// Right pane: a preview of the next level down, or the selected key's
/// value with its metadata.
fn draw_side(frame: &mut Frame, area: Rect, app: &App) {
    let focused = matches!(app.focus, PaneFocus::Detail);
    let border = if focused {
        FOCUSED_BORDER
    } else {
        UNFOCUSED_BORDER
    };

    match app.view() {
        View::Overview { selected } => {
            let title = app
                .snapshot
                .type_rows()
                .into_iter()
                .nth(*selected)
                .map(|row| row.target_type.to_string())
                .unwrap_or_default();
            let block = Block::bordered().title(title).border_style(border);
            let inner = block.inner(area);
            frame.render_widget(block, area);
            draw_type_preview(frame, inner, app, *selected);
        }
        View::TargetList {
            target_type,
            selected,
            filter,
        } => {
            let target = app
                .snapshot
                .target_rows(target_type, filter)
                .into_iter()
                .nth(*selected)
                .map(|row| row.target_value)
                .unwrap_or_default();
            let block = Block::bordered()
                .title(Line::from(vec![
                    Span::styled(format!("{target_type}:"), TYPE_PREFIX_STYLE),
                    Span::styled(target.clone(), VALUE_STYLE),
                ]))
                .border_style(border);
            let inner = block.inner(area);
            frame.render_widget(block, area);
            draw_keys_preview(frame, inner, app, target_type, &target, "");
        }
        View::KeyList {
            target_type,
            target_value,
            prefix,
            selected,
            filter,
        } => {
            // A selected namespace previews its children; a selected leaf
            // (or nothing) shows the loaded value.
            if let Some(KeyTreeRow::Namespace { segment, .. }) = app
                .snapshot
                .key_tree_rows(target_type, target_value, prefix, filter)
                .into_iter()
                .nth(*selected)
            {
                let child_prefix = join_prefix(prefix, &segment);
                let block = Block::bordered()
                    .title(child_prefix.clone())
                    .border_style(border);
                let inner = block.inner(area);
                frame.render_widget(block, area);
                draw_keys_preview(frame, inner, app, target_type, target_value, &child_prefix);
            } else {
                draw_value_pane(frame, area, app, border);
            }
        }
        View::Search { .. } => draw_value_pane(frame, area, app, border),
    }
}

/// The loaded detail (or a placeholder) inside a titled block.
fn draw_value_pane(frame: &mut Frame, area: Rect, app: &App, border: Style) {
    let title = app
        .detail()
        .map_or_else(|| "value".to_string(), |(request, _)| request.key.clone());
    let block = Block::bordered().title(title).border_style(border);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    match app.detail() {
        Some((request, detail)) => {
            draw_detail(frame, inner, app, request, detail, app.detail_scroll);
        }
        None => frame.render_widget(Span::styled("no value selected", DIM_STYLE), inner),
    }
}

/// Preview for a selected target type: its targets (or keys for project).
fn draw_type_preview(frame: &mut Frame, area: Rect, app: &App, selected: usize) {
    let Some(type_row) = app.snapshot.type_rows().into_iter().nth(selected) else {
        return;
    };
    if type_row.target_type == TargetType::Project {
        draw_keys_preview(frame, area, app, &TargetType::Project, "", "");
        return;
    }

    let rows: Vec<Row> = app
        .snapshot
        .target_rows(&type_row.target_type, "")
        .into_iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(Span::styled(row.target_value, VALUE_STYLE)),
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
            Constraint::Min(10),
            Constraint::Length(8),
            Constraint::Length(12),
        ],
    );
    frame.render_widget(table, area);
}

/// Preview of one key-namespace level: namespaces with counts, leaf keys
/// with one-line value previews.
fn draw_keys_preview(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    target_type: &TargetType,
    target_value: &str,
    prefix: &str,
) {
    let width = area.width as usize;
    let rows: Vec<Row> = app
        .snapshot
        .key_tree_rows(target_type, target_value, prefix, "")
        .into_iter()
        .map(|row| match row {
            KeyTreeRow::Namespace {
                segment, key_count, ..
            } => Row::new(vec![Cell::from(Line::from(vec![
                Span::styled(segment, TYPE_PREFIX_STYLE),
                Span::styled(
                    format!(
                        " ▸ {key_count} key{}",
                        if key_count == 1 { "" } else { "s" }
                    ),
                    DIM_STYLE,
                ),
            ]))]),
            KeyTreeRow::Leaf { segment, row } => {
                let preview =
                    format_value_oneline(&row.value, &row.value_type, width, segment.len());
                Row::new(vec![Cell::from(Line::from(vec![
                    Span::styled(segment, KEY_STYLE),
                    Span::raw("  "),
                    Span::styled(preview, DIM_STYLE),
                ]))])
            }
        })
        .collect();
    let table = Table::new(rows, [Constraint::Min(0)]);
    frame.render_widget(table, area);
}

/// The selected key's value plus its metadata block.
fn draw_detail(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    request: &DetailRequest,
    detail: &DetailData,
    scroll: usize,
) {
    let target_label = if request.target_type == TargetType::Project {
        "project".to_string()
    } else {
        format!("{}:{}", request.target_type, request.target_value)
    };
    let mut meta_lines = vec![
        Line::from(vec![
            Span::styled("target   ", DIM_STYLE),
            Span::styled(target_label, VALUE_STYLE),
        ]),
        Line::from(vec![
            Span::styled("key      ", DIM_STYLE),
            Span::styled(request.key.clone(), KEY_STYLE),
        ]),
        Line::from(Span::styled(
            format!(
                "type     {} · updated {} ({})",
                detail.value.value_type(),
                format_timestamp(detail.last_timestamp),
                format_relative(detail.last_timestamp, app.now_ms)
            ),
            DIM_STYLE,
        )),
    ];
    if let Some(authorship) = &detail.authorship {
        meta_lines.push(Line::from(Span::styled(
            format!("author   {}", authorship.email),
            DIM_STYLE,
        )));
    }

    let [meta_area, _separator, value_area] = Layout::vertical([
        Constraint::Length(meta_lines.len() as u16),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .areas(area);
    frame.render_widget(Paragraph::new(meta_lines), meta_area);

    let scroll_u16 = scroll.min(u16::MAX as usize) as u16;
    match &detail.value {
        MetaValue::String(s) => {
            let paragraph = Paragraph::new(s.as_str())
                .wrap(Wrap { trim: false })
                .scroll((scroll_u16, 0));
            frame.render_widget(paragraph, value_area);
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
            frame.render_stateful_widget(table, value_area, &mut state);
        }
        MetaValue::Set(members) => {
            let text: Vec<Line> = members.iter().map(|m| Line::raw(m.as_str())).collect();
            let paragraph = Paragraph::new(text).scroll((scroll_u16, 0));
            frame.render_widget(paragraph, value_area);
        }
        _ => {
            frame.render_widget(Paragraph::new("[unsupported value type]"), value_area);
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
        View::Search { .. } => {
            hints.push_str("type to search · ↑/↓ move · enter jump · esc cancel");
        }
        _ if matches!(app.focus, PaneFocus::Detail) => {
            hints.push_str("j/k scroll · g/G top/bottom · tab/esc back · q quit");
        }
        View::Overview { .. } => {
            hints.push_str("j/k move · enter open · s search · q quit");
        }
        View::TargetList { filter, .. } => {
            if !filter.is_empty() {
                hints.push_str(&format!("filter: {filter} · "));
            }
            hints.push_str("j/k move · enter open · / filter · s search · esc back · q quit");
        }
        View::KeyList { filter, .. } => {
            if !filter.is_empty() {
                hints.push_str(&format!("filter: {filter} · "));
            }
            if app.wanted_detail().is_some() {
                hints.push_str(
                    "j/k move · enter/tab value pane · / filter · s search · esc back · q quit",
                );
            } else {
                hints.push_str("j/k move · enter open · / filter · s search · esc back · q quit");
            }
        }
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

    use git_meta_lib::db::types::Authorship;
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

    fn press(app: &mut App, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn rendered_text(app: &App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
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
    fn overview_shows_types_with_target_preview_pane() {
        let text = rendered_text(&app());
        assert!(text.contains("git meta tui"));
        assert!(text.contains("target types"));
        assert!(text.contains("2 keys"));
        assert!(text.contains("(2 pending)"));
        // Right pane previews the selected type's targets.
        assert!(text.contains("aaa111"));
        assert!(text.contains("q quit"));
    }

    #[test]
    fn key_list_shows_namespace_groups_with_children_preview() {
        let mut app = app();
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Enter);
        let text = rendered_text(&app);
        // Left pane: namespace groups, not full truncated keys.
        assert!(text.contains("agent ▸ 1"));
        assert!(text.contains("review ▸ 1"));
        // Right pane previews the selected namespace's children.
        assert!(text.contains("model  claude"));
        assert!(text.contains("enter open"));
    }

    #[test]
    fn key_list_shows_value_and_metadata_in_side_pane() {
        let mut app = app();
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Enter);
        // Descend into the agent namespace to reach the leaf.
        press(&mut app, KeyCode::Enter);
        let request = app.wanted_detail().unwrap();
        app.set_detail(
            request,
            DetailData {
                value: MetaValue::String("claude".to_string()),
                last_timestamp: 1_000,
                authorship: Some(Authorship {
                    email: "test@example.com".to_string(),
                    timestamp: 1_000,
                }),
            },
        );
        let text = rendered_text(&app);
        assert!(text.contains("agent:model"));
        assert!(text.contains("commit:aaa111"));
        assert!(text.contains("claude"));
        assert!(text.contains("author   test@example.com"));
        assert!(text.contains("value pane"));
    }

    #[test]
    fn search_view_shows_query_and_matching_paths() {
        let mut app = app();
        press(&mut app, KeyCode::Char('s'));
        for c in "rev".chars() {
            press(&mut app, KeyCode::Char(c));
        }
        let text = rendered_text(&app);
        assert!(text.contains("search: rev"));
        assert!(text.contains("commit:aaa111 review:status"));
        assert!(!text.contains("agent:model  "));
        assert!(text.contains("enter jump"));
    }
}

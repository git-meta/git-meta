//! TUI state machine: a stack of views plus key handling.
//!
//! This module owns navigation and selection but performs no I/O. The one
//! action that needs the database — opening a key's detail view — is
//! returned as a [`Command`] for the event loop to execute, which keeps
//! everything here drivable by unit tests with a synthetic snapshot.

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use git_meta_lib::types::{MetaValue, TargetType};

use super::data::{DetailData, MetaSnapshot};

/// Whether keystrokes navigate or edit the current view's filter.
pub(super) enum InputMode {
    Normal,
    Filter,
}

/// One level of the browsing hierarchy. Each view owns its selection and
/// filter, so popping back restores them automatically.
pub(super) enum View {
    Overview {
        selected: usize,
    },
    TargetList {
        target_type: TargetType,
        selected: usize,
        filter: String,
    },
    KeyList {
        target_type: TargetType,
        target_value: String,
        selected: usize,
        filter: String,
    },
    Detail {
        key: String,
        detail: DetailData,
        scroll: usize,
    },
}

/// An effect the event loop must perform against the session.
pub(super) enum Command {
    OpenDetail {
        target_type: TargetType,
        target_value: String,
        key: String,
        is_git_ref: bool,
        last_timestamp: i64,
    },
}

const ROOT_VIEW: View = View::Overview { selected: 0 };

pub(super) struct App {
    pub(super) snapshot: MetaSnapshot,
    /// Wall-clock time captured at startup, for relative timestamps.
    pub(super) now_ms: i64,
    /// Never empty; `stack[0]` is always the overview.
    stack: Vec<View>,
    pub(super) input_mode: InputMode,
    should_quit: bool,
    /// Transient message shown in the footer (e.g. a detail load failure).
    pub(super) status: Option<String>,
    /// Rows visible in the body area, for half-page scrolling.
    viewport_rows: usize,
}

impl App {
    pub(super) fn new(snapshot: MetaSnapshot, now_ms: i64) -> Self {
        Self {
            snapshot,
            now_ms,
            stack: vec![ROOT_VIEW],
            input_mode: InputMode::Normal,
            should_quit: false,
            status: None,
            viewport_rows: 20,
        }
    }

    pub(super) fn view(&self) -> &View {
        self.stack.last().unwrap_or(&ROOT_VIEW)
    }

    /// The full view stack, root first, for the breadcrumb header.
    pub(super) fn stack(&self) -> &[View] {
        &self.stack
    }

    pub(super) fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub(super) fn set_viewport_rows(&mut self, rows: usize) {
        self.viewport_rows = rows.max(1);
    }

    pub(super) fn set_status(&mut self, message: String) {
        self.status = Some(message);
    }

    pub(super) fn push_detail(&mut self, key: String, detail: DetailData) {
        self.stack.push(View::Detail {
            key,
            detail,
            scroll: 0,
        });
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) -> Option<Command> {
        self.status = None;
        match self.input_mode {
            InputMode::Filter => {
                self.handle_filter_key(key);
                None
            }
            InputMode::Normal => self.handle_normal_key(key),
        }
    }

    fn handle_normal_key(&mut self, key: KeyEvent) -> Option<Command> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let half_page = (self.viewport_rows / 2).max(1) as isize;
        match key.code {
            KeyCode::Char('c') if ctrl => {
                self.should_quit = true;
                None
            }
            KeyCode::Char('q') => {
                self.should_quit = true;
                None
            }
            KeyCode::Char('d') if ctrl => {
                self.move_selection(half_page);
                None
            }
            KeyCode::Char('u') if ctrl => {
                self.move_selection(-half_page);
                None
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_selection(1);
                None
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_selection(-1);
                None
            }
            KeyCode::Char('g') => {
                self.jump_to(0);
                None
            }
            KeyCode::Char('G') => {
                self.jump_to(usize::MAX);
                None
            }
            KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => self.descend(),
            KeyCode::Esc => {
                if self.stack.len() > 1 {
                    self.stack.pop();
                } else {
                    self.should_quit = true;
                }
                None
            }
            KeyCode::Char('h') | KeyCode::Left | KeyCode::Backspace => {
                if self.stack.len() > 1 {
                    self.stack.pop();
                }
                None
            }
            KeyCode::Char('/') => {
                if matches!(self.view(), View::TargetList { .. } | View::KeyList { .. }) {
                    self.input_mode = InputMode::Filter;
                }
                None
            }
            _ => None,
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                if let Some(filter) = self.current_filter_mut() {
                    filter.clear();
                }
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Enter => self.input_mode = InputMode::Normal,
            KeyCode::Backspace => {
                if let Some(filter) = self.current_filter_mut() {
                    filter.pop();
                }
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(filter) = self.current_filter_mut() {
                    filter.push(c);
                }
            }
            _ => {}
        }
        self.clamp_selection();
    }

    fn current_filter_mut(&mut self) -> Option<&mut String> {
        match self.stack.last_mut()? {
            View::TargetList { filter, .. } | View::KeyList { filter, .. } => Some(filter),
            _ => None,
        }
    }

    /// Rows in the current view: list length, or scrollable lines in detail.
    fn row_count(&self) -> usize {
        match self.view() {
            View::Overview { .. } => self.snapshot.type_rows().len(),
            View::TargetList {
                target_type,
                filter,
                ..
            } => self.snapshot.target_rows(target_type, filter).len(),
            View::KeyList {
                target_type,
                target_value,
                filter,
                ..
            } => self
                .snapshot
                .key_rows(target_type, target_value, filter)
                .len(),
            View::Detail { detail, .. } => match &detail.value {
                MetaValue::String(s) => s.lines().count(),
                MetaValue::List(entries) => entries.len(),
                MetaValue::Set(members) => members.len(),
                _ => 0,
            },
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let count = self.row_count();
        if count == 0 {
            return;
        }
        let max = count - 1;
        if let Some(view) = self.stack.last_mut() {
            let position = match view {
                View::Overview { selected }
                | View::TargetList { selected, .. }
                | View::KeyList { selected, .. } => selected,
                View::Detail { scroll, .. } => scroll,
            };
            *position = position.saturating_add_signed(delta).min(max);
        }
    }

    fn jump_to(&mut self, target: usize) {
        let count = self.row_count();
        if count == 0 {
            return;
        }
        let clamped = target.min(count - 1);
        if let Some(view) = self.stack.last_mut() {
            match view {
                View::Overview { selected }
                | View::TargetList { selected, .. }
                | View::KeyList { selected, .. } => *selected = clamped,
                View::Detail { scroll, .. } => *scroll = clamped,
            }
        }
    }

    fn clamp_selection(&mut self) {
        let count = self.row_count();
        if let Some(view) = self.stack.last_mut() {
            let position = match view {
                View::Overview { selected }
                | View::TargetList { selected, .. }
                | View::KeyList { selected, .. } => selected,
                View::Detail { scroll, .. } => scroll,
            };
            *position = (*position).min(count.saturating_sub(1));
        }
    }

    /// Enter the selected row: push the next view down, or emit a command
    /// when opening a detail view (which needs the database).
    fn descend(&mut self) -> Option<Command> {
        let mut push: Option<View> = None;
        let mut command: Option<Command> = None;

        match self.view() {
            View::Overview { selected } => {
                if let Some(row) = self.snapshot.type_rows().into_iter().nth(*selected) {
                    // Project metadata has a single implicit target, so skip
                    // the target list and go straight to its keys.
                    push = Some(if row.target_type == TargetType::Project {
                        View::KeyList {
                            target_type: row.target_type,
                            target_value: String::new(),
                            selected: 0,
                            filter: String::new(),
                        }
                    } else {
                        View::TargetList {
                            target_type: row.target_type,
                            selected: 0,
                            filter: String::new(),
                        }
                    });
                }
            }
            View::TargetList {
                target_type,
                selected,
                filter,
            } => {
                if let Some(row) = self
                    .snapshot
                    .target_rows(target_type, filter)
                    .into_iter()
                    .nth(*selected)
                {
                    push = Some(View::KeyList {
                        target_type: target_type.clone(),
                        target_value: row.target_value,
                        selected: 0,
                        filter: String::new(),
                    });
                }
            }
            View::KeyList {
                target_type,
                target_value,
                selected,
                filter,
            } => {
                if let Some(row) = self
                    .snapshot
                    .key_rows(target_type, target_value, filter)
                    .into_iter()
                    .nth(*selected)
                {
                    command = Some(Command::OpenDetail {
                        target_type: target_type.clone(),
                        target_value: target_value.clone(),
                        key: row.key,
                        is_git_ref: row.is_git_ref,
                        last_timestamp: row.last_timestamp,
                    });
                }
            }
            View::Detail { .. } => {}
        }

        if let Some(view) = push {
            self.stack.push(view);
        }
        command
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use git_meta_lib::types::ValueType;

    use super::super::data::test_entry;
    use super::*;

    fn snapshot() -> MetaSnapshot {
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
                    "bbb222",
                    "agent:model",
                    "\"codex\"",
                    ValueType::String,
                    2_000,
                ),
                test_entry(
                    TargetType::Commit,
                    "bbb222",
                    "review:status",
                    "\"approved\"",
                    ValueType::String,
                    3_000,
                ),
                test_entry(
                    TargetType::Project,
                    "",
                    "ci:url",
                    "\"https://ci.example\"",
                    ValueType::String,
                    500,
                ),
            ],
            promised_counts: BTreeMap::new(),
        }
    }

    fn app() -> App {
        App::new(snapshot(), 10_000)
    }

    fn press(app: &mut App, code: KeyCode) -> Option<Command> {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn press_ctrl(app: &mut App, code: KeyCode) -> Option<Command> {
        app.handle_key(KeyEvent::new(code, KeyModifiers::CONTROL))
    }

    #[test]
    fn enter_descends_and_esc_pops_restoring_selection() {
        let mut app = app();
        // Overview rows sort commit before project.
        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.view(), View::TargetList { .. }));

        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        match app.view() {
            View::KeyList { target_value, .. } => assert_eq!(target_value, "bbb222"),
            _ => panic!("expected key list"),
        }

        press(&mut app, KeyCode::Esc);
        match app.view() {
            View::TargetList { selected, .. } => assert_eq!(*selected, 1),
            _ => panic!("expected target list"),
        }
    }

    #[test]
    fn selection_clamps_at_bounds() {
        let mut app = app();
        press(&mut app, KeyCode::Char('k'));
        assert!(matches!(app.view(), View::Overview { selected: 0 }));

        press(&mut app, KeyCode::Char('G'));
        assert!(matches!(app.view(), View::Overview { selected: 1 }));
        press(&mut app, KeyCode::Char('j'));
        assert!(matches!(app.view(), View::Overview { selected: 1 }));
        press(&mut app, KeyCode::Char('g'));
        assert!(matches!(app.view(), View::Overview { selected: 0 }));
    }

    #[test]
    fn half_page_scroll_uses_viewport() {
        let mut app = App::new(
            MetaSnapshot {
                entries: (0..40)
                    .map(|i| {
                        test_entry(
                            TargetType::Commit,
                            &format!("sha{i:03}"),
                            "k",
                            "\"v\"",
                            ValueType::String,
                            i,
                        )
                    })
                    .collect(),
                promised_counts: BTreeMap::new(),
            },
            10_000,
        );
        app.set_viewport_rows(20);
        press(&mut app, KeyCode::Enter);
        press_ctrl(&mut app, KeyCode::Char('d'));
        match app.view() {
            View::TargetList { selected, .. } => assert_eq!(*selected, 10),
            _ => panic!("expected target list"),
        }
        press_ctrl(&mut app, KeyCode::Char('u'));
        match app.view() {
            View::TargetList { selected, .. } => assert_eq!(*selected, 0),
            _ => panic!("expected target list"),
        }
    }

    #[test]
    fn project_skips_target_list() {
        let mut app = app();
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        match app.view() {
            View::KeyList {
                target_type,
                target_value,
                ..
            } => {
                assert_eq!(*target_type, TargetType::Project);
                assert_eq!(target_value, "");
            }
            _ => panic!("expected key list"),
        }
    }

    #[test]
    fn filter_narrows_rows_and_esc_clears() {
        let mut app = app();
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('G'));

        press(&mut app, KeyCode::Char('/'));
        assert!(matches!(app.input_mode, InputMode::Filter));
        press(&mut app, KeyCode::Char('a'));
        press(&mut app, KeyCode::Char('a'));
        press(&mut app, KeyCode::Char('a'));

        // Only aaa111 matches; the previous selection (1) re-clamps to 0.
        match app.view() {
            View::TargetList {
                selected, filter, ..
            } => {
                assert_eq!(filter, "aaa");
                assert_eq!(*selected, 0);
            }
            _ => panic!("expected target list"),
        }
        assert_eq!(
            app.snapshot.target_rows(&TargetType::Commit, "aaa").len(),
            1
        );

        // Enter keeps the filter; a second `/` + Esc clears it.
        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.input_mode, InputMode::Normal));
        match app.view() {
            View::TargetList { filter, .. } => assert_eq!(filter, "aaa"),
            _ => panic!("expected target list"),
        }
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        match app.view() {
            View::TargetList { filter, .. } => assert_eq!(filter, ""),
            _ => panic!("expected target list"),
        }
    }

    #[test]
    fn open_detail_command_carries_coordinates() {
        let mut app = app();
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('j'));
        let cmd = press(&mut app, KeyCode::Enter);
        match cmd {
            Some(Command::OpenDetail {
                target_type,
                target_value,
                key,
                is_git_ref,
                last_timestamp,
            }) => {
                assert_eq!(target_type, TargetType::Commit);
                assert_eq!(target_value, "bbb222");
                assert_eq!(key, "review:status");
                assert!(!is_git_ref);
                assert_eq!(last_timestamp, 3_000);
            }
            _ => panic!("expected OpenDetail command"),
        }
        // The view does not change until the loop pushes the loaded detail.
        assert!(matches!(app.view(), View::KeyList { .. }));

        app.push_detail(
            "review:status".to_string(),
            DetailData {
                value: MetaValue::String("approved".to_string()),
                last_timestamp: 3_000,
                authorship: None,
            },
        );
        assert!(matches!(app.view(), View::Detail { scroll: 0, .. }));
    }

    #[test]
    fn detail_scrolls_by_lines() {
        let mut app = app();
        app.push_detail(
            "notes".to_string(),
            DetailData {
                value: MetaValue::String("a\nb\nc\nd".to_string()),
                last_timestamp: 0,
                authorship: None,
            },
        );
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Char('j'));
        match app.view() {
            View::Detail { scroll, .. } => assert_eq!(*scroll, 2),
            _ => panic!("expected detail"),
        }
        press(&mut app, KeyCode::Char('G'));
        match app.view() {
            View::Detail { scroll, .. } => assert_eq!(*scroll, 3),
            _ => panic!("expected detail"),
        }
        press(&mut app, KeyCode::Char('g'));
        match app.view() {
            View::Detail { scroll: 0, .. } => {}
            _ => panic!("expected detail scrolled to top"),
        }
    }

    #[test]
    fn quit_paths() {
        let mut app = app();
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('q'));
        assert!(app.should_quit());

        let mut app = App::new(snapshot(), 0);
        press(&mut app, KeyCode::Esc);
        assert!(app.should_quit());

        let mut app = App::new(snapshot(), 0);
        press_ctrl(&mut app, KeyCode::Char('c'));
        assert!(app.should_quit());
    }
}

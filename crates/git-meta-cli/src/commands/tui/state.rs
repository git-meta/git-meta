//! TUI state machine: a stack of navigation views plus key handling.
//!
//! This module owns navigation, selection, and pane focus but performs no
//! I/O. The detail pane's content is described by [`App::wanted_detail`];
//! the event loop loads it (a per-key database read) and hands it back via
//! [`App::set_detail`], which keeps everything here drivable by unit tests
//! with a synthetic snapshot.

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use git_meta_lib::types::{MetaValue, TargetType};

use super::data::{join_prefix, DetailData, KeyTreeRow, MetaSnapshot};

/// Whether keystrokes navigate or edit the current view's filter.
pub(super) enum InputMode {
    Normal,
    Filter,
}

/// Which pane receives navigation keys: the left navigation list or the
/// right value pane (where j/k scroll the value).
pub(super) enum PaneFocus {
    Nav,
    Detail,
}

/// One level of the browsing hierarchy, shown in the left pane. Each view
/// owns its selection and filter, so popping back restores them.
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
        /// Key namespace being browsed; empty at the root. Each deeper
        /// level is its own stacked `KeyList`.
        prefix: String,
        selected: usize,
        filter: String,
    },
    /// Global fuzzy search over full key paths (`type:value key`).
    Search {
        query: String,
        selected: usize,
    },
}

/// Coordinates of the key whose value belongs in the detail pane.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct DetailRequest {
    pub(super) target_type: TargetType,
    pub(super) target_value: String,
    pub(super) key: String,
    pub(super) is_git_ref: bool,
    pub(super) last_timestamp: i64,
}

const ROOT_VIEW: View = View::Overview { selected: 0 };

pub(super) struct App {
    pub(super) snapshot: MetaSnapshot,
    /// Wall-clock time captured at startup, for relative timestamps.
    pub(super) now_ms: i64,
    /// Never empty; `stack[0]` is always the overview.
    stack: Vec<View>,
    pub(super) input_mode: InputMode,
    pub(super) focus: PaneFocus,
    should_quit: bool,
    /// Transient message shown in the footer (e.g. a detail load failure).
    pub(super) status: Option<String>,
    /// Rows visible in the body area, for half-page scrolling.
    viewport_rows: usize,
    /// The loaded detail pane content, keyed by the request it answers.
    detail: Option<(DetailRequest, DetailData)>,
    pub(super) detail_scroll: usize,
}

impl App {
    pub(super) fn new(snapshot: MetaSnapshot, now_ms: i64) -> Self {
        Self {
            snapshot,
            now_ms,
            stack: vec![ROOT_VIEW],
            input_mode: InputMode::Normal,
            focus: PaneFocus::Nav,
            should_quit: false,
            status: None,
            viewport_rows: 20,
            detail: None,
            detail_scroll: 0,
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

    /// The key the detail pane should currently show, if any: the selected
    /// row of a key list or of the search results.
    pub(super) fn wanted_detail(&self) -> Option<DetailRequest> {
        match self.view() {
            View::KeyList {
                target_type,
                target_value,
                prefix,
                selected,
                filter,
            } => match self
                .snapshot
                .key_tree_rows(target_type, target_value, prefix, filter)
                .into_iter()
                .nth(*selected)
            {
                Some(KeyTreeRow::Leaf { row, .. }) => Some(DetailRequest {
                    target_type: target_type.clone(),
                    target_value: target_value.clone(),
                    key: row.key,
                    is_git_ref: row.is_git_ref,
                    last_timestamp: row.last_timestamp,
                }),
                _ => None,
            },
            View::Search { query, selected } => self
                .snapshot
                .search_rows(query)
                .into_iter()
                .nth(*selected)
                .map(|row| DetailRequest {
                    target_type: row.target_type,
                    target_value: row.target_value,
                    key: row.key,
                    is_git_ref: row.is_git_ref,
                    last_timestamp: row.last_timestamp,
                }),
            _ => None,
        }
    }

    pub(super) fn detail_matches(&self, request: &DetailRequest) -> bool {
        self.detail.as_ref().is_some_and(|(r, _)| r == request)
    }

    pub(super) fn detail(&self) -> Option<(&DetailRequest, &DetailData)> {
        self.detail.as_ref().map(|(r, d)| (r, d))
    }

    pub(super) fn set_detail(&mut self, request: DetailRequest, data: DetailData) {
        self.detail = Some((request, data));
        self.detail_scroll = 0;
    }

    pub(super) fn clear_detail(&mut self) {
        self.detail = None;
        self.detail_scroll = 0;
        self.focus = PaneFocus::Nav;
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) {
        self.status = None;
        if matches!(self.input_mode, InputMode::Filter) {
            self.handle_filter_key(key);
            return;
        }
        if matches!(self.view(), View::Search { .. }) {
            self.handle_search_key(key);
            return;
        }
        match self.focus {
            PaneFocus::Detail => self.handle_detail_key(key),
            PaneFocus::Nav => self.handle_nav_key(key),
        }
    }

    fn handle_nav_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let half_page = (self.viewport_rows / 2).max(1) as isize;
        match key.code {
            KeyCode::Char('c') if ctrl => self.should_quit = true,
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('d') if ctrl => self.move_selection(half_page),
            KeyCode::Char('u') if ctrl => self.move_selection(-half_page),
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1),
            KeyCode::Char('g') => self.jump_to(0),
            KeyCode::Char('G') => self.jump_to(usize::MAX),
            KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => self.descend(),
            KeyCode::Tab => {
                if self.wanted_detail().is_some() {
                    self.focus = PaneFocus::Detail;
                }
            }
            KeyCode::Esc => {
                if self.stack.len() > 1 {
                    self.stack.pop();
                } else {
                    self.should_quit = true;
                }
            }
            KeyCode::Char('h') | KeyCode::Left | KeyCode::Backspace => {
                if self.stack.len() > 1 {
                    self.stack.pop();
                }
            }
            KeyCode::Char('/') => {
                if matches!(self.view(), View::TargetList { .. } | View::KeyList { .. }) {
                    self.input_mode = InputMode::Filter;
                }
            }
            KeyCode::Char('s') => self.stack.push(View::Search {
                query: String::new(),
                selected: 0,
            }),
            _ => {}
        }
    }

    /// Keys while the value pane is focused: scroll the value, or return
    /// focus to the navigation pane.
    fn handle_detail_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let half_page = (self.viewport_rows / 2).max(1) as isize;
        match key.code {
            KeyCode::Char('c') if ctrl => self.should_quit = true,
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('d') if ctrl => self.scroll_detail(half_page),
            KeyCode::Char('u') if ctrl => self.scroll_detail(-half_page),
            KeyCode::Char('j') | KeyCode::Down => self.scroll_detail(1),
            KeyCode::Char('k') | KeyCode::Up => self.scroll_detail(-1),
            KeyCode::Char('g') => self.detail_scroll = 0,
            KeyCode::Char('G') => {
                self.detail_scroll = self.detail_line_count().saturating_sub(1);
            }
            KeyCode::Tab | KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => {
                self.focus = PaneFocus::Nav;
            }
            _ => {}
        }
    }

    /// Keys while searching: printable characters edit the query (including
    /// `q`, so quitting from here is Ctrl-C or Esc), arrows move through the
    /// results, Enter jumps to the selected key.
    fn handle_search_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('c') if ctrl => self.should_quit = true,
            KeyCode::Esc => {
                self.stack.pop();
            }
            KeyCode::Enter => self.jump_to_search_result(),
            KeyCode::Down => self.move_selection(1),
            KeyCode::Up => self.move_selection(-1),
            KeyCode::Char('n') if ctrl => self.move_selection(1),
            KeyCode::Char('p') if ctrl => self.move_selection(-1),
            KeyCode::Backspace => {
                if let Some(View::Search { query, .. }) = self.stack.last_mut() {
                    query.pop();
                }
                self.clamp_selection();
            }
            KeyCode::Char(c) if !ctrl => {
                if let Some(View::Search { query, .. }) = self.stack.last_mut() {
                    query.push(c);
                }
                self.clamp_selection();
            }
            _ => {}
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

    /// Rows in the current navigation view.
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
                prefix,
                filter,
                ..
            } => self
                .snapshot
                .key_tree_rows(target_type, target_value, prefix, filter)
                .len(),
            View::Search { query, .. } => self.snapshot.search_rows(query).len(),
        }
    }

    /// Scrollable lines in the loaded detail value.
    fn detail_line_count(&self) -> usize {
        match self.detail.as_ref().map(|(_, d)| &d.value) {
            Some(MetaValue::String(s)) => s.lines().count(),
            Some(MetaValue::List(entries)) => entries.len(),
            Some(MetaValue::Set(members)) => members.len(),
            _ => 0,
        }
    }

    fn selected_mut(&mut self) -> Option<&mut usize> {
        match self.stack.last_mut()? {
            View::Overview { selected }
            | View::TargetList { selected, .. }
            | View::KeyList { selected, .. }
            | View::Search { selected, .. } => Some(selected),
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let count = self.row_count();
        if count == 0 {
            return;
        }
        if let Some(selected) = self.selected_mut() {
            *selected = selected.saturating_add_signed(delta).min(count - 1);
        }
    }

    fn jump_to(&mut self, target: usize) {
        let count = self.row_count();
        if count == 0 {
            return;
        }
        let clamped = target.min(count - 1);
        if let Some(selected) = self.selected_mut() {
            *selected = clamped;
        }
    }

    fn clamp_selection(&mut self) {
        let count = self.row_count();
        if let Some(selected) = self.selected_mut() {
            *selected = (*selected).min(count.saturating_sub(1));
        }
    }

    fn scroll_detail(&mut self, delta: isize) {
        let count = self.detail_line_count();
        if count == 0 {
            return;
        }
        self.detail_scroll = self
            .detail_scroll
            .saturating_add_signed(delta)
            .min(count - 1);
    }

    /// Enter the selected row: push the next view down, or move focus to
    /// the value pane when a key is already selected.
    fn descend(&mut self) {
        let mut push: Option<View> = None;
        let mut focus_detail = false;

        match self.view() {
            View::Overview { selected } => {
                if let Some(row) = self.snapshot.type_rows().into_iter().nth(*selected) {
                    // Project metadata has a single implicit target, so skip
                    // the target list and go straight to its keys.
                    push = Some(if row.target_type == TargetType::Project {
                        View::KeyList {
                            target_type: row.target_type,
                            target_value: String::new(),
                            prefix: String::new(),
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
                        prefix: String::new(),
                        selected: 0,
                        filter: String::new(),
                    });
                }
            }
            View::KeyList {
                target_type,
                target_value,
                prefix,
                selected,
                filter,
            } => {
                match self
                    .snapshot
                    .key_tree_rows(target_type, target_value, prefix, filter)
                    .into_iter()
                    .nth(*selected)
                {
                    Some(KeyTreeRow::Namespace { segment, .. }) => {
                        push = Some(View::KeyList {
                            target_type: target_type.clone(),
                            target_value: target_value.clone(),
                            prefix: join_prefix(prefix, &segment),
                            selected: 0,
                            filter: String::new(),
                        });
                    }
                    Some(KeyTreeRow::Leaf { .. }) => focus_detail = true,
                    None => {}
                }
            }
            View::Search { .. } => {}
        }

        if let Some(view) = push {
            self.stack.push(view);
        }
        if focus_detail {
            self.focus = PaneFocus::Detail;
        }
    }

    /// Replace the navigation stack with the path to the selected search
    /// result, leaving its key selected so the detail pane follows.
    fn jump_to_search_result(&mut self) {
        let Some(View::Search { query, selected }) = self.stack.last() else {
            return;
        };
        let Some(row) = self.snapshot.search_rows(query).into_iter().nth(*selected) else {
            return;
        };

        let type_index = self
            .snapshot
            .type_rows()
            .iter()
            .position(|r| r.target_type == row.target_type)
            .unwrap_or(0);
        let mut stack = vec![View::Overview {
            selected: type_index,
        }];

        if row.target_type != TargetType::Project {
            let target_index = self
                .snapshot
                .target_rows(&row.target_type, "")
                .iter()
                .position(|r| r.target_value == row.target_value)
                .unwrap_or(0);
            stack.push(View::TargetList {
                target_type: row.target_type.clone(),
                selected: target_index,
                filter: String::new(),
            });
        }

        // One stacked key list per namespace level of the key, each with
        // the next segment (or the final leaf) selected.
        let segments: Vec<&str> = row.key.split(':').collect();
        let mut prefix = String::new();
        for (i, segment) in segments.iter().enumerate() {
            let is_last = i == segments.len() - 1;
            let level_index = self
                .snapshot
                .key_tree_rows(&row.target_type, &row.target_value, &prefix, "")
                .iter()
                .position(|r| match r {
                    KeyTreeRow::Namespace { segment: s, .. } => !is_last && s == segment,
                    KeyTreeRow::Leaf { segment: s, .. } => is_last && s == segment,
                })
                .unwrap_or(0);
            stack.push(View::KeyList {
                target_type: row.target_type.clone(),
                target_value: row.target_value.clone(),
                prefix: prefix.clone(),
                selected: level_index,
                filter: String::new(),
            });
            if !is_last {
                prefix = join_prefix(&prefix, segment);
            }
        }

        self.stack = stack;
        self.focus = PaneFocus::Nav;
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

    fn press(app: &mut App, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn press_ctrl(app: &mut App, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::CONTROL));
    }

    fn loaded_detail() -> DetailData {
        DetailData {
            value: MetaValue::String("a\nb\nc\nd".to_string()),
            last_timestamp: 0,
            authorship: None,
        }
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
    fn wanted_detail_appears_only_on_leaf_keys() {
        let mut app = app();
        assert!(app.wanted_detail().is_none());

        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        // Root key level of bbb222: namespaces agent and review, no leaves.
        assert!(app.wanted_detail().is_none());

        // Descend into the agent namespace: its leaf is selected.
        press(&mut app, KeyCode::Enter);
        match app.view() {
            View::KeyList { prefix, .. } => assert_eq!(prefix, "agent"),
            _ => panic!("expected key list"),
        }
        let request = app.wanted_detail().unwrap();
        assert_eq!(request.target_value, "bbb222");
        assert_eq!(request.key, "agent:model");

        // Back up and into the review namespace instead.
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        let request = app.wanted_detail().unwrap();
        assert_eq!(request.key, "review:status");
        assert_eq!(request.last_timestamp, 3_000);
    }

    #[test]
    fn enter_or_tab_focuses_value_pane_and_scrolls() {
        let mut app = app();
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        // Tab on a namespace row does nothing — there is no value yet.
        press(&mut app, KeyCode::Tab);
        assert!(matches!(app.focus, PaneFocus::Nav));

        // Descend to the leaf; enter then focuses the value pane.
        press(&mut app, KeyCode::Enter);
        let request = app.wanted_detail().unwrap();
        app.set_detail(request, loaded_detail());

        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.focus, PaneFocus::Detail));

        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Char('j'));
        assert_eq!(app.detail_scroll, 2);
        press(&mut app, KeyCode::Char('G'));
        assert_eq!(app.detail_scroll, 3);
        press(&mut app, KeyCode::Char('g'));
        assert_eq!(app.detail_scroll, 0);

        // Tab returns to the navigation pane; Esc pops back to the root
        // key level with the namespace still selected.
        press(&mut app, KeyCode::Tab);
        assert!(matches!(app.focus, PaneFocus::Nav));
        press(&mut app, KeyCode::Esc);
        match app.view() {
            View::KeyList {
                prefix, selected, ..
            } => {
                assert_eq!(prefix, "");
                assert_eq!(*selected, 0);
            }
            _ => panic!("expected key list"),
        }
    }

    #[test]
    fn search_narrows_and_jumps_to_key() {
        let mut app = app();
        press(&mut app, KeyCode::Char('s'));
        assert!(matches!(app.view(), View::Search { .. }));

        for c in "rev".chars() {
            press(&mut app, KeyCode::Char(c));
        }
        assert_eq!(app.snapshot.search_rows("rev").len(), 1);
        let request = app.wanted_detail().unwrap();
        assert_eq!(request.key, "review:status");

        press(&mut app, KeyCode::Enter);
        // Stack is rebuilt to overview → targets → one key level per
        // namespace segment, with the result selected at each level.
        assert_eq!(app.stack().len(), 4);
        match &app.stack()[2] {
            View::KeyList {
                prefix, selected, ..
            } => {
                assert_eq!(prefix, "");
                // Root key level of bbb222: agent, then review.
                assert_eq!(*selected, 1);
            }
            _ => panic!("expected key list"),
        }
        match app.view() {
            View::KeyList {
                target_value,
                prefix,
                selected,
                ..
            } => {
                assert_eq!(target_value, "bbb222");
                assert_eq!(prefix, "review");
                assert_eq!(*selected, 0);
            }
            _ => panic!("expected key list"),
        }
        assert_eq!(app.wanted_detail().unwrap().key, "review:status");
    }

    #[test]
    fn search_jump_to_project_key_skips_target_list() {
        let mut app = app();
        press(&mut app, KeyCode::Char('s'));
        for c in "ci:url".chars() {
            press(&mut app, KeyCode::Char(c));
        }
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.stack().len(), 3);
        match app.view() {
            View::KeyList {
                target_type,
                prefix,
                ..
            } => {
                assert_eq!(*target_type, TargetType::Project);
                assert_eq!(prefix, "ci");
            }
            _ => panic!("expected key list"),
        }
        assert_eq!(app.wanted_detail().unwrap().key, "ci:url");
    }

    #[test]
    fn search_typing_q_edits_query_instead_of_quitting() {
        let mut app = app();
        press(&mut app, KeyCode::Char('s'));
        press(&mut app, KeyCode::Char('q'));
        assert!(!app.should_quit());
        match app.view() {
            View::Search { query, .. } => assert_eq!(query, "q"),
            _ => panic!("expected search"),
        }

        press(&mut app, KeyCode::Esc);
        assert!(!app.should_quit());
        assert!(matches!(app.view(), View::Overview { .. }));
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

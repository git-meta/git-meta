//! `git meta tui` — interactive full-screen browser for metadata.
//!
//! Two panes: the left navigates (target types → targets → keys, plus a
//! global fuzzy search over key paths), the right shows the selected
//! level's preview or the selected key's value and metadata. `state` owns
//! the (unit-tested) state machine, `data` the snapshot/detail loading,
//! `ui` the rendering; this module owns the terminal lifecycle and event
//! loop.

mod data;
mod state;
mod ui;

use std::io::IsTerminal;

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyEventKind};
use time::OffsetDateTime;

use git_meta_lib::Session;

use crate::context::CommandContext;
use data::MetaSnapshot;
use state::App;

pub(crate) fn run() -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        anyhow::bail!(
            "`git meta tui` requires an interactive terminal; \
             use `git meta inspect` for non-interactive browsing"
        );
    }

    let ctx = CommandContext::open(None)?;
    let snapshot = MetaSnapshot::load(&ctx.session)?;
    if snapshot.is_empty() {
        println!("no metadata stored");
        return Ok(());
    }

    let now_ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    let mut app = App::new(snapshot, now_ms);

    let mut terminal = ratatui::try_init()?;
    let result = event_loop(&mut terminal, &mut app, &ctx.session);
    ratatui::restore();
    result
}

fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    session: &Session,
) -> Result<()> {
    loop {
        terminal.draw(|frame| ui::draw(frame, app))?;

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            // Header, footer, and pane borders take four rows; the rest
            // is the navigation list.
            let body_rows = terminal.size()?.height.saturating_sub(4);
            app.set_viewport_rows(body_rows as usize);
            app.handle_key(key);
            reconcile_detail(session, app);
        }

        if app.should_quit() {
            return Ok(());
        }
    }
}

/// Keep the detail pane in sync with the navigation selection: load the
/// selected key's value when it changed. Failures become a footer status
/// message rather than tearing down the UI.
fn reconcile_detail(session: &Session, app: &mut App) {
    let Some(request) = app.wanted_detail() else {
        app.clear_detail();
        return;
    };
    if app.detail_matches(&request) {
        return;
    }
    match data::load_detail(
        session,
        &request.target_type,
        &request.target_value,
        &request.key,
        request.is_git_ref,
        request.last_timestamp,
    ) {
        Ok(detail) => app.set_detail(request, detail),
        Err(e) => {
            let key = request.key.clone();
            app.clear_detail();
            app.set_status(format!("failed to load {key}: {e}"));
        }
    }
}

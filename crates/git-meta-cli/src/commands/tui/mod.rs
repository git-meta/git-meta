//! `git meta tui` — interactive full-screen browser for metadata.
//!
//! Navigation: overview of target types → targets of a type → keys of a
//! target → full value detail. `state` owns the (unit-tested) state
//! machine, `data` the snapshot/detail loading, `ui` the rendering; this
//! module owns the terminal lifecycle and event loop.

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
use state::{App, Command};

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
            // Header and footer each take one row; the rest is the body.
            let body_rows = terminal.size()?.height.saturating_sub(2);
            app.set_viewport_rows(body_rows as usize);
            if let Some(command) = app.handle_key(key) {
                execute(session, app, command);
            }
        }

        if app.should_quit() {
            return Ok(());
        }
    }
}

/// Run a state-machine command against the session. Failures become a
/// footer status message rather than tearing down the UI.
fn execute(session: &Session, app: &mut App, command: Command) {
    match command {
        Command::OpenDetail {
            target_type,
            target_value,
            key,
            is_git_ref,
            last_timestamp,
        } => {
            match data::load_detail(
                session,
                &target_type,
                &target_value,
                &key,
                is_git_ref,
                last_timestamp,
            ) {
                Ok(detail) => app.push_detail(key, detail),
                Err(e) => app.set_status(format!("failed to load {key}: {e}")),
            }
        }
    }
}

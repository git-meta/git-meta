use anyhow::Result;
use git_meta_lib::serialize::{SerializeMode, SerializeProgress};

use crate::context::CommandContext;

pub fn run(_verbose: bool, force_full: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let output = if force_full {
        ctx.session.serialize_full_with_progress(print_progress)?
    } else {
        ctx.session.serialize_with_progress(print_progress)?
    };

    if output.changes == 0 {
        println!("no metadata to serialize");
        return Ok(());
    }

    for ref_name in &output.refs_written {
        println!("serialized to {ref_name}");
    }

    if output.pruned > 0 {
        println!("auto-pruned {} entries", output.pruned);
    }

    Ok(())
}

fn print_progress(event: SerializeProgress) {
    match event {
        SerializeProgress::Reading { mode } => {
            let mode = match mode {
                SerializeMode::Incremental => "incremental",
                SerializeMode::Full => "full",
            };
            eprintln!("reading SQLite metadata ({mode})...");
        }
        SerializeProgress::Read {
            metadata,
            tombstones,
            set_tombstones,
            list_tombstones,
            changes,
        } => {
            eprintln!(
                "read {metadata} metadata rows, {tombstones} tombstones, {set_tombstones} set tombstones, {list_tombstones} list tombstones ({changes} changes)"
            );
        }
        SerializeProgress::Pruned { entries } => {
            eprintln!("filtered {entries} rows by prune settings");
        }
        SerializeProgress::Routed {
            destinations,
            records,
        } => {
            eprintln!("routed {records} records across {destinations} destination refs");
        }
        SerializeProgress::BuildingRef { ref_name, records } => {
            eprintln!("building {ref_name} ({records} records)...");
        }
        SerializeProgress::RefUnchanged { ref_name } => {
            eprintln!("{ref_name} unchanged");
        }
        SerializeProgress::RefWritten { ref_name } => {
            eprintln!("wrote {ref_name}");
        }
        SerializeProgress::AutoPruned {
            ref_name,
            keys_dropped,
            keys_retained,
        } => {
            eprintln!("auto-pruned {ref_name} ({keys_dropped} dropped, {keys_retained} retained)");
        }
    }
}

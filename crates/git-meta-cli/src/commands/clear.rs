use anyhow::Result;

use crate::context::CommandContext;

pub(crate) fn run(pattern: &str) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let removed = ctx.session.clear_key_prefix(pattern)?;

    if removed == 0 {
        eprintln!("no keys matching '{pattern}' found");
    } else {
        let noun = if removed == 1 { "entry" } else { "entries" };
        println!("cleared {removed} {noun}");
    }

    Ok(())
}

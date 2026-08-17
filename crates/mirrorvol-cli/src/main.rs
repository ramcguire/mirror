//! Local Syncthing bootstrap utility. Runtime backend administration belongs
//! exclusively to each node-local agent.

use clap::{Parser, Subcommand};
use mirrorvol_backend::syncthing::fetch_device_id;

#[derive(Parser)]
#[command(name = "mirrorvol-cli", about = "Mirror volume bootstrap utility")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Read one local Syncthing instance's stable device ID for enrollment.
    DeviceId {
        #[arg(long)]
        base_url: String,
        #[arg(long)]
        api_key: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::DeviceId { base_url, api_key } => {
            let device_id = fetch_device_id(&reqwest::Client::new(), &base_url, &api_key).await?;
            println!("{device_id}");
        }
    }
    Ok(())
}

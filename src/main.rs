#![feature(arc_unwrap_or_clone, let_chains, map_entry_replace, map_try_insert,
           result_option_inspect)]

use ahash::AHashMap;
use anyhow::Error;

use parking_lot::Mutex;

use std::env;
use std::sync::Arc;

use tokio::sync::mpsc;

mod config;
mod error;
mod git;
mod k8s;

use crate::config::Settings;
use crate::git::{CmRepoMap, main_loop};
use crate::k8s::run;


#[tokio::main]
async fn main() -> Result<(), Error> {
    let (tx_repo, rx_repo) = mpsc::channel(100);
    let (tx_cms, rx_cms) = mpsc::channel(100);
    let cm_repo: CmRepoMap = Arc::new(Mutex::new(AHashMap::new()));
    let cm_repo_cp = cm_repo.clone();

    let home = dirs::home_dir().expect("user's home directory not found");
    let setting_file = env::var("SETTING_FILE").unwrap_or_else(|_| ".githanos".to_string());
    let settings = Settings::new(home, &setting_file)?;
    let namespace = settings.namespace.clone();

    tokio::spawn(async move {
        main_loop(tx_repo, rx_cms, cm_repo, settings).await;
    });

    run(tx_cms, rx_repo, cm_repo_cp, namespace).await?;

    println!("terminating the process...");

    Ok(())
}

use ahash::{AHashMap, HashMap, HashMapExt};

use git2::{AutotagOption, Cred, FetchOptions, Progress, RemoteCallbacks, Repository};
use git2::build::{CheckoutBuilder, RepoBuilder};

use parking_lot::Mutex;
use tokio::fs::{remove_dir_all, create_dir_all};

use std::cell::RefCell;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use tokio::pin;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinSet, AbortHandle};
use tokio::time::{sleep, Duration};

use crate::config::Settings;


struct State {
    progress: Option<Progress<'static>>,
    total: usize,
    current: usize,
    path: Option<PathBuf>,
    newline: bool,
}

impl State {
    fn print(&mut self) {
        let stats = self.progress.as_ref().unwrap();
        let network_pct = (100 * stats.received_objects()) / stats.total_objects();
        let index_pct = (100 * stats.indexed_objects()) / stats.total_objects();
        let co_pct = if self.total > 0 {
            (100 * self.current) / self.total
        } else {
            0
        };
        let kbytes = stats.received_bytes() / 1024;
        if stats.received_objects() == stats.total_objects() {
            if !self.newline {
                println!();
                self.newline = true;
            }
            print!(
                "Resolving deltas {}/{}\r",
                stats.indexed_deltas(),
                stats.total_deltas()
            );
        } else {
            print!(
                "net {:3}% ({:4} kb, {:5}/{:5})  /  idx {:3}% ({:5}/{:5})  \
                 /  chk {:3}% ({:4}/{:4}) {}\r",
                network_pct,
                kbytes,
                stats.received_objects(),
                stats.total_objects(),
                index_pct,
                stats.indexed_objects(),
                stats.total_objects(),
                co_pct,
                self.current,
                self.total,
                self.path.as_ref()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            )
        }
        io::stdout().flush().unwrap()
    }
}

fn clone(
    user: &str,
    pass: &str,
    url: &str,
    path: &str,
    branch: &str,
) -> Result<(), git2::Error> {
    let state = RefCell::new(State {
        progress: None,
        total: 0,
        current: 0,
        path: None,
        newline: false,
    });
    let mut cb = RemoteCallbacks::new();
    cb.credentials(|_, _, _| {
        Ok(Cred::userpass_plaintext(user, pass)
            .expect("Cannot create credentials object."))
    });
    cb.transfer_progress(|stats| {
        let mut state = state.borrow_mut();
        state.progress = Some(stats.to_owned());
        state.print();
        true
    });

    let mut co = CheckoutBuilder::new();
    co.progress(|path, cur, total| {
        let mut state = state.borrow_mut();
        state.path = path.map(|p| p.to_path_buf());
        state.current = cur;
        state.total = total;
        state.print();
    });

    let mut fo = FetchOptions::new();
    fo.remote_callbacks(cb);
    RepoBuilder::new()
        .fetch_options(fo)
        .with_checkout(co)
        .branch(branch)
        .clone(url, Path::new(path))?;
    println!();

    Ok(())
}

fn fetch(
    user: &str,
    pass: &str,
    path: &str,
    branch: &str,
) -> Result<bool, git2::Error> {
    let repo = Repository::open(path)?;
	let remote = "origin";
    let mut ggpull = false;

    // Figure out whether it's a named remote or a URL
    println!("Fetching {} for repo", remote);
    let mut remote = repo.find_remote(remote)
        .or_else(|_| repo.remote_anonymous(remote))?;

    let mut cb = RemoteCallbacks::new();
    cb.credentials(|_, _, _| {
        Ok(Cred::userpass_plaintext(user, pass)
            .expect("Cannot create credentials object."))
    });
    cb.sideband_progress(|data| {
        print!("remote: {}", str::from_utf8(data).unwrap());
        io::stdout().flush().unwrap();
        true
    });

	// This callback gets called for each remote-tracking branch that gets
    // updated. The message we output depends on whether it's a new one or an
    // update.
    cb.update_tips(|refname, a, b| {
        if a.is_zero() {
            println!("[new]     {:20} {}", b, refname);
        } else {
            println!("[updated] {:10}..{:10} {}", a, b, refname);
        }
        true
    });

	// Here we show processed and total objects in the pack and the amount of
    // received data. Most frontends will probably want to show a percentage and
    // the download rate.
    cb.transfer_progress(|stats| {
        if stats.received_objects() == stats.total_objects() {
            print!(
                "Resolving deltas {}/{}\r",
                stats.indexed_deltas(),
                stats.total_deltas()
            );
        } else if stats.total_objects() > 0 {
            print!(
                "Received {}/{} objects ({}) in {} bytes\r",
                stats.received_objects(),
                stats.total_objects(),
                stats.indexed_objects(),
                stats.received_bytes()
            );
        }
        io::stdout().flush().unwrap();
        true
    });

	// Download the packfile and index it. This function updates the amount of
    // received data and the indexer stats which lets you inform the user about
    // progress.
    let mut fo = FetchOptions::new();
    fo.remote_callbacks(cb)
        .download_tags(git2::AutotagOption::All);
    remote.fetch(&[branch], Some(&mut fo), None)?;

    // If there are local objects (we got a thin pack), then tell the user
    // how many objects we saved from having to cross the network.
    let stats = remote.stats();
    if stats.local_objects() > 0 {
        println!(
            "\r* Received {}/{} objects in {} bytes (used {} local \
             objects)",
            stats.indexed_objects(),
            stats.total_objects(),
            stats.received_bytes(),
            stats.local_objects(),
        );
    } else {
        println!(
            "\r* Received {}/{} objects in {} bytes",
            stats.indexed_objects(),
            stats.total_objects(),
            stats.received_bytes(),
        );
    }

    if stats.indexed_objects() > 0 {
        let fetch_head = repo.find_reference("FETCH_HEAD")?;
        let rc = repo.reference_to_annotated_commit(&fetch_head)?;
        let refname = format!("refs/heads/{}", branch);
        let mut lb = repo.find_reference(&refname)?;
        fast_forward(&repo, &mut lb, &rc)?;
        ggpull = true;
    }

	// Disconnect the underlying connection to prevent from idling.
    remote.disconnect()?;

    // Update the references in the remote's namespace to point to the right
    // commits. This may be needed even if there was no packfile to download,
    // which can happen e.g. when the branches have been changed but all the
    // needed objects are available locally.
    remote.update_tips(None, true, AutotagOption::Unspecified, None)?;

    Ok(ggpull)
}

fn fast_forward(
    repo: &Repository,
    lb: &mut git2::Reference,
    rc: &git2::AnnotatedCommit,
) -> Result<(), git2::Error> {
    let name = match lb.name() {
        Some(s) => s.to_string(),
        None => String::from_utf8_lossy(lb.name_bytes()).to_string(),
    };
    let msg = format!("Fast-Forward: Setting {} to id: {}", name, rc.id());
    println!("{}", msg);
    lb.set_target(rc.id(), &msg)?;
    repo.set_head(&name)?;
    repo.checkout_head(Some(
        git2::build::CheckoutBuilder::default()
            // For some reason the force is required to make the working directory actually get
            // updated I suspect we should be adding some logic to handle dirty working directory
            // states but this is just an example so maybe not.
            .force(),
    ))?;

    Ok(())
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct CM {
    name: String,
    url: String,
    branch: String,
}

#[derive(Debug)]
enum Command {
    AddCmRepo(CM),
    DelCmRepo(CM),
}

async fn follow_repo(
    tx_repo: Sender<String>,
    cm_name: &str,
    url: &str,
    branch: &str,
    settings: Arc<Settings>,
) {
    let path = format!("/tmp/repos/{}", cm_name);

    let _ = remove_dir_all(&path).await;
    if create_dir_all(&path).await
        .map_err(|e| println!("Error: {}", &e))
        .is_ok()
    {
        match clone(
            &settings.username,
            &settings.apikey,
            url,
            &path,
            branch,
        ) {
            Ok(_) => {
                if let Err(e) = tx_repo.send(cm_name.to_string()).await {
                    println!("Error: {}", &e);
                }
                loop {
                    sleep(Duration::from_secs(settings.polling_interval)).await;
                    match fetch(
                        &settings.username,
                        &settings.apikey,
                        &path,
                        branch,
                    ) {
                        Ok(ggpull) if ggpull => {
                            if let Err(e) = tx_repo.send(cm_name.to_string()).await {
                                println!("Error: {}", &e);
                            }
                        },
                        Err(e) => println!("error: {}", &e),
                        _ => (),
                    }
                }
            },
            Err(e) => println!("error: {}", &e),
        }
    }
}

async fn watch_repos(tx_repo: Sender<String>, mut rx: Receiver<Command>, settings: Settings) {
    let mut set: JoinSet<()> = JoinSet::new();
    let mut aborts: HashMap<CM, AbortHandle> = HashMap::new();
    let settings = Arc::new(settings);
    while let Some(x) = rx.recv().await {
        match x {
            Command::AddCmRepo(cm) => {
                let settings = settings.clone();
                let cm_name = cm.name.clone();
                let url = cm.url.clone();
                let branch = cm.branch.clone();
                let tx_repo = tx_repo.clone();
                let abort_hdl = set.spawn(async move {
                    follow_repo(tx_repo, &cm_name, &url, &branch, settings).await;
                });
                if let Err(o) = aborts.try_insert(cm, abort_hdl) {
                    let (_, abort) = o.entry.replace_entry(o.value);
                    abort.abort();
                }
            },
            Command::DelCmRepo(cm) => {
                if let Some(abort) = aborts.remove(&cm) {
                    println!("Aborting...");
                    abort.abort();
                }
            },
        }
    }
}

pub type CmRepoMap = Arc<Mutex<AHashMap<String, Repo>>>;

#[derive(Clone, Eq, PartialEq)]
pub struct Repo {
    url: String,
    branch: String,
    pub thanos: Option<bool>,
}

impl Repo {
    fn new(url: &str, branch: &str, thanos: Option<bool>) -> Self {
        Self {
            url: url.to_string(),
            branch: branch.to_string(),
            thanos,
        }
    }
}

pub enum Msg {
    Create {
        cm: String,
        repo: String,
        branch: String,
        thanos: Option<bool>,
    },
    Destroy(String),
}

impl Msg {
    pub fn new_create(cm: String, repo: String, branch: String, thanos: Option<bool>) -> Self {
        Self::Create {
            cm,
            repo,
            branch,
            thanos,
        }
    }

    pub fn new_destroy(cm: String) -> Self {
        Self::Destroy(cm)
    }
}

pub async fn main_loop(
    tx_repo: Sender<String>,
    mut rx_cms: Receiver<Msg>,
    cm_repo: CmRepoMap,
    settings: Settings,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let repo_watcher = watch_repos(tx_repo, rx, settings);

    pin!(repo_watcher);

    loop {
        tokio::select! {
            Some(cm) = rx_cms.recv() => {
                match cm {
                    Msg::Create { cm, repo, branch, thanos } => {
                        let cm_name = cm.clone();
                        let old_value = match cm_repo.lock()
                            .try_insert(cm, Repo::new(&repo, &branch, thanos))
                        {
                            Err(o) => {
                                if o.value != *o.entry.get() {
                                    let (_, old_value) = o.entry.replace_entry(o.value);
                                    Some(old_value)
                                } else {
                                    continue;
                                }
                            },
                            _ => None,
                        };
                        if let Some(old_value) = old_value {
                            tx.send(Command::DelCmRepo(CM {
                                name: cm_name.clone(),
                                url: old_value.url,
                                branch: old_value.branch,
                            })).await
                            .expect("Communication channel is down");
                        }
                        let Repo { url, branch, .. } = cm_repo.lock()
                            .get(&cm_name)
                            .map(Clone::clone)
                            .expect("Imposible, value must exist!!!");
                        tx.send(Command::AddCmRepo(CM {
                            name: cm_name,
                            url,
                            branch,
                        })).await
                        .expect("Communication channel is down");
                    },
                    Msg::Destroy(cm) => {
                        let Some(ret) = cm_repo.lock().remove(&cm) else {
                            continue
                        };
                        tx.send(Command::DelCmRepo(CM {
                            name: cm,
                            url: ret.url,
                            branch: ret.branch,
                        })).await
                        .expect("Communication channel is down");
                    },
                }
            },
            _ = &mut repo_watcher => (),
        };
    }
}

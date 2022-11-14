use ahash::{AHashMap, AHashSet};

use crate::error::{Error, Result};
use crate::git::{CmRepoMap, Msg};

use futures::{select, StreamExt, TryStreamExt, FutureExt, Future};
use futures::future::{self, join_all};

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::runtime::{watcher, WatchStreamExt};
use kube::{self, Api, Client, ResourceExt};
use kube::api::{ListParams, PostParams};
use kube::runtime::controller::{Controller, Action};

use parking_lot::Mutex;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::Error as IOError;
use std::os::unix::prelude::OsStrExt;
use std::sync::Arc;
use std::time::Duration;

use tokio::fs::{read_dir, read_to_string};
use tokio::pin;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio_stream::wrappers::{ReceiverStream, ReadDirStream};


type CmIpsMap = Arc<Mutex<AHashMap<String, Vec<String>>>>;
type CmChanged = Arc<Mutex<AHashSet<String>>>;

struct Context {
    client: Client,
    namespace: String,
    tx_cms: Sender<Msg>,
    cm_ips: CmIpsMap,
    cm_repo: CmRepoMap,
    cm_changed: CmChanged,
}

impl Context {
    fn new(
        client: Client,
        tx_cms: Sender<Msg>,
        cm_ips: CmIpsMap,
        cm_repo: CmRepoMap,
        cm_changed: CmChanged,
        namespace: String,
    ) -> Self {
        Self {
            client,
            tx_cms,
            cm_ips,
            cm_repo,
            cm_changed,
            namespace,
        }
    }
}

fn error_policy(_obj: Arc<ConfigMap>, _error: &Error, _ctx: Arc<Context>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

async fn directory_to_data_map(obj_name: &str)
-> Result<Vec<impl Future<Output = Result<(String, String), IOError>>>, IOError> {
   let mut vec = vec![];
   let rd = read_dir(&format!("/tmp/repos/{}", obj_name)).await?;
   let rds = ReadDirStream::new(rd)
        .filter_map(|file| {
            future::ready(
                file.inspect_err(|e| println!("Error: {}", e))
                    .ok()
            )
        })
        .filter_map(|file| async {
            file.file_type().await
                .inspect_err(|e| println!("Error: {}", e))
                .ok()
                .and_then(|file_type| {
                    if !file_type.is_dir()
                        && file.path().extension() == Some(OsStr::from_bytes(b"yaml"))
                    {
                        Some(file)
                    } else {
                        None
                    }
                })
        });

    pin!(rds);

    while let Some(file) = rds.next().await {
        let file_name = file.file_name()
            .into_string()
            .expect("Caracterres extraños no UTF-8");
        vec.push(read_to_string(file.path())
            .map(|c| c.map(|c| (file_name, c))));
    }

    Ok(vec)
}

fn trans(ips: Option<&Vec<String>>)
-> Option<Vec<impl Future<Output = Result<reqwest::Response, reqwest::Error>>>>
{
    let client = reqwest::Client::new();
    ips.map(|ips| {
        ips.iter()
            .map(|ip| client.post(format!("http://{}:10902/-/reload", ip.as_str())).send())
            .collect::<Vec<_>>()
    })
}

async fn notify(
    requests: Option<Vec<impl Future<Output = Result<reqwest::Response, reqwest::Error>>>>
) {
    if let Some(requests) = requests {
        for i in join_all(requests).await {
            if let Err(e) = i {
                println!("Error: {}", &e);
            }
        }
    }
}

async fn reconcile(obj: Arc<ConfigMap>, ctx: Arc<Context>) -> Result<Action> {
    let api_cms = Api::<ConfigMap>::namespaced(ctx.client.clone(), ctx.namespace.as_str());
    let obj_name = obj.name_any();
    let mut clean_map = true;

    println!("reconcile request: ConfigMap {} in namespace: {}",
             &obj_name,
             obj.namespace().as_deref().unwrap_or("`unknown`"));

    let annotations = obj.annotations();
    if !annotations.is_empty() {
        let mut repo = None;
        let mut branch = None;
        let mut thanos = None;
        for (k, v) in annotations.iter() {
            match k.as_str() {
                "githanos.repo" => repo = Some(v.clone()),
                "githanos.branch" => branch = Some(v.clone()),
                "githanos.thanos" => thanos = Some(v.parse::<bool>().unwrap_or_default()),
                _ => (),
            };
        }
        if let (Some(repo), Some(branch), thanos) = (repo, branch, thanos) {
            clean_map = false;
            let msg = Msg::new_create(obj.name_any(), repo, branch, thanos);
            if let Err(e) = ctx.tx_cms.send(msg).await {
                println!("Error: {}", &e);
            }
        }
    }

    if clean_map {
        // Existia un repo y ahora se ha eliminado
        let repo = ctx.cm_repo.lock().remove(&obj_name);
        // notificar 
        if let Some(repo) = repo && let Some(_thanos @ true) = repo.thanos {
            if let Err(e) = ctx.tx_cms.send(Msg::new_destroy(obj.name_any())).await {
                println!("Error: {}", &e);
            }
            let requests = trans(ctx.cm_ips.lock().get(&obj_name));
            notify(requests).await;
        }
    }

    if ctx.cm_changed.lock().remove(&obj_name) {
        // For each directory:
        let Ok(vec) = directory_to_data_map(&obj_name).await else {
            return Ok(Action::requeue(Duration::from_secs(3600))); // TODO: transformar el error
        };
        let mut data: BTreeMap<String, String> = BTreeMap::new();
        for i in join_all(vec).await {
            match i {
                Ok((file_name, content)) => { data.insert(file_name, content); },
                Err(e) => println!("Error: {}", &e),
            }
        }
        let mut patch = (*obj).clone();
        patch.data = Some(data);
        match api_cms.replace(obj_name.as_str(), &PostParams::default(), &patch).await {
            Ok(_) => {
                let do_notify = {
                    let repo_mutex = ctx.cm_repo.lock();
                    if let Some(repo) = repo_mutex.get(&obj_name)
                        && let Some(_thanos @ true) = repo.thanos
                    {
                        true
                    } else {
                        false
                    }
                };
                if do_notify {
                    let requests = trans(ctx.cm_ips.lock().get(&obj_name));
                    notify(requests).await;
                }
            },
            Err(e) => println!("Err: {}", &e),
        }
    }

    Ok(Action::requeue(Duration::from_secs(3600)))
}

pub async fn run(
    tx_cms: Sender<Msg>,
    rx_repo: Receiver<String>,
    cm_repo: CmRepoMap,
    namespace: String,
) -> Result<(), kube::Error> {
    let client = Client::try_default().await?;
    let cms = Api::<ConfigMap>::namespaced(client.clone(), &namespace);
    let pods = Api::<Pod>::namespaced(client.clone(), &namespace);
    let pod_watcher = watcher(pods, ListParams::default());
    let cm_ips: CmIpsMap = Arc::new(Mutex::new(AHashMap::new()));
    let cm_changed: CmChanged = Arc::new(Mutex::new(AHashSet::new()));
    let ctx = Arc::new(
        Context::new(client, tx_cms, cm_ips.clone(), cm_repo, cm_changed.clone(), namespace));

    let rx_repo_stream = ReceiverStream::new(rx_repo)
        .map(move |cm_name| { cm_changed.lock().insert(cm_name); });

    let pod_watcher_stream = pod_watcher.applied_objects()
        .try_filter_map(move |p| {
            let pod_name = p.name_any();
            let Some(pod_ip) = p.status.and_then(|status| status.pod_ip) else {
                println!("Applied: {} not allocated", &pod_name);
                return future::ok(None);
            };
            let Some(pod_vols) = p.spec.and_then(|spec| spec.volumes) else {
                println!("Applied: {} without volumes", &pod_name);
                return future::ok(None);
            };
            let config_maps_names = pod_vols.into_iter()
                .filter_map(|vol| vol.config_map)
                .filter_map(|config_map| config_map.name)
                .collect::<Vec<String>>();
                println!("Applied: {} with IP: {}, config_maps: {:?}",
                        &pod_name,
                        &pod_ip,
                        &config_maps_names);

            future::ok(
                if !config_maps_names.is_empty() {
                    Some((pod_ip, config_maps_names))
                } else {
                    println!("Applied: {} without volumes", &pod_name);
                    None
                }
            )
        })
        .fuse();

    let controller_stream = Controller::new(cms, Default::default())
        .shutdown_on_signal()
        .reconcile_all_on(rx_repo_stream)
        .run(reconcile, error_policy, ctx)
        .map_err(|e| println!("Error: {}", e))
        .for_each(|_| future::ready(()))
        .fuse();

    pin!(pod_watcher_stream);
    pin!(controller_stream);

    loop {
        select! {
            res = pod_watcher_stream.next() => {
                match res {
                    Some(Ok((ip, config_maps_names))) => {
                        let mut cm_ips = cm_ips.lock();
                        for config_map_name in config_maps_names.into_iter() {
                            if let Err(mut o) = cm_ips
                                .try_insert(config_map_name, vec![ip.clone()])
                            {
                                let ips = o.entry.get_mut();
                                let ip = o.value.pop().unwrap();
                                if !ips.iter().any(|x| x == &ip) {
                                    ips.push(ip);
                                }
                            }
                        }
                    },
                    Some(Err(e)) => {
                        println!("Error: {}", &e);
                        break;
                    }
                    _ => break,
                }
            },
            _ = controller_stream => break,
            complete => break,
        };
    }

    Ok(())
}

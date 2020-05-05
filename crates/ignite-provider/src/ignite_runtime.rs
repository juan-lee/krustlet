use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::{thread, time};

use log::{debug, info};
use tempfile::NamedTempFile;
use tokio::sync::watch::{self, Sender};
use tokio::task::JoinHandle;

use kubelet::handle::{RuntimeHandle, Stop};
use kubelet::status::ContainerStatus;

pub struct HandleStopper {
    handle: JoinHandle<anyhow::Result<()>>,

    container: String,
}

#[async_trait::async_trait]
impl Stop for HandleStopper {
    async fn stop(&mut self) -> anyhow::Result<()> {
        debug!("stopping {:?}", self.container);
        let output = Command::new("sudo")
            .arg("/home/jpang/.wks/bin/ignite")
            .arg("rm")
            .arg("-f")
            .arg(self.container.clone())
            .output()
            .expect("failed to ignite");

        info!("command: {:?}", output);
        Ok(())
    }

    async fn wait(&mut self) -> anyhow::Result<()> {
        (&mut self.handle).await??;
        Ok(())
    }
}

/// IgniteRuntime provides a WASI compatible runtime. A runtime should be used for
/// each "instance" of a process and can be passed to a thread pool for running
pub struct IgniteRuntime {
    /// Data needed for the runtime
    data: Arc<Data>,
    /// The tempfile that output from the wasmtime process writes to
    output: Arc<NamedTempFile>,
}

struct Data {
    /// container name
    name: String,
    /// container image
    image: String,
    /// key/value environment variables made available to the wasm process
    env: HashMap<String, String>,
    /// the arguments passed as the command-line arguments list
    args: Vec<String>,
    /// a hash map of local file system paths to optional path names in the runtime
    /// (e.g. /tmp/foo/myfile -> /app/config). If the optional value is not given,
    /// the same path will be allowed in the runtime
    dirs: HashMap<PathBuf, Option<PathBuf>>,
}

impl IgniteRuntime {
    /// Creates a new IgniteRuntime
    ///
    /// # Arguments
    ///
    /// * `module_path` - the path to the WebAssembly binary
    /// * `env` - a collection of key/value pairs containing the environment variables
    /// * `args` - the arguments passed as the command-line arguments list
    /// * `dirs` - a map of local file system paths to optional path names in the runtime
    ///     (e.g. /tmp/foo/myfile -> /app/config). If the optional value is not given,
    ///     the same path will be allowed in the runtime
    /// * `log_dir` - location for storing logs
    pub async fn new<L: AsRef<Path> + Send + Sync + 'static>(
        name: String,
        image: String,
        env: HashMap<String, String>,
        args: Vec<String>,
        dirs: HashMap<PathBuf, Option<PathBuf>>,
        log_dir: L,
    ) -> anyhow::Result<Self> {
        let temp = tokio::task::spawn_blocking(move || -> anyhow::Result<NamedTempFile> {
            Ok(NamedTempFile::new_in(log_dir)?)
        })
        .await??;

        // We need to use named temp file because we need multiple file handles
        // and if we are running in the temp dir, we run the possibility of the
        // temp file getting cleaned out from underneath us while running. If we
        // think it necessary, we can make these permanent files with a cleanup
        // loop that runs elsewhere. These will get deleted when the reference
        // is dropped
        Ok(IgniteRuntime {
            data: Arc::new(Data {
                name,
                image,
                env,
                args,
                dirs,
            }),
            output: Arc::new(temp),
        })
    }

    pub async fn start(&self) -> anyhow::Result<RuntimeHandle<HandleStopper, tokio::fs::File>> {
        let temp = self.output.clone();
        // Because a reopen is blocking, run in a blocking task to get new
        // handles to the tempfile
        let (_output_write, output_read) = tokio::task::spawn_blocking(
            move || -> anyhow::Result<(std::fs::File, std::fs::File)> {
                Ok((temp.reopen()?, temp.reopen()?))
            },
        )
        .await??;

        let (status_sender, status_recv) = watch::channel(ContainerStatus::Waiting {
            timestamp: chrono::Utc::now(),
            message: "No status has been received from the process".into(),
        });
        let (container, handle) = self.spawn_ignite(status_sender).await?;

        Ok(RuntimeHandle::new(
            HandleStopper { handle, container },
            tokio::fs::File::from_std(output_read),
            status_recv,
        ))
    }

    // Spawns a running wasmtime instance with the given context and status
    // channel. Due to the Instance type not being Send safe, all of the logic
    // needs to be done within the spawned task
    async fn spawn_ignite(
        &self,
        status_sender: Sender<ContainerStatus>,
    ) -> anyhow::Result<(String, JoinHandle<anyhow::Result<()>>)> {
        // Clone the module data Arc so it can be moved
        let data = self.data.clone();

        debug!("Starting container {} on thread", &data.image);
        for (key, value) in &data.env {
            debug!("env: {}: {}", key, value);
        }
        for value in &data.args {
            debug!("arg: {}", value);
        }
        for (key, value) in &data.dirs {
            debug!("dir: {:?}: {:?}", key.to_str(), value);
        }

        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            info!("starting run of module");

            if data.image != "k8s.gcr.io/kube-proxy:v1.17.2" {
                // sudo $HOME/.wks/bin/ignite run weaveworks/ignite-ubuntu --cpus 1 --memory 1GB --ssh --name my-vm
                let output = Command::new("sudo")
                    .arg("/home/jpang/.wks/bin/ignite")
                    .arg("run")
                    .arg(data.image.clone())
                    .arg("--cpus")
                    .arg("1")
                    .arg("--memory")
                    .arg("1GB")
                    .arg("--ssh")
                    .arg("--name")
                    .arg(data.name.clone())
                    .arg("--log-level")
                    .arg("error")
                    .arg("-q")
                    .output()
                    .expect("failed to ignite");

                info!("command: {:?}", output);
                status_sender
                    .broadcast(ContainerStatus::Running {
                        timestamp: chrono::Utc::now(),
                    })
                    .expect("status should be able to send");

                loop {
                    info!("sleeping before inspect");
                    thread::sleep(time::Duration::from_secs(5));
                    let inspect_output = Command::new("sudo")
                        .arg("/home/jpang/.wks/bin/ignite")
                        .arg("inspect")
                        .arg("vm")
                        .arg(str::from_utf8(&output.stdout).unwrap().trim())
                        .output()
                        .expect("failed to inspect vm");
                    info!("inspect command: {:?}", inspect_output);
                    if !inspect_output.status.success() {
                        info!("inspect break");
                        break;
                    }
                }
            }

            info!("module run complete");
            status_sender
                .broadcast(ContainerStatus::Terminated {
                    failed: false,
                    message: "Module run completed".into(),
                    timestamp: chrono::Utc::now(),
                })
                .expect("status should be able to send");
            Ok(())
        });
        Ok((self.data.name.clone(), handle))
    }
}

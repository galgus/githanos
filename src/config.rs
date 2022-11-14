use config::{ConfigError, Config, File};

use serde::Deserialize;
use std::path::PathBuf;


#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub username: String,
    pub apikey: String,
    pub polling_interval: u64,
    pub namespace: String,
}

impl Settings {
    pub fn new(path: PathBuf, app_name: &str) -> Result<Self, ConfigError> {
        let mut home = path;
        home.push(app_name);

        let settings = Config::builder()
            .add_source(File::from(home))
            .build()?;

        settings.try_deserialize()
    }
}

use serde::de::DeserializeOwned;

use crate::traits::FromReader;

/// An adapter for deserializing a Toml document from a buffered reader
#[derive(Debug, Clone)]
pub struct TomlOf<T>(pub T);
impl<T: DeserializeOwned> FromReader for TomlOf<T> {
    type Error = std::io::Error;
    fn from_reader(reader: &mut impl std::io::BufRead) -> Result<Self, Self::Error> {
        let string = String::from_reader(reader)?;
        let toml = toml::from_str(&string)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        Ok(TomlOf(toml))
    }
}

impl<T> crate::fs::PathTo<TomlOf<T>> {
    /// Returns a reference to the inner Toml datatype
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> anyhow::Result<()> {
    /// use clap::Parser;
    /// use clap_adapters::prelude::*;
    ///
    /// #[derive(Debug, Parser)]
    /// struct Cli {
    ///     #[clap(long)]
    ///     config: PathTo<TomlOf<serde_json::Value>>,
    /// }
    ///
    /// // Create a config file in a temporary directory
    /// let config_dir = tempfile::tempdir()?;
    /// let config_path = config_dir.path().join("config.json");
    /// let config_path_string = config_path.display().to_string();
    ///
    /// // Write a test config to the config file
    /// let config_string = r#"hello = "world""#;
    /// std::fs::write(&config_path, &config_string)?;
    ///
    /// // Parse our CLI, passing our config file path to --config
    /// let cli = Cli::parse_from(["app", "--config", &config_path_string]);
    /// let value = cli.config.data();
    ///
    /// // We should expect the value we get to match what we wrote to the config
    /// assert_eq!(value, &serde_json::json!({"hello":"world"}));
    /// # Ok(())
    /// # }
    /// ```
    pub fn data(&self) -> &T {
        &self.data.0
    }

    /// Returns the owned inner datatype parsed from Toml
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> anyhow::Result<()> {
    /// use clap::Parser;
    /// use clap_adapters::prelude::*;
    ///
    /// #[derive(Debug, Parser)]
    /// struct Cli {
    ///     #[clap(long)]
    ///     config: PathTo<TomlOf<serde_json::Value>>,
    /// }
    ///
    /// // Create a config file in a temporary directory
    /// let config_dir = tempfile::tempdir()?;
    /// let config_path = config_dir.path().join("config.json");
    /// let config_path_string = config_path.display().to_string();
    ///
    /// // Write a test config to the config file
    /// let config_string = r#"hello = "world""#;
    /// std::fs::write(&config_path, &config_string)?;
    ///
    /// // Parse our CLI, passing our config file path to --config
    /// let cli = Cli::parse_from(["app", "--config", &config_path_string]);
    /// let data = cli.config.into_data();
    ///
    /// // We should expect the value we get to match what we wrote to the config
    /// assert_eq!(data, serde_json::json!({"hello":"world"}));
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_data(self) -> T {
        self.data.0
    }
}

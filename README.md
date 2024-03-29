# `clap-adapters`

Adapter types for declaratively loading configurations with [`clap`]

Did you know that any type that implements [`FromStr`] can
be used in a [`clap`] derive struct? That means that any
logic you can fit into a `fn(&str) -> Result<T, Error>` can
be run at parsing-time. This can be expecially useful for
declaratively selecting config files or doing other cool
stuff. Check this out:

[`clap`]: https://docs.rs/clap

```rust
use clap::Parser;
use clap_adapters::prelude::*;

#[derive(Debug, Parser)]
struct Cli {
    /// Path to a config file of arbitrary Json
    #[clap(long)]
    config: PathTo<JsonOf<serde_json::Value>>,
}

fn main() {
    // Create a config file in a temporary directory
    let config_dir = tempfile::tempdir()?;
    let config_path = config_dir.path().join("config.json");
    let config_path_string = config_path.display().to_string();

    // Write a test config of {"hello":"world"} to the config file
    let config = serde_json::json!({"hello": "world"});
    let config_string = serde_json::to_string(&config)?;
    std::fs::write(&config_path, &config_string)?;

    // Parse our CLI, passing our config file path to --config
    let cli = Cli::parse_from(["app", "--config", &config_path_string]);
    let data = cli.config.data();

    // We should expect the value we get to match what we wrote to the config
    assert_eq!(data, &serde_json::json!({"hello":"world"}));
}
```

# Extending `clap-adapters`

You can implement additional composable adapters by defining new types that
implement traits in this crate. For example, by implementing `FromReader`,
you could define an adapter that can construct itself from a file path, nesting
your adapter into `PathTo<T>`, like `PathTo<YourAdapter>`.

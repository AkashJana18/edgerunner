use std::path::Path;

use anyhow::{Context, Result};
use edgerunner_core::{DislocationConfig, EngineConfig, OrderMode, PaperConfig, RiskConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct BackendConfig {
    pub decision_history: usize,
    pub strategy: DislocationConfig,
    pub risk: RiskConfig,
    pub paper: PaperConfig,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            decision_history: 100,
            strategy: DislocationConfig::default(),
            risk: RiskConfig::default(),
            paper: PaperConfig::default(),
        }
    }
}

impl BackendConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("read backend config {}", path.display()))?;
        toml::from_str(&contents)
            .with_context(|| format!("parse backend config {}", path.display()))
    }

    pub fn engine(&self) -> EngineConfig {
        EngineConfig {
            mode: OrderMode::Paper,
            risk: self.risk.clone(),
            paper: self.paper.clone(),
            decision_history: self.decision_history,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_toml_inherits_safe_defaults() {
        let config: BackendConfig = toml::from_str(
            r#"
                decision_history = 25
                [strategy]
                minimum_edge_micros = 30000
            "#,
        )
        .unwrap();
        assert_eq!(config.decision_history, 25);
        assert_eq!(config.strategy.minimum_edge_micros, 30_000);
        assert_eq!(config.strategy.order_size, 25);
        assert_eq!(config.risk.max_position, 250);
    }
}

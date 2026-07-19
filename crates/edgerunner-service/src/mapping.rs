use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use edgerunner_adapters::{
    PascalMarket, PascalMarketClient, TxLineDiscoveryClient, TxLineFixture, TxLineOddsLine,
};
use edgerunner_core::{MarketMapping, TxLineMarketSelection};

#[derive(Clone)]
pub struct DiscoveryConfig {
    pub txline_origin: String,
    pub api_token: String,
    pub pascal_ws: String,
    fixture_id_override: Option<u64>,
    market_override: Option<String>,
    pascal_symbol_override: Option<String>,
}

#[derive(Clone)]
pub struct ResolvedLiveFeed {
    pub mapping: MarketMapping,
    pub event_label: String,
    pub contract_label: String,
    pub market_period: String,
    pub expected_event_start_time_ms: Option<u64>,
    pub txline_origin: String,
    pub api_token: String,
    pub pascal_ws: String,
}

impl DiscoveryConfig {
    pub fn from_environment(
        market_override: Option<String>,
        pascal_symbol_override: Option<String>,
    ) -> Result<Self> {
        let fixture_id_override = optional_env("TXLINE_FIXTURE_ID")
            .map(|value| {
                value
                    .parse()
                    .context("TXLINE_FIXTURE_ID must be an unsigned integer")
            })
            .transpose()?;
        Ok(Self {
            txline_origin: std::env::var("TXLINE_ORIGIN")
                .unwrap_or_else(|_| "https://txline-dev.txodds.com".into()),
            api_token: required_env("TXLINE_API_TOKEN")?,
            pascal_ws: std::env::var("PASCAL_WS_URL")
                .unwrap_or_else(|_| "wss://data.pascal.trade/ws".into()),
            fixture_id_override,
            market_override: market_override.or_else(|| optional_env("TXLINE_MARKET")),
            pascal_symbol_override: pascal_symbol_override
                .or_else(|| optional_env("PASCAL_SYMBOL")),
        })
    }

    pub fn optional_from_environment(
        market_override: Option<String>,
        pascal_symbol_override: Option<String>,
    ) -> Result<Option<Self>> {
        if optional_env("TXLINE_API_TOKEN").is_none() {
            return Ok(None);
        }
        Self::from_environment(market_override, pascal_symbol_override).map(Some)
    }

    pub async fn resolve(&self) -> Result<ResolvedLiveFeed> {
        let txline =
            TxLineDiscoveryClient::new(self.txline_origin.clone(), self.api_token.clone())?;
        let fixtures = txline.fixtures().await?;
        let now_ms = epoch_ms();
        let fixtures = fixture_candidates(fixtures, self.fixture_id_override, now_ms);
        if fixtures.is_empty() {
            bail!("TxLINE did not return a live or upcoming fixture for discovery");
        }

        let pascal = PascalMarketClient::from_ws_url(&self.pascal_ws)?;
        let markets = pascal.list_markets().await?;
        for fixture in fixtures {
            let candidates =
                matching_markets(&fixture, &markets, self.pascal_symbol_override.as_deref());
            if candidates.is_empty() {
                continue;
            }
            let odds_lines = txline.odds_snapshot(fixture.fixture_id).await?;
            for market in candidates {
                if let Some(selection) = select_outcome(&fixture, &market, &odds_lines) {
                    let home = home_participant(&fixture);
                    let away = away_participant(&fixture);
                    return Ok(ResolvedLiveFeed {
                        event_label: format!("{home} vs {away}"),
                        contract_label: market.market_description.clone(),
                        market_period: market.market_period.clone(),
                        expected_event_start_time_ms: market.expected_event_start_time_ms,
                        mapping: MarketMapping {
                            fixture_id: fixture.fixture_id,
                            fixture_label: format!("{home} vs {away}"),
                            fixture_start_time_ms: fixture.start_time_ms,
                            market: self
                                .market_override
                                .clone()
                                .unwrap_or_else(|| market.symbol.clone()),
                            pascal_symbol: market.symbol,
                            txline_selection: selection,
                        },
                        txline_origin: self.txline_origin.clone(),
                        api_token: self.api_token.clone(),
                        pascal_ws: self.pascal_ws.clone(),
                    });
                }
            }
        }
        bail!(
            "no current TxLINE fixture could be matched to an active Pascal market and TxLINE outcome"
        )
    }
}

fn fixture_candidates(
    fixtures: Vec<TxLineFixture>,
    fixture_id_override: Option<u64>,
    now_ms: u64,
) -> Vec<TxLineFixture> {
    let mut candidates = fixtures
        .into_iter()
        .filter(|fixture| fixture.game_state != Some(6))
        .filter(|fixture| {
            fixture_id_override.is_none_or(|fixture_id| fixture.fixture_id == fixture_id)
        })
        .filter(|fixture| fixture.start_time_ms.saturating_add(6 * 60 * 60 * 1_000) >= now_ms)
        .collect::<Vec<_>>();
    candidates.sort_by_key(|fixture| fixture_priority(fixture.start_time_ms, now_ms));
    candidates
}

fn fixture_priority(start_time_ms: u64, now_ms: u64) -> (u8, u64) {
    if start_time_ms <= now_ms {
        (0, now_ms.saturating_sub(start_time_ms))
    } else {
        (1, start_time_ms.saturating_sub(now_ms))
    }
}

fn matching_markets(
    fixture: &TxLineFixture,
    markets: &[PascalMarket],
    pascal_symbol_override: Option<&str>,
) -> Vec<PascalMarket> {
    let home = normalize(&home_participant(fixture));
    let away = normalize(&away_participant(fixture));
    let mut matches = markets
        .iter()
        .filter(|market| {
            if let Some(symbol) = pascal_symbol_override {
                market.symbol == symbol
            } else {
                market_matches_fixture(market, &home, &away, fixture.start_time_ms)
            }
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by_key(|market| market_priority(market, fixture.start_time_ms));
    matches
}

fn market_matches_fixture(
    market: &PascalMarket,
    home: &str,
    away: &str,
    fixture_start_time_ms: u64,
) -> bool {
    let description = normalize(&market.event_description);
    let teams_match = description.contains(home) && description.contains(away);
    let timing_matches = market
        .expected_event_start_time_ms
        .is_none_or(|start_time| {
            start_time.abs_diff(fixture_start_time_ms) <= 36 * 60 * 60 * 1_000
        });
    teams_match && timing_matches
}

fn market_priority(market: &PascalMarket, fixture_start_time_ms: u64) -> (u64, String) {
    (
        market
            .expected_event_start_time_ms
            .map(|start_time| start_time.abs_diff(fixture_start_time_ms))
            .unwrap_or(u64::MAX),
        market.symbol.clone(),
    )
}

fn select_outcome(
    fixture: &TxLineFixture,
    market: &PascalMarket,
    lines: &[TxLineOddsLine],
) -> Option<TxLineMarketSelection> {
    let home = normalize(&home_participant(fixture));
    let away = normalize(&away_participant(fixture));
    let participant_1 = normalize(&fixture.participant_1);
    let participant_2 = normalize(&fixture.participant_2);
    let description = normalize(&market.market_description);
    let mut best = None;

    for line in lines {
        if !market_period_matches(&line.market_period, market) {
            continue;
        }
        if !market_parameters_match(&line.market_parameters, market) {
            continue;
        }
        for (price_index, price_name) in line.price_names.iter().enumerate() {
            let score = outcome_score(
                &normalize(price_name),
                &description,
                &home,
                &away,
                &participant_1,
                &participant_2,
            );
            if score == 0 {
                continue;
            }
            let selection = TxLineMarketSelection {
                super_odds_type: line.super_odds_type.clone(),
                market_parameters: line.market_parameters.clone(),
                market_period: line.market_period.clone(),
                price_index,
                price_name: price_name.clone(),
            };
            if best
                .as_ref()
                .is_none_or(|(best_score, _)| score > *best_score)
            {
                best = Some((score, selection));
            }
        }
    }
    best.map(|(_, selection)| selection)
}

fn market_period_matches(txline_period: &str, market: &PascalMarket) -> bool {
    let txline_period = normalize(txline_period);
    let pascal_period = normalize(&format!(
        "{} {}",
        market.market_period, market.market_description
    ));

    if txline_period.is_empty() {
        return !["half", "quarter", "period", "inning", "set"]
            .iter()
            .any(|segment| pascal_period.contains(segment));
    }

    if txline_period.contains("fulltime") || txline_period.contains("regtime") {
        return pascal_period.contains("fulltime") || pascal_period.contains("regtime");
    }

    if txline_period.contains("half1") {
        return pascal_period.contains("firsthalf")
            || pascal_period.contains("1sthalf")
            || pascal_period.contains("half1");
    }
    if txline_period.contains("half2") {
        return pascal_period.contains("secondhalf")
            || pascal_period.contains("2ndhalf")
            || pascal_period.contains("half2");
    }

    pascal_period.contains(&txline_period)
}

fn market_parameters_match(txline_parameters: &str, market: &PascalMarket) -> bool {
    let parameters = txline_parameters.trim();
    if parameters.is_empty() {
        return true;
    }
    let Some(txline_line) = parameter_line(parameters) else {
        return false;
    };
    extract_numbers(&market.market_description)
        .into_iter()
        .any(|pascal_line| pascal_line == txline_line)
}

fn parameter_line(parameters: &str) -> Option<String> {
    parameters.split([',', ';', '&']).find_map(|parameter| {
        let (key, value) = parameter.split_once('=')?;
        (key.trim().eq_ignore_ascii_case("line")).then(|| canonical_number(value.trim()))?
    })
}

fn extract_numbers(value: &str) -> Vec<String> {
    let mut numbers = Vec::new();
    let mut start = None;
    for (index, character) in value.char_indices() {
        if character.is_ascii_digit() || (character == '-' && start.is_none()) || character == '.' {
            start.get_or_insert(index);
        } else if let Some(start_index) = start.take()
            && !character.is_ascii_alphabetic()
            && let Some(number) = canonical_number(&value[start_index..index])
        {
            numbers.push(number);
        }
    }
    if let Some(start_index) = start
        && let Some(number) = canonical_number(&value[start_index..])
    {
        numbers.push(number);
    }
    numbers
}

fn canonical_number(value: &str) -> Option<String> {
    let value = value.trim().strip_prefix('+').unwrap_or(value.trim());
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let (integer, fraction) = value.split_once('.').unwrap_or((value, ""));
    if integer.is_empty() || !integer.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    if !fraction.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let fraction = fraction.trim_end_matches('0');
    let normalized = if fraction.is_empty() {
        integer.to_owned()
    } else {
        format!("{integer}.{fraction}")
    };
    Some(if negative && normalized != "0" {
        format!("-{normalized}")
    } else {
        normalized
    })
}

fn outcome_score(
    price_name: &str,
    market_description: &str,
    home: &str,
    away: &str,
    participant_1: &str,
    participant_2: &str,
) -> u8 {
    if market_description.contains(price_name) && price_name.len() >= 3 {
        return 100;
    }
    let home_price = matches!(price_name, "home" | "1") || price_name.contains(home);
    if home_price && market_description.contains(home) {
        return 90;
    }
    let away_price = matches!(price_name, "away" | "2") || price_name.contains(away);
    if away_price && market_description.contains(away) {
        return 90;
    }
    let participant_1_price =
        matches!(price_name, "part1" | "participant1") || price_name.contains(participant_1);
    if participant_1_price && market_description.contains(participant_1) {
        return 90;
    }
    let participant_2_price =
        matches!(price_name, "part2" | "participant2") || price_name.contains(participant_2);
    if participant_2_price && market_description.contains(participant_2) {
        return 90;
    }
    if matches!(price_name, "draw" | "tie" | "x")
        && (market_description.contains("draw") || market_description.contains("tie"))
    {
        return 80;
    }
    0
}

fn home_participant(fixture: &TxLineFixture) -> String {
    if fixture.participant_1_is_home {
        fixture.participant_1.clone()
    } else {
        fixture.participant_2.clone()
    }
}

fn away_participant(fixture: &TxLineFixture) -> String {
    if fixture.participant_1_is_home {
        fixture.participant_2.clone()
    } else {
        fixture.participant_1.clone()
    }
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .filter_map(|character| {
            character
                .is_ascii_alphanumeric()
                .then_some(character.to_ascii_lowercase())
        })
        .collect()
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn required_env(key: &str) -> Result<String> {
    optional_env(key).with_context(|| format!("{key} is required for live feeds"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> TxLineFixture {
        TxLineFixture {
            fixture_id: 42,
            participant_1: "France".into(),
            participant_2: "Spain".into(),
            participant_1_is_home: true,
            start_time_ms: 1_000_000,
            game_state: Some(1),
        }
    }

    #[test]
    fn matches_a_pascal_event_using_both_participants_and_time() {
        let market = PascalMarket {
            symbol: "FRAESP.FRANCE".into(),
            event_description: "France vs Spain".into(),
            market_description: "France win".into(),
            market_period: "Moneyline - Reg Time".into(),
            expected_event_start_time_ms: Some(1_100_000),
            tags: vec!["Sports".into()],
        };
        assert!(market_matches_fixture(
            &market, "france", "spain", 1_000_000
        ));
    }

    #[test]
    fn selects_the_txline_home_outcome_for_the_matching_pascal_market() {
        let market = PascalMarket {
            symbol: "FRAESP.FRANCE".into(),
            event_description: "France vs Spain".into(),
            market_description: "France win".into(),
            market_period: "Moneyline - Reg Time".into(),
            expected_event_start_time_ms: Some(1_000_000),
            tags: vec!["Sports".into()],
        };
        let lines = vec![TxLineOddsLine {
            super_odds_type: "match_result".into(),
            market_parameters: String::new(),
            market_period: "full_time".into(),
            price_names: vec!["Home".into(), "Draw".into(), "Away".into()],
        }];
        let selection = select_outcome(&fixture(), &market, &lines).unwrap();
        assert_eq!(selection.price_index, 0);
        assert_eq!(selection.price_name, "Home");
    }

    #[test]
    fn pascal_symbol_override_skips_event_name_matching() {
        let market = PascalMarket {
            symbol: "MANUAL.MARKET".into(),
            event_description: "Different event".into(),
            market_description: "France win".into(),
            market_period: "Moneyline - Reg Time".into(),
            expected_event_start_time_ms: None,
            tags: vec![],
        };
        let matches = matching_markets(&fixture(), &[market], Some("MANUAL.MARKET"));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn refuses_to_map_a_first_half_txline_price_to_a_regulation_time_market() {
        let market = PascalMarket {
            symbol: "FRAESP.DRAW".into(),
            event_description: "France vs Spain".into(),
            market_description: "Draw".into(),
            market_period: "Moneyline - Reg Time".into(),
            expected_event_start_time_ms: Some(1_000_000),
            tags: vec![],
        };
        assert!(!market_period_matches("half=1", &market));
        assert!(market_period_matches("", &market));
    }

    #[test]
    fn requires_total_line_parameters_to_match_the_pascal_market() {
        let market = PascalMarket {
            symbol: "FRAESP.TOTAL".into(),
            event_description: "France vs Spain".into(),
            market_description: "Total Over 1.5".into(),
            market_period: "Totals - Reg Time".into(),
            expected_event_start_time_ms: Some(1_000_000),
            tags: vec![],
        };
        assert!(market_parameters_match("line=1.50", &market));
        assert!(!market_parameters_match("line=3", &market));
        assert!(!market_parameters_match("other=value", &market));
        assert_eq!(extract_numbers("1st half total 1.5"), vec!["1.5"]);
    }

    #[test]
    fn resolves_participant_labels_using_the_fixture_participants() {
        let market = PascalMarket {
            symbol: "FRAESP.HOME".into(),
            event_description: "France vs Spain".into(),
            market_description: "France to win".into(),
            market_period: "Moneyline - Reg Time".into(),
            expected_event_start_time_ms: Some(1_000_000),
            tags: vec![],
        };
        let lines = vec![TxLineOddsLine {
            super_odds_type: "1X2_PARTICIPANT_RESULT".into(),
            market_parameters: String::new(),
            market_period: "reg_time".into(),
            price_names: vec!["part1".into(), "draw".into(), "part2".into()],
        }];
        let selection = select_outcome(&fixture(), &market, &lines).unwrap();
        assert_eq!(selection.price_name, "part1");
    }
}

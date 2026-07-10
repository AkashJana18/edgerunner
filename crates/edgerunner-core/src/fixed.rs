use std::{fmt, ops};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SCALE: i64 = 1_000_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PriceError {
    #[error("price is outside [0, 1]")]
    OutOfRange,
    #[error("invalid decimal price")]
    Invalid,
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct Price(i64);

impl Price {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(SCALE);

    pub fn from_micros(value: i64) -> Result<Self, PriceError> {
        if !(0..=SCALE).contains(&value) {
            return Err(PriceError::OutOfRange);
        }
        Ok(Self(value))
    }

    pub fn from_decimal(value: &str) -> Result<Self, PriceError> {
        let value = value.trim();
        let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
        let whole: i64 = whole.parse().map_err(|_| PriceError::Invalid)?;
        if whole < 0 || fractional.len() > 6 || !fractional.bytes().all(|b| b.is_ascii_digit()) {
            return Err(PriceError::Invalid);
        }
        let mut padded = fractional.to_owned();
        while padded.len() < 6 {
            padded.push('0');
        }
        let fraction = if padded.is_empty() {
            0
        } else {
            padded.parse::<i64>().map_err(|_| PriceError::Invalid)?
        };
        Self::from_micros(whole.saturating_mul(SCALE).saturating_add(fraction))
    }

    pub const fn micros(self) -> i64 {
        self.0
    }

    pub fn abs_diff(self, other: Self) -> i64 {
        self.0.abs_diff(other.0) as i64
    }

    pub fn complement(self) -> Self {
        Self(SCALE - self.0)
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:06}", self.0 / SCALE, self.0 % SCALE)
    }
}

impl ops::Sub for Price {
    type Output = i64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fixed_prices_without_float_rounding() {
        assert_eq!(Price::from_decimal("0.53").unwrap().micros(), 530_000);
        assert_eq!(Price::from_decimal("1").unwrap(), Price::ONE);
        assert_eq!(Price::from_decimal("1.1"), Err(PriceError::OutOfRange));
        assert_eq!(Price::from_decimal("0.1234567"), Err(PriceError::Invalid));
    }
}

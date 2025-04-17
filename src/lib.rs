use std::fmt;
use std::str::FromStr;
use valuable::Valuable;

pub mod consts;
pub mod failover_manager;
pub mod google;

#[derive(Debug, Clone, Valuable)]
pub enum Environment {
    Staging,
    Production,
    Test,
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Staging => write!(f, "staging"),
            Self::Production => write!(f, "production"),
            Self::Test => write!(f, "test"),
        }
    }
}

impl FromStr for Environment {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "staging" => Ok(Self::Staging),
            "production" => Ok(Self::Production),
            "test" => Ok(Self::Test),
            _ => Err(format!("Invalid environment: {}", s)),
        }
    }
}

#[cfg(test)]
mod test_utils;

use rand::Rng;
use std::time::Duration;

/// TODO(arjan-bal): Move this
#[derive(Clone)]
pub struct BackoffConfig {
    /// The amount of time to backoff after the first failure.
    pub base_delay: Duration,

    /// The factor with which to multiply backoffs after a
    /// failed retry. Should ideally be greater than 1.
    pub multiplier: f64,

    /// The factor with which backoffs are randomized.
    pub jitter: f64,

    /// The upper bound of backoff delay.
    pub max_delay: Duration,
}

pub struct ExponentialBackoff {
    config: BackoffConfig,

    /// The delay for the next retry, without the random jitter. Store as f64
    /// to avoid rounding errors.
    next_delay_secs: f64,
}

/// This is a backoff configuration with the default values specified
/// at https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md.
///
/// This should be useful for callers who want to configure backoff with
/// non-default values only for a subset of the options.
pub const DEFAULT_EXPONENTIAL_CONFIG: BackoffConfig = BackoffConfig {
    base_delay: Duration::from_secs(1),
    multiplier: 1.6,
    jitter: 0.2,
    max_delay: Duration::from_secs(120),
};

impl ExponentialBackoff {
    pub fn new(mut config: BackoffConfig) -> Self {
        // Adjust params to get them in valid ranges.
        // 0 <= base_dealy <= max_delay
        config.base_delay = config.base_delay.min(config.max_delay);
        // 1 <= multiplier
        config.multiplier = config.multiplier.max(1.0);
        // 0 <= jitter <= 1
        config.jitter = config.jitter.max(0.0);
        config.jitter = config.jitter.min(1.0);
        let next_delay_secs = config.base_delay.as_secs_f64();
        ExponentialBackoff {
            config,
            next_delay_secs,
        }
    }

    pub fn reset(&mut self) {
        self.next_delay_secs = self.config.base_delay.as_secs_f64();
    }

    pub fn backoff_duration(&mut self) -> Duration {
        let ret = self.next_delay_secs
            * (1.0 + self.config.jitter * rand::thread_rng().gen_range(-1.0..1.0));
        self.next_delay_secs = self
            .config
            .max_delay
            .as_secs_f64()
            .min(self.next_delay_secs * self.config.multiplier);
        Duration::from_secs_f64(ret)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::client::name_resolution::backoff::{BackoffConfig, ExponentialBackoff};

    // Epsilon for floating point comparisons if needed, though Duration
    // comparisons are often better.
    const EPSILON: f64 = 1e-9;

    #[test]
    fn base_less_than_max() {
        let config = BackoffConfig {
            base_delay: Duration::from_secs(10),
            multiplier: 123.0,
            jitter: 0.0,
            max_delay: Duration::from_secs(100),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(10));
    }

    #[test]
    fn base_more_than_max() {
        let config = BackoffConfig {
            multiplier: 123.0,
            jitter: 0.0,
            base_delay: Duration::from_secs(100),
            max_delay: Duration::from_secs(10),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(10));
    }

    #[test]
    fn negative_multiplier() {
        let config = BackoffConfig {
            multiplier: -123.0,
            jitter: 0.0,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(100),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        // multiplier gets clipped to 1.
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(10));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(10));
    }

    #[test]
    fn negative_jitter() {
        let config = BackoffConfig {
            multiplier: 1.0,
            jitter: -10.0,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(100),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        // jitter gets clipped to 0.
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(10));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(10));
    }

    #[test]
    fn jitter_greater_than_one() {
        let config = BackoffConfig {
            multiplier: 1.0,
            jitter: 2.0,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(100),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        // jitter gets clipped to 1.
        // 0 <= duration <= 20.
        let duration = backoff.backoff_duration();
        assert_eq!(duration.lt(&Duration::from_secs(20)), true);
        assert_eq!(duration.gt(&Duration::from_secs(0)), true);

        let duration = backoff.backoff_duration();
        assert_eq!(duration.lt(&Duration::from_secs(20)), true);
        assert_eq!(duration.gt(&Duration::from_secs(0)), true);
    }

    #[test]
    fn backoff_reset_no_jitter() {
        let config = BackoffConfig {
            multiplier: 2.0,
            jitter: 0.0,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(15),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(1));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(2));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(4));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(8));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(15));
        // Duration is capped to max_delay.
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(15));

        // reset and repeat.
        backoff.reset();
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(1));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(2));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(4));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(8));
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(15));
        // Duration is capped to max_delay.
        assert_eq!(backoff.backoff_duration(), Duration::from_secs(15));
    }

    #[test]
    fn backoff_with_jitter() {
        let config = BackoffConfig {
            multiplier: 2.0,
            jitter: 0.2,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(15),
        };
        let mut backoff = ExponentialBackoff::new(config.clone());
        // 0.8 <= duration <= 1.2.
        let duration = backoff.backoff_duration();
        assert_eq!(duration.gt(&Duration::from_secs_f64(0.8 - EPSILON)), true);
        assert_eq!(duration.lt(&Duration::from_secs_f64(1.2 + EPSILON)), true);
        // 1.6 <= duration <= 2.4.
        let duration = backoff.backoff_duration();
        assert_eq!(duration.gt(&Duration::from_secs_f64(1.6 - EPSILON)), true);
        assert_eq!(duration.lt(&Duration::from_secs_f64(2.4 + EPSILON)), true);
        // 3.2 <= duration <= 4.8.
        let duration = backoff.backoff_duration();
        assert_eq!(duration.gt(&Duration::from_secs_f64(3.2 - EPSILON)), true);
        assert_eq!(duration.lt(&Duration::from_secs_f64(4.8 + EPSILON)), true);
    }
}

//! Token bucket rate limiter for per-client message throttling.
//!
//! Implements the token bucket algorithm using lock-free atomics.
//! Each client gets their own bucket, allowing bursts up to the
//! configured capacity while enforcing a long-term rate limit.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Thread-safe token bucket rate limiter.
///
/// Tokens are consumed on each request and refill over time.
/// Uses lock-free CAS operations for high concurrency.
///
/// # Example
///
/// ```
/// use lynx_server::rate_limiter::TokenBucket;
///
/// // 10 requests/sec, burst of 5
/// let limiter = TokenBucket::new(10.0, 5);
///
/// // first 5 requests succeed (burst)
/// for _ in 0..5 {
///     assert!(limiter.try_acquire());
/// }
///
/// // 6th request is rate limited
/// assert!(!limiter.try_acquire());
/// ```
pub struct TokenBucket {
    tokens: AtomicU32,
    last_refill_ms: AtomicU64,
    rate_per_second: f64,
    burst: u32,
}

impl TokenBucket {
    /// Creates a new token bucket.
    ///
    /// # Arguments
    ///
    /// * `rate_per_second` - Token refill rate
    /// * `burst` - Maximum tokens (initial and cap)
    pub fn new(rate_per_second: f64, burst: usize) -> Self {
        Self {
            tokens: AtomicU32::new(burst as u32),
            last_refill_ms: AtomicU64::new(Self::now_ms()),
            rate_per_second,
            burst: burst as u32,
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis() as u64
    }

    /// Attempts to acquire a token.
    ///
    /// Automatically refills tokens based on elapsed time before checking.
    ///
    /// # Returns
    ///
    /// - `true` - token acquired, request allowed
    /// - `false` - no tokens available, request should be rejected
    pub fn try_acquire(&self) -> bool {
        self.refill();

        // CAS loop to atomically decrement if tokens > 0
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(current, current - 1, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
            // CAS failed, retry
        }
    }

    fn refill(&self) {
        let now = Self::now_ms();
        let last = self.last_refill_ms.load(Ordering::Acquire);
        let elapsed_ms = now.saturating_sub(last);

        if elapsed_ms == 0 {
            return;
        }

        let refill = ((elapsed_ms as f64 / 1000.0) * self.rate_per_second) as u32;
        if refill == 0 {
            return;
        }

        // try to update last_refill_ms - only one thread wins
        if self
            .last_refill_ms
            .compare_exchange(last, now, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            // we won the race, add tokens (capped at burst)
            loop {
                let current = self.tokens.load(Ordering::Acquire);
                let new_val = current.saturating_add(refill).min(self.burst);
                if self
                    .tokens
                    .compare_exchange_weak(current, new_val, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_acquire_under_burst() {
        let bucket = TokenBucket::new(10.0, 5);
        // should allow up to burst (5) tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire(), "should allow within burst");
        }
    }

    #[test]
    fn test_acquire_depleted() {
        let bucket = TokenBucket::new(10.0, 3);
        // deplete tokens
        for _ in 0..3 {
            assert!(bucket.try_acquire());
        }
        // next should fail
        assert!(
            !bucket.try_acquire(),
            "should be rate limited when depleted"
        );
    }

    #[test]
    fn test_refill_over_time() {
        let bucket = TokenBucket::new(10.0, 5);
        // deplete tokens
        for _ in 0..5 {
            bucket.try_acquire();
        }
        assert!(!bucket.try_acquire(), "should be depleted");

        // wait for refill (100ms = 1 token at 10/sec)
        thread::sleep(Duration::from_millis(150));

        // should have at least 1 token now
        assert!(bucket.try_acquire(), "should have refilled");
    }

    #[test]
    fn test_burst_cap() {
        let bucket = TokenBucket::new(100.0, 5);
        // wait for lots of refill time
        thread::sleep(Duration::from_millis(100));

        // should still only have burst (5) tokens max
        let mut count = 0;
        while bucket.try_acquire() {
            count += 1;
            if count > 10 {
                panic!("exceeded burst cap");
            }
        }
        assert!(count <= 5, "should be capped at burst");
    }
}

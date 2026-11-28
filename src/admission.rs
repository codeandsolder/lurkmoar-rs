use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::Error;

/// The reservation survives queueing, compression and the RAM outage queue.
/// Delivery, durable spill or an explicitly recorded drop releases it.
#[derive(Debug)]
pub struct Permit {
    _payload: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub struct Admission {
    payloads: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_payloads: usize,
    max_bytes: usize,
}

impl Admission {
    pub fn new(max_payloads: usize, max_bytes: usize) -> Result<Self, Error> {
        if max_payloads == 0 || max_payloads >= Semaphore::MAX_PERMITS {
            return Err(Error::InvalidConfig(
                "invalid RAM backlog payload limit".into(),
            ));
        }
        if max_bytes == 0 || max_bytes > Semaphore::MAX_PERMITS || u32::try_from(max_bytes).is_err()
        {
            return Err(Error::InvalidConfig(
                "invalid RAM backlog byte limit".into(),
            ));
        }
        Ok(Self {
            payloads: Arc::new(Semaphore::new(max_payloads)),
            bytes: Arc::new(Semaphore::new(max_bytes)),
            max_payloads,
            max_bytes,
        })
    }

    pub fn reserve(&self, bytes: usize) -> Result<Permit, Error> {
        if bytes > self.max_bytes {
            return Err(Error::PayloadTooLarge {
                bytes,
                limit: self.max_bytes,
            });
        }
        let count = u32::try_from(bytes).map_err(|_| Error::PayloadTooLarge {
            bytes,
            limit: self.max_bytes,
        })?;
        let payload = Arc::clone(&self.payloads)
            .try_acquire_owned()
            .map_err(|_| Error::Backpressure)?;
        let bytes = Arc::clone(&self.bytes)
            .try_acquire_many_owned(count)
            .map_err(|_| Error::Backpressure)?;
        Ok(Permit {
            _payload: payload,
            _bytes: bytes,
        })
    }

    pub fn outstanding(&self) -> (usize, usize) {
        (
            self.max_payloads - self.payloads.available_permits(),
            self.max_bytes - self.bytes.available_permits(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Admission;
    use crate::Error;

    #[test]
    fn count_and_byte_reservations_release_independently_on_drop() -> Result<(), Error> {
        let admission = Admission::new(2, 100)?;
        let first = admission.reserve(60)?;
        assert!(matches!(admission.reserve(41), Err(Error::Backpressure)));
        assert!(matches!(
            admission.reserve(101),
            Err(Error::PayloadTooLarge { .. })
        ));
        assert_eq!(admission.outstanding(), (1, 60));
        let second = admission.reserve(40)?;
        assert!(matches!(admission.reserve(0), Err(Error::Backpressure)));
        drop(first);
        assert_eq!(admission.outstanding(), (1, 40));
        drop(second);
        assert_eq!(admission.outstanding(), (0, 0));
        Ok(())
    }
}

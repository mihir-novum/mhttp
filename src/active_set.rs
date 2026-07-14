use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

pub struct ActiveSet<Id, Info> {
    inner: Arc<Inner<Id, Info>>,
}

struct Inner<Id, Info> {
    count: AtomicUsize,
    empty_notify: Notify,
    // None when telemetry is disabled.
    // The hot path (insert + drop) never touches this pointer if telemetry is off.
    map: Option<Mutex<HashMap<Id, Info>>>,
}

/// The public guard that can be freely and safely cloned.
pub struct ActiveGuard<Id, Info>
where
    Id: Eq + Hash,
{
    // The inner Arc ensures that no matter how many times this guard is cloned,
    // the actual Drop logic only executes exactly ONCE when the last clone drops.
    drop_guard: Arc<GuardDrop<Id, Info>>,
}

/// A private helper struct that strictly manages the Drop lifecycle.
struct GuardDrop<Id, Info>
where
    Id: Eq + Hash,
{
    inner: Arc<Inner<Id, Info>>,
    id: Id,
}

#[allow(dead_code)]
impl<Id, Info> ActiveSet<Id, Info>
where
    Id: Eq + Hash + Clone,
{
    /// Hot path: counter only. Zero map allocation, zero lock.
    pub fn new() -> Self {
        Self::internal_new(false)
    }

    /// Use this when on_request_complete is Some.
    /// Allocates the map; snapshot/update/value become available.
    pub fn new_with_telemetry(with_telemetry: bool) -> Self {
        Self::internal_new(with_telemetry)
    }

    fn internal_new(with_map: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                count: AtomicUsize::new(0),
                empty_notify: Notify::new(),
                map: if with_map {
                    Some(Mutex::new(HashMap::new()))
                } else {
                    None
                },
            }),
        }
    }

    pub fn insert(&self, id: Id, info: Info) -> ActiveGuard<Id, Info> {
        // ALWAYS increment the graceful shutdown counter unconditionally
        self.inner.count.fetch_add(1, Ordering::Relaxed);

        // OPTIONALLY insert into telemetry map (only taking the lock if map exists)
        if let Some(map) = &self.inner.map {
            map.lock().unwrap().insert(id.clone(), info);
        }

        ActiveGuard {
            drop_guard: Arc::new(GuardDrop {
                inner: self.inner.clone(),
                id,
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, id: &Id) -> bool {
        self.inner
            .map
            .as_ref()
            .map(|m| m.lock().unwrap().contains_key(id))
            .unwrap_or(false)
    }

    pub fn get(&self, id: &Id) -> Option<Info>
    where
        Info: Clone,
    {
        self.inner.map.as_ref()?.lock().unwrap().get(id).cloned()
    }

    pub fn snapshot(&self) -> HashMap<Id, Info>
    where
        Id: Clone,
        Info: Clone,
    {
        self.inner
            .map
            .as_ref()
            .map(|m| m.lock().unwrap().clone())
            .unwrap_or_default()
    }

    pub async fn wait_for_zero(&self) {
        loop {
            // Register the listener BEFORE loading the counter.
            // Closes the race window perfectly.
            let notified = self.inner.empty_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // Acquire pairs with Release in Drop — when we see 0, all prior
            // map removals and Info mutations are visible to this thread.
            if self.inner.count.load(Ordering::Acquire) == 0 {
                return;
            }

            notified.await;
        }
    }
}

#[allow(dead_code)]
impl<Id, Info> ActiveGuard<Id, Info>
where
    Id: Eq + Hash,
    Info: Clone,
{
    pub fn id(&self) -> &Id {
        &self.drop_guard.id
    }

    pub fn update(&self, f: impl FnOnce(&mut Info)) {
        if let Some(map) = &self.drop_guard.inner.map {
            if let Some(info) = map.lock().unwrap().get_mut(&self.drop_guard.id) {
                f(info);
            }
        }
    }

    pub fn value(&self) -> Option<Info> {
        self.drop_guard
            .inner
            .map
            .as_ref()?
            .lock()
            .unwrap()
            .get(&self.drop_guard.id)
            .cloned()
    }
}

// ✨ Safe Cloning: Only increments the Arc counter. `Id` no longer needs to be cloned!
impl<Id, Info> Clone for ActiveGuard<Id, Info>
where
    Id: Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            drop_guard: self.drop_guard.clone(),
        }
    }
}

// ✨ Exactly-Once Drop Logic
impl<Id, Info> Drop for GuardDrop<Id, Info>
where
    Id: Eq + Hash,
{
    fn drop(&mut self) {
        // If telemetry is active, remove it from the map
        if let Some(map) = &self.inner.map {
            map.lock().unwrap().remove(&self.id);
        }

        // ALWAYS decrement the counter exactly once per insert
        // Release: guarantees map.remove() and any update() writes are
        // visible to the Acquire load in wait_for_zero when count hits 0.
        let prev = self.inner.count.fetch_sub(1, Ordering::Release);

        debug_assert!(prev > 0, "count underflow — double-drop of ActiveGuard");

        if prev == 1 {
            self.inner.empty_notify.notify_waiters();
        }
    }
}

// ── Manual Send/Sync Bounds (Optional but kept for compatibility) ───────────

unsafe impl<Id, Info> Send for ActiveSet<Id, Info>
where
    Id: Send + Eq + Hash + Clone,
    Info: Send,
{
}

unsafe impl<Id, Info> Sync for ActiveSet<Id, Info>
where
    Id: Send + Sync + Eq + Hash + Clone,
    Info: Send + Sync,
{
}
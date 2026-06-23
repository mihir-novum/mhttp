use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Condvar, Mutex};

pub struct ActiveSet<Id, Info> {
    inner: Arc<Inner<Id, Info>>,
}

struct Inner<Id, Info> {
    map: Mutex<HashMap<Id, Info>>,
    count: AtomicUsize,
    empty_cv: Condvar,
}

pub struct ActiveGuard<Id, Info>
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
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                map: Mutex::new(HashMap::new()),
                count: AtomicUsize::new(0),
                empty_cv: Condvar::new(),
            }),
        }
    }

    pub fn insert(&self, id: Id, info: Info) -> ActiveGuard<Id, Info> {
        let mut map = self.inner.map.lock().unwrap();

        let is_new = !map.contains_key(&id);

        map.insert(id.clone(), info);

        if is_new {
            self.inner
                .count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        ActiveGuard {
            inner: self.inner.clone(),
            id,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.count.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, id: &Id) -> bool {
        self.inner.map.lock().unwrap().contains_key(id)
    }

    pub fn get(&self, id: &Id) -> Option<Info>
    where
        Info: Clone,
    {
        self.inner.map.lock().unwrap().get(id).cloned()
    }

    pub fn snapshot(&self) -> HashMap<Id, Info>
    where
        Id: Clone,
        Info: Clone,
    {
        self.inner.map.lock().unwrap().clone()
    }

    pub fn wait_for_zero(&self) {
        let mut guard = self.inner.map.lock().unwrap();

        loop {
            if self.inner.count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                return;
            }

            guard = self.inner.empty_cv.wait(guard).unwrap();
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
        &self.id
    }

    pub fn update(&self, f: impl FnOnce(&mut Info)) {
        let mut map = self.inner.map.lock().unwrap();
        let info = map.get_mut(&self.id).unwrap();
        f(info);
    }

    pub fn value(&self) -> Option<Info> {
        self.inner.map.lock().unwrap().get(&self.id).cloned()
    }
}

impl<Id, Info> Clone for ActiveGuard<Id, Info>
where
    Id: Clone + Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            id: self.id.clone(),
        }
    }
}

impl<Id, Info> Drop for ActiveGuard<Id, Info>
where
    Id: Eq + Hash,
{
    fn drop(&mut self) {
        let mut map = self.inner.map.lock().unwrap();
        let removed = map.remove(&self.id);

        if removed.is_some() {
            let prev = self
                .inner
                .count
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

            debug_assert!(prev > 0);

            if prev == 1 {
                self.inner.empty_cv.notify_all();
            }
        }
    }
}

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

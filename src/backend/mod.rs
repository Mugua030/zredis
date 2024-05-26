use crate::{RespFrame, SimpleString};
use dashmap::DashMap;
use dashmap::DashSet;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hmap: DashMap<String, DashMap<String, RespFrame>>,
    pub(crate) dset: DashMap<String, DashSet<RespFrame>>,
}

impl Deref for Backend {
    type Target = BackendInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self(Arc::new(BackendInner::default()))
    }
}

impl Default for BackendInner {
    fn default() -> Self {
        Self {
            map: DashMap::new(),
            hmap: DashMap::new(),
            dset: DashMap::new(),
        }
    }
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<RespFrame> {
        //self.map.get(key).map(|v| v.value().clone())
        self.map.get(key).map(|v| v.value().clone())
    }

    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hmap
            .get(key)
            .and_then(|v| v.get(field).map(|v| v.value().clone()))
    }

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let hmap = self.hmap.entry(key).or_default();
        hmap.insert(field, value);
    }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hmap.get(key).map(|v| v.clone())
    }

    pub fn hmget(&self, key: &str, fields: Vec<String>) -> Option<Vec<RespFrame>> {
        //self.hmap.get(key).filter(|x| fields.contains(x));
        self.hmap.get(key).map(|smap| {
            fields
                .iter()
                .filter_map(|field| smap.get(field).map(|v| v.value().clone()))
                .collect()
        })
    }

    pub fn echo(&self, key: &str) -> Option<RespFrame> {
        Some(RespFrame::SimpleString(SimpleString::new(key.to_string())))
    }

    pub fn sadd(&self, key: String, memb: RespFrame) -> Option<u8> {
        //self.dset.get(key).and(optb)
        let set: DashSet<RespFrame> = DashSet::new();
        set.insert(memb);
        if self.dset.insert(key, set).is_some() {
            Some(1)
        } else {
            Some(0)
        }
    }

    pub fn sismember(&self, key: String, item: RespFrame) -> Option<u8> {
        match self.dset.get(&key) {
            Some(vset) => {
                if vset.contains(&item) {
                    Some(1)
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

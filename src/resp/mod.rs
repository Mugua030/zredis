mod decode;
mod encode;

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use thiserror::Error;

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}
pub trait RespDecode: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Invalid frame length: {0}")]
    InvalidFrameLength(isize),
    #[error("Frame is not complete")]
    NotComplete,
    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("Parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
}

#[enum_dispatch(RespEncode)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
    Null(RespNull),

    Boolean(bool),
    Double(Nf64),
    Map(RespMap),
    Set(RespSet),
}

// for set
#[derive(Debug, Clone, Copy)]
pub struct Nf64(f64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimpleString(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimpleError(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BulkString(pub(crate) Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RespNull;

// argument extra need access the value inner
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RespArray(pub(crate) Vec<RespFrame>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RespMap(BTreeMap<String, RespFrame>);

#[derive(Debug, Clone)]
pub struct RespSet(Vec<RespFrame>);

impl Deref for SimpleString {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for SimpleError {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for Nf64 {
    type Target = f64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// new type instance
impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleString(s.into())
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(s.into())
    }
}

impl RespMap {
    pub fn new() -> Self {
        RespMap(BTreeMap::new())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        RespMap::new()
    }
}

impl RespSet {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespSet(s.into())
    }
}

impl Nf64 {
    pub fn new(f: impl Into<f64>) -> Self {
        Nf64(f.into())
    }
}

// from
impl From<&str> for SimpleString {
    fn from(value: &str) -> Self {
        SimpleString(value.to_string())
    }
}

impl From<&str> for RespFrame {
    fn from(value: &str) -> Self {
        SimpleString(value.to_string()).into()
    }
}

impl From<&str> for SimpleError {
    fn from(value: &str) -> Self {
        SimpleError(value.to_string())
    }
}

impl From<&str> for BulkString {
    fn from(value: &str) -> Self {
        BulkString(value.as_bytes().to_vec())
    }
}

// TODO:: maybe default f64 value or try_from?
impl From<&str> for Nf64 {
    fn from(value: &str) -> Self {
        match value.parse::<f64>() {
            Ok(num) => Nf64(num),
            Err(_) => Nf64(0.0),
        }
    }
}

impl From<&[u8]> for BulkString {
    fn from(value: &[u8]) -> Self {
        BulkString(value.to_vec())
    }
}

impl From<&[u8]> for RespFrame {
    fn from(value: &[u8]) -> Self {
        BulkString(value.to_vec()).into()
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(value: &[u8; N]) -> Self {
        BulkString(value.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(value: &[u8; N]) -> Self {
        BulkString(value.to_vec()).into()
    }
}

// AsRef
impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// Nf64 Eq
impl PartialEq for Nf64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 || (self.0.is_nan() && other.0.is_nan())
    }
}
impl Eq for Nf64 {}

impl PartialOrd for Nf64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        //self.0.partial_cmp(&other.0)
        Some(self.cmp(other))
    }
}
impl Ord for Nf64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Hash for Nf64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            state.write_u8(0xFF);
        } else {
            self.0.to_bits().hash(state);
        }
    }
}

// Eq : RespSet
impl PartialEq for RespSet {
    fn eq(&self, other: &Self) -> bool {
        let mut self_sorted = self.0.clone();
        let mut other_sorted = other.0.clone();
        self_sorted.sort();
        other_sorted.sort();
        self_sorted == other_sorted
    }
}
impl PartialOrd for RespSet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        //let mut self_sorted = self.0.clone();
        //let mut other_sorted = other.0.clone();
        //self_sorted.sort();
        //other_sorted.sort();
        //self_sorted.partial_cmp(&other_sorted)

        Some(self.cmp(other))
    }
}
impl Ord for RespSet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl Eq for RespSet {}
impl Hash for RespSet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut sorted_elements = self.0.clone();
        sorted_elements.sort();
        for element in sorted_elements {
            element.hash(state);
        }
    }
}

mod hmap;
mod map;

use crate::{Backend, RespArray, RespError, RespFrame, RespNull, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use thiserror::Error;

lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    RespError(#[from] RespError),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
    HMGet(HMGet),

    Echo(Echo),
    Sadd(Sadd),
    Sismember(Sismember),

    Unrecognized(Unrecognized),
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
}

#[derive(Debug)]
pub struct HMGet {
    key: String,
    fields: Vec<String>,
}

#[derive(Debug)]
pub struct Echo {
    key: String,
}

#[derive(Debug)]
pub struct Sadd {
    key: String,
    item: RespFrame,
}

#[derive(Debug)]
pub struct Sismember {
    key: String,
    item: RespFrame,
}

#[derive(Debug)]
pub struct Unrecognized;
impl CommandExecutor for Unrecognized {
    fn execute(self, _backend: &Backend) -> RespFrame {
        RESP_OK.clone()
    }
}

// for echo command
impl TryFrom<RespArray> for Echo {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["echo"], 1)?;
        //m how
        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Echo {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid argument".to_string(),
            )),
        }
    }
}

impl CommandExecutor for Echo {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.echo(self.key.as_str()) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;
    fn try_from(value: RespFrame) -> Result<Self, Self::Error> {
        match value {
            RespFrame::Array(array) => array.try_into(),
            _ => Err(CommandError::InvalidCommand(
                "Command must be an Array".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;
    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        match v.first() {
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_ref() {
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                b"hmget" => Ok(HMGet::try_from(v)?.into()),
                b"echo" => Ok(Echo::try_from(v)?.into()),
                b"sadd" => Ok(Sadd::try_from(v)?.into()),
                b"sismember" => Ok(Sismember::try_from(v)?.into()),
                _ => Ok(Unrecognized.into()),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must have a BulkString as the first argument".to_string(),
            )),
        }
    }
}

fn validate_command(
    value: &RespArray,
    names: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if value.len() != n_args + names.len() {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} argument",
            names.join(" "),
            n_args
        )));
    }

    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "Invalid command: expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn extract_args(value: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    Ok(value.0.into_iter().skip(start).collect::<Vec<RespFrame>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BulkString, RespDecode, RespEncode, RespNull};
    use anyhow::{Context, Ok, Result};
    use bytes::BytesMut;

    #[test]
    fn test_sadd() -> Result<()> {
        let mut buf = BytesMut::new();
        //buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$4\r\nskey\r\n$7\r\nsvalue1\r\n");
        buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$4\r\nskey\r\n$2\r\n12\r\n");

        let frame =
            RespArray::decode(&mut buf).with_context(|| "[sadd] decode fail".to_string())?;
        let cmd: Command = frame.try_into()?;
        println!("cmd: {:?}", &cmd);

        let bkend = Backend::new();
        let ret = cmd.execute(&bkend);

        assert_eq!(ret, RespFrame::Integer(1));
        Ok(())
    }

    fn exec_sadd_cmd(bkend: &Backend) -> Result<RespFrame> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$4\r\nskey\r\n$7\r\nsvalue1\r\n");
        //buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$4\r\nskey\r\n$2\r\n12\r\n");

        let frame =
            RespArray::decode(&mut buf).with_context(|| "[sadd] decode fail".to_string())?;
        let cmd: Command = frame.try_into()?;
        println!("cmd: {:?}", &cmd);

        //let bkend = Backend::new();
        let ret = cmd.execute(bkend);

        Ok(ret)
    }

    #[test]
    fn test_sismember() -> Result<()> {
        let bkend = Backend::new();

        let r = exec_sadd_cmd(&bkend)?;
        println!("[exec-sadd-cmd] r: {:?}", r);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$9\r\nsismember\r\n$4\r\nskey\r\n$7\r\nsvalue1\r\n");
        let frame = RespArray::decode(&mut buf).with_context(|| "[sismember] decode fail")?;
        let cmd: Command = frame.try_into()?;
        let ret = cmd.execute(&bkend);

        assert_eq!(ret, RespFrame::Integer(1));

        Ok(())
    }

    #[test]
    fn test_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame =
            RespArray::decode(&mut buf).with_context(|| "respArray decode fail".to_string())?;
        let cmd: Command = frame.try_into()?;
        let backend = Backend::new();
        let ret = cmd.execute(&backend);

        assert_eq!(ret, RespFrame::Null(RespNull));

        Ok(())
    }

    #[test]
    fn test_echo() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$4\r\necho\r\n$5\r\nmecho\r\n");

        let frame = RespArray::decode(&mut buf)
            .with_context(|| "[test_echo] respArray decode fail".to_string())?;
        let cmd: Command = frame.try_into()?;
        let backend = Backend::new();
        let ret = cmd.execute(&backend);

        println!("ret: {:?}", String::from_utf8(ret.encode()));

        Ok(())
    }

    #[test]
    fn test_hmget() -> Result<()> {
        //Set the values
        let mut bf = BytesMut::new();
        bf.extend_from_slice(
            b"*4\r\n$4\r\nhset\r\n$9\r\nhmgetkey1\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n",
        );
        let fm = RespArray::decode(&mut bf)
            .with_context(|| "[test_hmget] hset value decode fail".to_string())?;
        let cmd0: Command = fm.try_into()?;
        let bkend = Backend::new();
        let ret_v = cmd0.execute(&bkend);
        println!("ret_v: {:?}", String::from_utf8(ret_v.encode()));

        //HMGet values
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            b"*4\r\n$5\r\nhmget\r\n$9\r\nhmgetkey1\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n",
        );
        //buf.extend_from_slice(b"*4\r\n$5\r\nhmget\r\n$9\r\nhmgetkey1\r\n$6\r\nfield1\r\n");

        let frame = RespArray::decode(&mut buf)
            .with_context(|| "[test_hmget] RespArray decode fail".to_string())?;
        let cmd: Command = frame.try_into()?;
        //let backend = Backend::new();
        let ret = cmd.execute(&bkend);

        //println!("ret: {:?}", String::from_utf8(ret.encode()));

        let expected = RespFrame::Array(RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("value1")),
            RespFrame::Null(RespNull),
        ]));

        assert_ne!(ret, expected);

        Ok(())
    }
}

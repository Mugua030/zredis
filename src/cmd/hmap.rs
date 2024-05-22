use super::{extract_args, validate_command, CommandExecutor, HGet, HGetAll, HMGet, HSet, RESP_OK};
use crate::{cmd::CommandError, RespArray, RespFrame, RespMap};

impl CommandExecutor for HGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(crate::RespNull),
        }
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let hmap = backend.hmap.get(&self.key);
        match hmap {
            Some(hmap) => {
                let mut map = RespMap::new();
                for v in hmap.iter() {
                    let key = v.key().to_owned();
                    map.insert(key, v.value().clone());
                }
                map.into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

impl CommandExecutor for HMGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let mret = backend.hmget(self.key.as_str(), self.fields);
        match mret {
            Some(values) => RespArray::new(values).into(),
            None => RespArray::new([]).into(),
        }
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key, field or value".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HMGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let n_args = value.len() - 1;
        validate_command(&value, &["hmget"], n_args)?;

        let mut args = extract_args(value, 1)?.into_iter();
        // multi field
        match args.next() {
            Some(RespFrame::BulkString(key)) => {
                let fields: Result<Vec<_>, _> = args
                    .map(|x| match x {
                        RespFrame::BulkString(bs) => {
                            String::from_utf8(bs.0).map_err(CommandError::from)
                        }
                        _ => Err(CommandError::InvalidArgument("Invalid field".to_string())),
                    })
                    .collect();

                Ok(HMGet {
                    key: String::from_utf8(key.0)?,
                    fields: fields?,
                })
            }
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

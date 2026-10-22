//! Private, sequential stdio transport. Store ownership stays inside dispatch.
use std::io::{self, Read, Write};

use fastk::{FastKError, Result};
use serde::Deserialize;

use super::{dispatch, Input, JsonOutput};

const MAX_FRAME_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    id: u64,
    args: Vec<String>,
    payload: Option<serde_json::Value>,
}

fn read_frame(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut prefix = [0; 8];
    if reader.read(&mut prefix[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..])?;
    let size = u64::from_le_bytes(prefix);
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid session frame size",
        ));
    }
    let mut body = Vec::new();
    reader.take(size).read_to_end(&mut body)?;
    if body.len() as u64 != size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated session frame",
        ));
    }
    Ok(Some(body))
}

fn write_frame(writer: &mut impl Write, body: &[u8]) -> io::Result<()> {
    if body.is_empty() || body.len() as u64 > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid session response size",
        ));
    }
    writer.write_all(&(body.len() as u64).to_le_bytes())?;
    writer.write_all(body)?;
    writer.flush()
}

pub(super) fn run() -> Result<()> {
    let mut reader = io::BufReader::new(io::stdin().lock());
    let mut writer = io::BufWriter::with_capacity(64 * 1024, io::stdout().lock());
    write_frame(
        &mut writer,
        &serde_json::to_vec(&serde_json::json!({
            "session_protocol": 1, "contract_version": 1, "max_frame_bytes": MAX_FRAME_BYTES
        }))?,
    )?;
    let mut last_id = 0;
    while let Some(body) = read_frame(&mut reader)? {
        let request: Request = serde_json::from_slice(&body)?;
        if request.id <= last_id
            || request.args.is_empty()
            || matches!(
                request.args[0].as_str(),
                "--help" | "-h" | "--session-stdio"
            )
        {
            return Err(FastKError::InvalidInput("invalid session request".into()));
        }
        last_id = request.id;
        let mut response = format!("{{\"id\":{},\"ok\":true,\"result\":", request.id).into_bytes();
        // Same dispatcher and response serializer as one-shot CLI. Its local
        // stores/guards have dropped before a response is written.
        match dispatch(
            request.args,
            Input::Session(request.payload),
            &mut JsonOutput::Compact(&mut response),
        ) {
            Ok(()) => response.push(b'}'),
            Err(error) => {
                // Nothing has reached stdout. Discard any partial success
                // bytes and reuse the same allocation for the error envelope.
                response.clear();
                serde_json::to_writer(
                    &mut response,
                    &serde_json::json!({
                        "id": request.id, "ok": false,
                        "error": {"code": error.code(), "message": error.to_string()}
                    }),
                )?;
            }
        }
        write_frame(&mut writer, &response)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eof_only_between_frames_and_bad_sizes_fail_closed() {
        assert!(read_frame(&mut &b""[..]).unwrap().is_none());
        for bytes in [
            vec![1],
            vec![0; 8],
            (MAX_FRAME_BYTES + 1).to_le_bytes().to_vec(),
            [5u64.to_le_bytes().as_slice(), b"{}"].concat(),
        ] {
            assert!(read_frame(&mut bytes.as_slice()).is_err());
        }
    }

    #[test]
    fn framing_preserves_multiline_json_and_separates_requests() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, b"{\n\"value\":9007199254740993\n}").unwrap();
        write_frame(&mut bytes, b"{}").unwrap();
        let mut reader = bytes.as_slice();
        assert_eq!(
            read_frame(&mut reader).unwrap().unwrap(),
            b"{\n\"value\":9007199254740993\n}"
        );
        assert_eq!(read_frame(&mut reader).unwrap().unwrap(), b"{}");
        assert!(read_frame(&mut reader).unwrap().is_none());
    }
}

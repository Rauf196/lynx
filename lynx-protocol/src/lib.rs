use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    // client wants to register with username
    Connect { username: String },

    // client sends a message to current room
    SendRoomMessage { text: String },

    // client joins a room
    JoinRoom { room_name: String },

    // client sends private message to another client
    SendPrivateMessage { to: String, text: String },

    // request list of online users
    ListUsers,

    // client disconnects
    Disconnect,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    // generic success
    Success {
        message: String,
    },

    // something went wrong
    Error {
        message: String,
    },

    // list of online users
    UserList {
        users: Vec<String>,
    },

    // someone sent a message
    IncomingMessage {
        from: String,
        text: String,
        room: Option<String>, // None if private message
    },

    // system notification
    SystemNotification {
        text: String,
    },
}

/// Encodes a message into a length-prefixed frame.
///
/// Returns a Vec<u8> with format: [4 bytes length][message bytes]
pub fn encode_frame(msg: &Message) -> Result<Vec<u8>, String> {
    // serialize message to bytes
    let msg_bytes = bincode::serialize(msg).map_err(|e| format!("serialization failed: {}", e))?;

    // get the length (as u32) and convert to 4 bytes
    let length = msg_bytes.len() as u32;
    let length_bytes = length.to_be_bytes(); // [u8; 4]

    // combine the length bytes and the message bytes
    let mut frame = Vec::new();
    frame.extend_from_slice(&length_bytes); // add the 4 bytes of length info
    frame.extend_from_slice(&msg_bytes); // add message bytes

    Ok(frame)
}

/// Decodes a length-prefixed frame into a message.
///
/// Expects format: [4 bytes length][message bytes]
pub fn decode_frame(bytes: &[u8]) -> Result<Message, String> {
    if bytes.len() < 4 {
        return Err("not enough bytes for length".to_string());
    }

    // taking the first 4 bytes
    let length_bytes: [u8; 4] = bytes[0..4]
        .try_into()
        .map_err(|_| "failed to read length bytes".to_string())?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    // validate we have enough bytes for the full message
    if bytes.len() < 4 + length {
        return Err("incomplete message".to_string());
    }

    // cut the rest of the slice
    let msg_bytes = &bytes[4..4 + length];

    // deserialize
    let message =
        bincode::deserialize(msg_bytes).map_err(|e| format!("deserialization failed: {}", e))?;

    Ok(message)
}

/// Encodes a response into a length-prefixed frame.
///
/// Returns a Vec<u8> with format: [4 bytes length][response bytes]
pub fn encode_response(resp: &Response) -> Result<Vec<u8>, String> {
    // serialize response to bytes
    let resp_bytes =
        bincode::serialize(resp).map_err(|e| format!("serialization failed: {}", e))?;

    // get the length (as u32) and convert to 4 bytes
    let length = resp_bytes.len() as u32;
    let length_bytes = length.to_be_bytes(); // [u8; 4]

    // combine the length bytes and the response bytes
    let mut frame = Vec::new();
    frame.extend_from_slice(&length_bytes); // add the 4 bytes of length info
    frame.extend_from_slice(&resp_bytes); // add response bytes

    Ok(frame)
}

/// Decodes a length-prefixed frame into a response.
///
/// Expects format: [4 bytes length][response bytes]
pub fn decode_response(bytes: &[u8]) -> Result<Response, String> {
    if bytes.len() < 4 {
        return Err("not enough bytes for length".to_string());
    }

    // taking the first 4 bytes
    let length_bytes: [u8; 4] = bytes[0..4]
        .try_into()
        .map_err(|_| "failed to read length bytes".to_string())?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    // validate we have enough bytes for the full response
    if bytes.len() < 4 + length {
        return Err("incomplete response".to_string());
    }

    // cut the rest of the slice
    let resp_bytes = &bytes[4..4 + length];

    // deserialize
    let response =
        bincode::deserialize(resp_bytes).map_err(|e| format!("deserialization failed: {}", e))?;

    Ok(response)
}

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1 MB

/// extracts a complete message from accumulator buffer.
/// returns Ok(None) if incomplete, Ok(Some((msg, bytes_consumed))) if complete.
pub fn try_extract_frame(bytes: &[u8]) -> Result<Option<(Message, usize)>, String> {
    if bytes.len() < 4 {
        return Ok(None);
    }

    let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

    if length > MAX_MESSAGE_SIZE {
        return Err(format!("message too large: {} bytes", length));
    }

    let total_frame_size = 4 + length;
    if bytes.len() < total_frame_size {
        return Ok(None);
    }

    let message = bincode::deserialize(&bytes[4..total_frame_size])
        .map_err(|e| format!("deserialization failed: {}", e))?;

    Ok(Some((message, total_frame_size)))
}

/// same as try_extract_frame but for Response (client-side).
pub fn try_extract_response(bytes: &[u8]) -> Result<Option<(Response, usize)>, String> {
    if bytes.len() < 4 {
        return Ok(None);
    }

    let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

    if length > MAX_MESSAGE_SIZE {
        return Err(format!("response too large: {} bytes", length));
    }

    let total_frame_size = 4 + length;
    if bytes.len() < total_frame_size {
        return Ok(None);
    }

    let response = bincode::deserialize(&bytes[4..total_frame_size])
        .map_err(|e| format!("deserialization failed: {}", e))?;

    Ok(Some((response, total_frame_size)))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = Message::Connect {
            username: "Rauf".to_string(),
        };

        // encode
        let frame = encode_frame(&original).expect("encode failed");

        // decode
        let decoded = decode_frame(&frame).expect("decode failed");

        // they should match
        assert!(matches!(decoded, Message::Connect { username } if username == "Rauf"));
    }

    #[test]
    fn test_decode_insufficient_bytes() {
        let bytes = vec![0, 0]; // only 2 bytes
        let result = decode_frame(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_response_encode_decode_roundtrip() {
        let original = Response::Success {
            message: "connected".to_string(),
        };

        // encode
        let frame = encode_response(&original).expect("encode failed");

        // decode
        let decoded = decode_response(&frame).expect("decode failed");

        // they should match
        assert!(matches!(decoded, Response::Success { message } if message == "connected"));
    }

    #[test]
    fn test_decode_incomplete_message() {
        // claim to have 100 bytes but only provide 10
        let mut bytes = vec![0, 0, 0, 100]; // length = 100
        bytes.extend_from_slice(&[1, 2, 3, 4, 5, 6]); // only 6 bytes

        let result = decode_frame(&bytes);
        assert!(result.is_err());
    }

    // tests for try_extract_frame (accumulator pattern)

    #[test]
    fn test_try_extract_frame_incomplete_length() {
        // only 2 bytes, need at least 4 for length prefix
        let bytes = vec![0, 0];
        let result = try_extract_frame(&bytes);
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_try_extract_frame_incomplete_payload() {
        // length says 100 bytes, but only 6 provided
        let mut bytes = vec![0, 0, 0, 100]; // length = 100
        bytes.extend_from_slice(&[1, 2, 3, 4, 5, 6]);
        let result = try_extract_frame(&bytes);
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_try_extract_frame_complete() {
        let msg = Message::Connect {
            username: "test".to_string(),
        };
        let frame = encode_frame(&msg).unwrap();

        let result = try_extract_frame(&frame).unwrap();
        assert!(result.is_some());

        let (decoded, consumed) = result.unwrap();
        assert_eq!(consumed, frame.len());
        assert!(matches!(decoded, Message::Connect { username } if username == "test"));
    }

    #[test]
    fn test_try_extract_frame_with_extra_bytes() {
        // simulate partial second message after complete first message
        let msg = Message::ListUsers;
        let mut bytes = encode_frame(&msg).unwrap();
        let first_frame_len = bytes.len();

        // add some extra bytes (partial next message)
        bytes.extend_from_slice(&[0, 0, 0, 50, 1, 2, 3]);

        let result = try_extract_frame(&bytes).unwrap();
        assert!(result.is_some());

        let (decoded, consumed) = result.unwrap();
        assert_eq!(consumed, first_frame_len); // only consumed first message
        assert!(matches!(decoded, Message::ListUsers));
    }

    #[test]
    fn test_try_extract_frame_multiple_messages() {
        let msg1 = Message::Connect {
            username: "alice".to_string(),
        };
        let msg2 = Message::ListUsers;

        let mut bytes = encode_frame(&msg1).unwrap();
        let frame1_len = bytes.len();
        bytes.extend(encode_frame(&msg2).unwrap());

        // first extraction
        let (decoded1, consumed1) = try_extract_frame(&bytes).unwrap().unwrap();
        assert_eq!(consumed1, frame1_len);
        assert!(matches!(decoded1, Message::Connect { username } if username == "alice"));

        // second extraction from remaining bytes
        let (decoded2, _consumed2) = try_extract_frame(&bytes[consumed1..]).unwrap().unwrap();
        assert!(matches!(decoded2, Message::ListUsers));
    }

    #[test]
    fn test_try_extract_frame_rejects_huge_message() {
        // claim 2MB message (over 1MB limit)
        let bytes = vec![0, 32, 0, 0]; // 0x00200000 = 2,097,152 bytes
        let result = try_extract_frame(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too large"));
    }

    #[test]
    fn test_try_extract_response_complete() {
        let resp = Response::Success {
            message: "ok".to_string(),
        };
        let frame = encode_response(&resp).unwrap();

        let result = try_extract_response(&frame).unwrap();
        assert!(result.is_some());

        let (decoded, consumed) = result.unwrap();
        assert_eq!(consumed, frame.len());
        assert!(matches!(decoded, Response::Success { message } if message == "ok"));
    }

    #[test]
    fn test_try_extract_response_incomplete() {
        let bytes = vec![0, 0, 0, 50]; // claims 50 bytes, has 0
        let result = try_extract_response(&bytes);
        assert!(matches!(result, Ok(None)));
    }
}

//! Binary protocol for the Lynx chat server.
//!
//! This crate defines the wire protocol used between clients and the server.
//! Messages are length-prefixed binary frames using bincode serialization.
//!
//! # Frame Format
//!
//! ```text
//! +----------------+------------------+
//! | length (4 bytes, big-endian u32) |
//! +----------------+------------------+
//! | payload (bincode-serialized)     |
//! +----------------------------------+
//! ```
//!
//! # Usage
//!
//! ```
//! use lynx_protocol::{Message, Response, encode_frame, try_extract_frame};
//!
//! // encode a message
//! let msg = Message::Connect { username: "alice".into() };
//! let frame = encode_frame(&msg).unwrap();
//!
//! // decode using accumulator pattern (handles partial reads)
//! if let Ok(Some((message, consumed))) = try_extract_frame(&frame) {
//!     // process message, drain consumed bytes from buffer
//! }
//! ```

use serde::{Deserialize, Serialize};

/// Messages sent from client to server.
///
/// Each variant represents a distinct action a client can perform.
/// The server responds with a [`Response`] after processing.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    /// Register with the server using a username.
    ///
    /// Must be the first message sent. Username must be unique.
    /// Server responds with `Success` or `Error` (if username taken).
    Connect { username: String },

    /// Send a message to the client's current room.
    ///
    /// Broadcasts to all users in the same room.
    /// Client must be connected first.
    SendRoomMessage { text: String },

    /// Join a chat room.
    ///
    /// Switches the client's active room. Default room is "general".
    JoinRoom { room_name: String },

    /// Send a private message to another user.
    ///
    /// Delivered only to the recipient. Returns error if user not found.
    SendPrivateMessage { to: String, text: String },

    /// Request list of all online users.
    ///
    /// Server responds with `UserList`.
    ListUsers,

    /// Gracefully disconnect from the server.
    Disconnect,
}

/// Responses sent from server to client.
///
/// Clients should handle all variants, as the server may push messages
/// asynchronously (e.g., `IncomingMessage` from other users).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    /// Operation completed successfully.
    Success { message: String },

    /// Operation failed with an error message.
    Error { message: String },

    /// List of currently online usernames.
    ///
    /// Sent in response to [`Message::ListUsers`].
    UserList { users: Vec<String> },

    /// Incoming chat message from another user.
    ///
    /// - `room: Some(name)` - room message
    /// - `room: None` - private/direct message
    IncomingMessage {
        from: String,
        text: String,
        room: Option<String>,
    },

    /// System notification (e.g., shutdown warning).
    SystemNotification { text: String },
}

/// Encodes a [`Message`] into a length-prefixed binary frame.
///
/// # Returns
///
/// A `Vec<u8>` containing: `[4-byte big-endian length][bincode payload]`
///
/// # Errors
///
/// Returns an error if bincode serialization fails (should not happen
/// with valid `Message` variants).
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

/// Decodes a length-prefixed binary frame into a [`Message`].
///
/// # Errors
///
/// - `"not enough bytes for length"` - fewer than 4 bytes provided
/// - `"incomplete message"` - length header indicates more bytes than available
/// - `"deserialization failed: ..."` - payload is not a valid `Message`
///
/// # Note
///
/// For streaming sockets, prefer [`try_extract_frame`] which handles
/// partial reads gracefully.
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

/// Encodes a [`Response`] into a length-prefixed binary frame.
///
/// # Returns
///
/// A `Vec<u8>` containing: `[4-byte big-endian length][bincode payload]`
///
/// # Errors
///
/// Returns an error if bincode serialization fails.
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

/// Decodes a length-prefixed binary frame into a [`Response`].
///
/// # Errors
///
/// - `"not enough bytes for length"` - fewer than 4 bytes provided
/// - `"incomplete response"` - length header indicates more bytes than available
/// - `"deserialization failed: ..."` - payload is not a valid `Response`
///
/// # Note
///
/// For streaming sockets, prefer [`try_extract_response`] which handles
/// partial reads gracefully.
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

/// Maximum allowed message size (1 MB).
///
/// Messages larger than this are rejected to prevent memory exhaustion attacks.
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Attempts to extract a complete [`Message`] from an accumulator buffer.
///
/// This implements the accumulator pattern for streaming sockets, handling
/// partial reads and multiple messages in a single buffer.
///
/// # Returns
///
/// - `Ok(None)` - not enough bytes yet, keep reading
/// - `Ok(Some((message, consumed)))` - message extracted, drain `consumed` bytes
/// - `Err(...)` - malformed data, connection should be closed
///
/// # Example
///
/// ```
/// use lynx_protocol::try_extract_frame;
///
/// let mut accumulator = Vec::new();
/// // ... read bytes into accumulator ...
///
/// loop {
///     match try_extract_frame(&accumulator) {
///         Ok(Some((msg, consumed))) => {
///             // handle msg
///             accumulator.drain(..consumed);
///         }
///         Ok(None) => break, // need more data
///         Err(e) => panic!("protocol error: {}", e),
///     }
/// }
/// ```
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

/// Attempts to extract a complete [`Response`] from an accumulator buffer.
///
/// Client-side equivalent of [`try_extract_frame`]. See that function
/// for usage pattern.
///
/// # Returns
///
/// - `Ok(None)` - not enough bytes yet, keep reading
/// - `Ok(Some((response, consumed)))` - response extracted, drain `consumed` bytes
/// - `Err(...)` - malformed data
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

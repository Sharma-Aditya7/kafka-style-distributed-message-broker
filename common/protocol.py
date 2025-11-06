"""
Message Protocol Definitions
Defines the message formats and serialization/deserialization logic
"""
import json
import time
from typing import Dict, Any, Optional


class MessageType:
    """Message type constants"""
    PRODUCE = "PRODUCE"
    REPLICATE = "REPLICATE"
    FETCH = "FETCH"
    ACK = "ACK"
    HEARTBEAT = "HEARTBEAT"
    METADATA = "METADATA"
    ERROR = "ERROR"


class Message:
    """Base message class for communication between nodes"""

    def __init__(self, msg_type: str, data: Any = None, offset: Optional[int] = None,
                 timestamp: Optional[float] = None, message_id: Optional[str] = None):
        self.type = msg_type
        self.data = data
        self.offset = offset
        self.timestamp = timestamp or time.time()
        self.message_id = message_id

    def to_json(self) -> str:
        """Serialize message to JSON string"""
        return json.dumps({
            'type': self.type,
            'data': self.data,
            'offset': self.offset,
            'timestamp': self.timestamp,
            'message_id': self.message_id
        })

    @staticmethod
    def from_json(json_str: str) -> 'Message':
        """Deserialize message from JSON string"""
        data = json.loads(json_str)
        return Message(
            msg_type=data.get('type'),
            data=data.get('data'),
            offset=data.get('offset'),
            timestamp=data.get('timestamp'),
            message_id=data.get('message_id')
        )

    def __repr__(self):
        return f"Message(type={self.type}, offset={self.offset}, data={self.data})"


def create_produce_message(data: Any, message_id: str) -> Message:
    """Create a PRODUCE message"""
    return Message(MessageType.PRODUCE, data=data, message_id=message_id)


def create_replicate_message(data: Any, offset: int, message_id: str) -> Message:
    """Create a REPLICATE message"""
    return Message(MessageType.REPLICATE, data=data, offset=offset, message_id=message_id)


def create_ack_message(offset: int, success: bool = True, error_msg: str = None) -> Message:
    """Create an ACK message"""
    return Message(MessageType.ACK, data={'success': success, 'error': error_msg}, offset=offset)


def create_fetch_message(start_offset: int, max_messages: int = 100) -> Message:
    """Create a FETCH message"""
    return Message(MessageType.FETCH, data={'start_offset': start_offset, 'max_messages': max_messages})


def create_heartbeat_message() -> Message:
    """Create a HEARTBEAT message"""
    return Message(MessageType.HEARTBEAT)


def create_metadata_message(leader_host: str, leader_port: int) -> Message:
    """Create a METADATA message"""
    return Message(MessageType.METADATA, data={'leader_host': leader_host, 'leader_port': leader_port})


def create_error_message(error_msg: str, error_type: str = "GENERAL") -> Message:
    """Create an ERROR message"""
    return Message(MessageType.ERROR, data={'error': error_msg, 'error_type': error_type})


def send_message(sock, message: Message) -> None:
    """
    Send a message over a socket with length prefix
    Format: [4 bytes length][JSON message]
    """
    json_data = message.to_json()
    encoded = json_data.encode('utf-8')
    length = len(encoded)

    # Send length prefix (4 bytes, big-endian)
    sock.sendall(length.to_bytes(4, byteorder='big'))
    # Send actual message
    sock.sendall(encoded)


def receive_message(sock, timeout: int = 30) -> Optional[Message]:
    """
    Receive a message from a socket with length prefix
    Format: [4 bytes length][JSON message]
    """
    sock.settimeout(timeout)

    try:
        # Read length prefix (4 bytes)
        length_bytes = sock.recv(4)
        if not length_bytes or len(length_bytes) < 4:
            return None

        message_length = int.from_bytes(length_bytes, byteorder='big')

        # Read the actual message
        chunks = []
        bytes_received = 0

        while bytes_received < message_length:
            chunk = sock.recv(min(message_length - bytes_received, 4096))
            if not chunk:
                return None
            chunks.append(chunk)
            bytes_received += len(chunk)

        json_data = b''.join(chunks).decode('utf-8')
        return Message.from_json(json_data)

    except Exception as e:
        print(f"Error receiving message: {e}")
        return None

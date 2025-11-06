"""
Log Manager for Broker
Handles message storage and retrieval
"""
import threading
from typing import List, Optional, Dict, Any


class LogEntry:
    """Represents a single log entry"""

    def __init__(self, offset: int, data: Any, message_id: str, timestamp: float):
        self.offset = offset
        self.data = data
        self.message_id = message_id
        self.timestamp = timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary"""
        return {
            'offset': self.offset,
            'data': self.data,
            'message_id': self.message_id,
            'timestamp': self.timestamp
        }


class LogManager:
    """Thread-safe log manager for storing messages"""

    def __init__(self):
        self.log: List[LogEntry] = []
        self.lock = threading.Lock()
        self.message_ids = set()  # For deduplication
        self.next_offset = 0

    def append(self, data: Any, message_id: str, timestamp: float) -> Optional[int]:
        """
        Append a message to the log
        Returns the offset if successful, None if duplicate
        """
        with self.lock:
            # Check for duplicate message_id
            if message_id in self.message_ids:
                print(f"Duplicate message detected: {message_id}")
                # Find and return existing offset
                for entry in self.log:
                    if entry.message_id == message_id:
                        return entry.offset
                return None

            # Append new entry
            offset = self.next_offset
            entry = LogEntry(offset, data, message_id, timestamp)
            self.log.append(entry)
            self.message_ids.add(message_id)
            self.next_offset += 1

            print(f"Appended message at offset {offset}: {data}")
            return offset

    def get_messages(self, start_offset: int, max_messages: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve messages starting from start_offset
        Returns list of message dictionaries
        """
        with self.lock:
            if start_offset < 0 or start_offset >= len(self.log):
                return []

            end_offset = min(start_offset + max_messages, len(self.log))
            messages = [self.log[i].to_dict() for i in range(start_offset, end_offset)]
            return messages

    def get_latest_offset(self) -> int:
        """Get the latest offset (next offset - 1)"""
        with self.lock:
            return self.next_offset - 1 if self.next_offset > 0 else -1

    def get_message_count(self) -> int:
        """Get total number of messages in log"""
        with self.lock:
            return len(self.log)

    def get_message_at_offset(self, offset: int) -> Optional[Dict[str, Any]]:
        """Get a specific message by offset"""
        with self.lock:
            if 0 <= offset < len(self.log):
                return self.log[offset].to_dict()
            return None

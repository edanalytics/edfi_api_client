import collections
import logging

from typing import Dict, List, Optional


class ResponseLog:
    """

    """
    SUCCESS: str = "Success"
    ERROR  : str = "Error"

    def __init__(self, log_every: int = 500):
        self.log_dict: Dict[int, str] = {}
        self.log_every: int = log_every

    def __len__(self) -> int:
        return len(self.log_dict)

    def record(self, key: str, status: Optional[str] = None, message: Optional[str] = None):
        # 200 responses return no JSON message.
        if not status:
            status = self.ERROR

        # Caught exceptions return no status codes.
        if not message:
            message = self.SUCCESS

        self.log_dict[key] = (status, message)
        self.log_progress(force=False)  # Only log every N responses

    def count_statuses(self) -> Dict[str, int]:
        counts_by_elem1 = collections.Counter(status for status, _ in self.log_dict.values())
        return dict(counts_by_elem1)

    def aggregate_messages(self) -> Dict[str, List[str]]:
        message_indexes = collections.defaultdict(list)

        for id, (status, message) in self.log_dict.items():
            full_message = f"{status} {message}"
            message_indexes[full_message].append(id)

        # Sort outputs before returning.
        sorted_values = {
            key: sorted(value) for key, value in message_indexes.items()
        }
        return sorted_values

    def log_progress(self, force: bool = True):
        # Do not log empty dict, and only log every N records.
        if not force and len(self.log_dict) % self.log_every and self.log_dict:
            return

        status_counts = self.count_statuses()
        status_strings = [f"({status}: {count})" for status, count in status_counts.items()]

        message = f"[Count Processed: {len(self.log_dict)}]"
        if status_strings:
            message += ": " + ', '.join(status_strings)
        logging.info(message)

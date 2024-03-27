import collections
import logging

from typing import Any, Dict, List, Optional, Tuple, Union


class ResponseLog:
    """
    This is a helper class for documenting responses in long-running posts, deletes, and puts.
    It will display an overview when `log_progress` is called and the length of the dictionary is modulus to `log_every`.
    """
    SUCCESS: str = "Success"
    ERROR  : str = "Error"

    def __init__(self, log_every: int = 500):
        self.log_dict: Dict[int, str] = {}
        self.log_every: int = log_every

    def __len__(self) -> int:
        return len(self.log_dict)

    def __repr__(self) -> str:
        return str(self.count_messages())

    def record(self, key: int, status: Optional[Union[str, int]] = None, message: Optional[str] = None):
        status = status or self.ERROR  # Caught exceptions return no status codes.
        message = message or self.SUCCESS  # 200 responses return no JSON message.

        self.log_dict[key] = (str(status), str(message))
        self.log_progress(force=False)  # Only log every N responses

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

    def count_statuses(self) -> Dict[str, int]:
        counts_by_elem1 = collections.Counter(status for status, _ in self.log_dict.values())
        return dict(counts_by_elem1)

    def count_messages(self) -> Dict[Tuple[str, str], int]:
        counts_by_value = collections.Counter(self.log_dict.values())
        return dict(counts_by_value)

    def aggregate_statuses(self) -> Dict[str, List[str]]:
        message_indexes = collections.defaultdict(list)
        for id, (status, _) in self.log_dict.items():
            message_indexes[status].append(id)
        return self._sort_value_lists(message_indexes)  # Sort outputs before returning.

    def aggregate_messages(self) -> Dict[Tuple[str, str], List[str]]:
        message_indexes = collections.defaultdict(list)
        for id, (status, message) in self.log_dict.items():
            message_indexes[(status, message)].append(id)
        return self._sort_value_lists(message_indexes)  # Sort outputs before returning.

    @staticmethod
    def _sort_value_lists(response_dict: Dict[Any, List[str]]):
        return {
            key: sorted(value) for key, value in response_dict.items()
        }

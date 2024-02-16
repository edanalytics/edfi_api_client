import aiohttp
import collections
import logging

from typing import Dict, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from requests import Response
    from aiohttp import ClientResponse


class ResponseLog:
    """

    """
    SUCCESS: str = "Success"
    ERROR  : str = "Error"

    def __init__(self):
        self.log_dict: Dict[int, str] = {}

    def __len__(self):
        return len(self.log_dict)

    def count_statuses(self):
        counts_by_elem1 = collections.Counter(status for status, _ in self.log_dict.values())
        return dict(counts_by_elem1)

    def aggregate_messages(self):
        message_indexes = collections.defaultdict(list)

        for id, (status, message) in self.log_dict.items():
            full_message = f"{status} {message}"
            message_indexes[full_message].append(id)

        # TODO: Sort IDs before displaying.
        return dict(message_indexes)

    def log_progress(self, n: int = 1):
        # Do not log empty dict, and only log every N records.
        if len(self.log_dict) % n and self.log_dict:
            return

        status_counts = self.count_statuses()
        # status_counts = sorted(status_counts, key=self.custom_status_sort) # See TD below.
        status_strings = [f"({status}: {count})" for status, count in status_counts.items()]

        message = f"[Count Processed: {len(self.log_dict)}]"
        if status_strings:
            message += ": " + ', '.join(status_strings)
        logging.info(message)

    # TODO: Elegant way to put Success first and Error last?
    # @classmethod
    # def custom_status_sort(cls, item):
    #     if item == cls.SUCCESS:
    #         return 0  # Always first!
    #     return item



    def record(self,
        idx: int,
        *,
        response: Optional['Response'] = None,
        message: Optional[Exception] = None
    ):
        """
        Helper for saving response outputs during POSTs/DELETEs
        """
        if response is not None:
            status = str(response.status_code)
            if not response.ok:
                message = response.json().get('message')
            else:
                message = self.SUCCESS

        else:
            status = self.ERROR
            message = str(message)

        self.log_dict[idx] = (status, message)

    async def async_record(self,
        idx: int,
        *,
        response: Optional['ClientResponse'] = None,
        message: Optional[Exception] = None
    ):
        """
        Same as log_response, but with async responses.
        TODO: Union these into a single method.
        """
        if response is not None:
            status = str(response.status)
            if not response.ok:
                res_json = await response.json()
                message = res_json.get('message')
            else:
                message = self.SUCCESS

        else:
            status = self.ERROR
            message = str(message)

        self.log_dict[idx] = (status, message)

import os
import time
import contextlib
import json
import logging

import portalocker


class TokenCacheError(Exception):
    pass


class PortalockerTokenCache():
    def __init__(
        self, 
        token_id,
        token_cache_directory: str = '~/.edfi-tokens',
        default_timeout=20
    ):
        self.cache_path: str = os.path.expanduser(f'{token_cache_directory}/{token_id}.json')
        self.default_timeout = default_timeout
        self.read_lock = portalocker.Lock(
            self.cache_path,
            mode='r+',
            timeout=self.default_timeout,
            flags=portalocker.LockFlags.SHARED | portalocker.LockFlags.NON_BLOCKING
        )
        self.write_lock = portalocker.Lock(
            self.cache_path,
            mode='a+',
            timeout=self.default_timeout,
            flags=portalocker.LockFlags.EXCLUSIVE | portalocker.LockFlags.NON_BLOCKING
        )

        # Make sure parent directory exists
        os.makedirs(os.path.expanduser(token_cache_directory), exist_ok=True)

    def exists(self):
        return os.path.exists(self.cache_path)


    def load(self) -> dict: 
        """Loads value from cache"""
        try:
            logging.info(f'Loading cache from {self.cache_path}')
            if self.read_lock.fh:
                f = self.read_lock.fh
                value = json.loads(f.read())
            elif self.write_lock.fh:
                f = self.write_lock.fh
                value = json.loads(f.read())
            else:
                raise TokenCacheError('Lock not held')
        except json.JSONDecodeError:
            raise TokenCacheError('Cache corruption')
        except FileNotFoundError:
            raise TokenCacheError('Cache does not yet exist')

        return value


    def update(self, value: dict):
        """Updates cache with new value"""
        if self.write_lock.fh:
            f = self.write_lock.fh
            f.seek(0)
            f.truncate()
        else:
            raise TokenCacheError('Lock not held')
        logging.info(f'Writing cache to{self.cache_path}')
        f.write(json.dumps(value))


    def get_last_modified(self) -> int:
        """Gets Unix time of when cache was last modified"""

        if os.path.exists(self.cache_path):
            return os.path.getmtime(self.cache_path)
        else:
            return 0

    @contextlib.contextmanager
    def get_read_lock(self, max_retries=3):
        # A portalocker lock with non-blocking and a timeout set has its own repeated
        # retry logic; however, nice to surface some info in logs with our own retries.
        try:
            attempt = 0
            f = None
            while attempt <= max_retries:
                try:
                    f = self.read_lock.acquire()
                    break
                except Exception as err:
                    logging.info(f'Failed to acquire read lock; {max_retries - attempt} tries left')
                    time.sleep(2**attempt)
                    attempt += 1

            if not f:
                raise TokenCacheError('Unable to acquire read lock on token cache.')
            else:
                yield f
        finally:
            self.read_lock.release()
            
    @contextlib.contextmanager
    def get_write_lock(self, max_retries=3):
        try:
            f = None
            attempt = 0
            while attempt <= max_retries:
                try:
                    f = self.write_lock.acquire()
                    break
                except portalocker.exceptions.LockException as err:
                    logging.info(f'Failed to acquire write lock; {max_retries - attempt} tries left')
                    time.sleep(2**attempt)
                    attempt += 1

            if not f:
                raise TokenCacheError('Unable to acquire write lock on token cache.')
            yield f
        finally:
            self.write_lock.release()

            

import os
import time
import json
import logging
import abc
import contextlib
import hashlib
from pathlib import Path

from typing import Union, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from edfi_api_client.session import EdFiSession


class TokenCacheError(Exception):
    pass


class BaseTokenCache(abc.ABC):
    """Base interface for a single EdFiSession, having a single oauth url /
    client key"""

    @abc.abstractmethod
    def exists(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_modified(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def load(self) -> dict:
        """
        Load value from cache

        Should assume that a read or a write lock has already been acquired by
        caller.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, value: dict):
        """
        Update value in cache

        Should assume that a write lock has already been acquired by caller.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_read_lock(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def get_write_lock(self, **kwargs):
        raise NotImplementedError
    
    # EdFiSession should pass in a reference to itself to provide access
    # to any needed attributes, such as OAuth URL or client ID
    @property
    @abc.abstractmethod
    def session(self):
        raise NotImplementedError

    @session.setter
    @abc.abstractmethod
    def session(self, val):
        raise NotImplementedError


class LockfileTokenCache(BaseTokenCache):
    """
    On-disk token cache coordinated with a lockfile, dependent on OS
    implementation of exclusive file creation. 
    """

    def __init__(
        self, 
        token_cache_directory: Union[str, os.PathLike] = '~/.edfi-tokens',
        write_lock_timeout: int = 30,
        write_lock_staleness_threshold: int = 60,
        write_lock_retry_delay: float = 0.5
    ):
        # On-disk token cache instance variables
        self.token_cache_directory = token_cache_directory
        os.makedirs(os.path.expanduser(self.token_cache_directory), exist_ok=True)

        # Token id passed in after instantiation by EdFiSession; initialize associated
        # paths to None
        self._session: Optional[EdFiSession] = None
        self._token_id : str = 'default'
        self.cache_path : os.PathLike = Path(self.token_cache_directory) / 'default.json'
        self.lockfile_path : os.PathLike = Path(self.token_cache_directory) / 'default.json.lock'

        # Other configuration instance variables
        self.write_lock_timeout = write_lock_timeout
        self.write_lock_staleness_threshold = write_lock_staleness_threshold
        self.write_lock_retry_delay = write_lock_retry_delay

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, val):
        self._session = val

        # Hash oauth url and client key for unique filename
        instance_client_id = hashlib.md5(self._session.oauth_url.encode('utf-8'))
        instance_client_id.update(self._session.client_key.encode('utf-8'))
        self._token_id = instance_client_id.hexdigest()

        # Update associated paths
        self.cache_path = os.path.expanduser(f'{self.token_cache_directory}/{self._token_id}.json')
        self.lockfile_path = self.cache_path + '.lock'

    def exists(self):
        return os.path.exists(self.cache_path)

    def get_last_modified(self) -> int:
        """Gets Unix time of when cache was last modified"""
        if os.path.exists(self.cache_path):
            return int(os.path.getmtime(self.cache_path))
        else:
            return 0

    def load(self) -> dict: 
        """Loads value from cache"""
        try:
            logging.info(f'Loading cache from {self.cache_path}')
            with open(self.cache_path, 'r') as fp:
                value = json.loads(fp.read())
        except json.JSONDecodeError:
            raise TokenCacheError('Cache corruption')
        except FileNotFoundError:
            raise TokenCacheError('Cache does not yet exist')

        return value

    def update(self, value: dict):
        """Updates cache with new value"""
        with open(self.cache_path, 'w') as fp:
            logging.info(f'Writing cache to {self.cache_path}')
            fp.write(json.dumps(value))

    @contextlib.contextmanager
    def get_read_lock(self, **kwargs):
        # Optimistic
        yield True

    @contextlib.contextmanager
    def get_write_lock(self):
        try:
            timeout_end = time.time() + self.write_lock_timeout
            acquired = False

            while time.time() <= timeout_end:
                try:
                    if os.path.exists(self.lockfile_path):
                        lockfile_age = time.time() - os.path.getmtime(self.lockfile_path)
                        if lockfile_age > self.write_lock_staleness_threshold: 
                            # assume another client died while holding the lock
                            logging.info(f'Lockfile at {self.lockfile_path} touched more than {self.write_lock_staleness_threshold}s ago. Removing lockfile.')
                            os.remove(self.lockfile_path)
                    
                    with open(self.lockfile_path, 'x') as f:
                        f.write(f'{os.getpid()}')
                        acquired = True
                    
                    break

                except FileNotFoundError:
                    # case where lockfile is removed in between check for lockfile and getmtime
                    time.sleep(self.write_lock_retry_delay)
                    
                except FileExistsError:
                    # case where lockfile already exists and it is not yet considered stale
                    time.sleep(self.write_lock_retry_delay)

            if not acquired:
                raise TokenCacheError('Unable to acquire write lock on token cache.')
            
            yield acquired
        
        finally:
            if acquired and os.path.exists(self.lockfile_path):
                os.remove(self.lockfile_path)

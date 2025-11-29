import os
import time
import json
import logging
import abc
import contextlib

from typing import Union


class TokenCacheError(Exception):
    pass


class BaseTokenCache(abc.ABC):
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

    @property
    @abc.abstractmethod
    def token_id(self):
        raise NotImplementedError

    @token_id.setter
    @abc.abstractmethod
    def token_id(self, val):
        raise NotImplementedError


class LockfileTokenCache(BaseTokenCache):
    def __init__(
        self, 
        token_cache_directory: Union[str, os.PathLike] = '~/.edfi-tokens',
    ):
        self.token_cache_directory = token_cache_directory
        self._token_id = None # updated after instantiation by EdFiSession

        # Make sure parent directory exists
        os.makedirs(os.path.expanduser(self.token_cache_directory), exist_ok=True)

    @property
    def token_id(self):
        return self._token_id
    
    @token_id.setter
    def token_id(self, val):
        self._token_id = val
        
        # Update associated paths
        self.cache_path = os.path.expanduser(f'{self.token_cache_directory}/{self._token_id}.json')
        self.lockfile_path = self.cache_path + '.lock'

    def exists(self):
        return os.path.exists(self.cache_path)

    def get_last_modified(self) -> int:
        """Gets Unix time of when cache was last modified"""
        if os.path.exists(self.cache_path):
            return os.path.getmtime(self.cache_path)
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
    def get_write_lock(self, timeout: int = 30, staleness_threshold: int = 60):
        try:
            timeout_end = time.time() + timeout
            acquired = False

            while time.time() <= timeout_end:
                try:
                    if os.path.exists(self.lockfile_path):
                        lockfile_age = time.time() - os.path.getmtime(self.lockfile_path)
                        if lockfile_age > staleness_threshold: 
                            # assume another client died while holding the lock
                            logging.info(f'Lockfile at {self.lockfile_path} touched more than {staleness_threshold}s ago. Removing lockfile.')
                            os.remove(self.lockfile_path)
                    
                    with open(self.lockfile_path, 'x') as f:
                        f.write(f'{os.getpid()}')
                        acquired = True
                    
                    break

                except FileNotFoundError as err:
                    # case where lockfile is removed in between check for lockfile and getmtime
                    time.sleep(0.25)
                    
                except FileExistsError as err:
                    # TODO; make sleep time configurable?
                    time.sleep(0.25)

            if not acquired:
                raise TokenCacheError('Unable to acquire write lock on token cache.')
            
            yield acquired
        
        finally:
            if acquired and os.path.exists(self.lockfile_path):
                os.remove(self.lockfile_path)

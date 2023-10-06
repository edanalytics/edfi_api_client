import logging

from typing import Iterator, List, Optional

from edfi_api_client import util


class EdFiParams(dict):
    """
    Many parameters can optionally be passed to GET-requests to the Ed-Fi API.
    This class consistently builds and increments these parameters.
    They can be passed in either via a `params` dictionary, or as kwargs.
    """
    def __init__(self,
        params: Optional[dict] = None,
        **kwargs
    ):
        _sanitized = self.sanitize_params(params, **kwargs)
        super().__init__(_sanitized)

        self.min_change_version = self.get('minChangeVersion')
        self.max_change_version = self.get('maxChangeVersion')

    def copy(self) -> 'EdFiParams':
        return EdFiParams( super().copy() )

    @classmethod
    def sanitize_params(cls,
        params: Optional[dict] = None,
        **kwargs
    ) -> dict:
        """
        To maximize flexibility to the user, params can be passed in a dictionary or as kwargs.
        These are all sterilized to enforce camelCasing and to remove null values.

        If multiples of the same key are provided within params or kwargs, the last of each is chosen.
        If multiples of the same key are provided between params and kwargs, the kwarg is chosen.

        This method ensures that defining params is consistent, regardless of means of input.

        :param params:
        :param kwargs:
        :return:
        """
        def __get_duplicates(list_: List[str]):
            return set(
                item for item in list_ if list_.count(item) > 1
            )

        # Retrieve all non-null params and kwargs passed by the user.
        _params = {
            key: val for key, val in (params or {}).items()
            if val is not None
        }
        _kwargs = {
            key: val for key, val in kwargs.items()
            if val is not None
        }

        # Make sure the user does not pass in duplicates in either params or kwargs.
        cc_params = [util.snake_to_camel(key) for key in _params.keys()]
        cc_kwargs = [util.snake_to_camel(key) for key in _kwargs.keys()]

        for key in __get_duplicates(cc_params):
            logging.warning(f"Duplicate key `{key}` found in `params`! The last will be used.")

        for key in __get_duplicates(cc_kwargs):
            logging.warning(f"Duplicate key `{key}` found in `kwargs`! The last will be used.")


        # Make sure the user does not pass in duplicates between params and kwargs.
        cc_kwargs_params = list(set(cc_params)) + list(set(cc_kwargs))

        for key in __get_duplicates(cc_kwargs_params):
            logging.warning(f"Duplicate key `{key}` found between `params` and `kwargs`! The kwarg will be used.")

        # Populate the final parameters.
        final_params = {}

        for key, val in _params.items():
            final_params[util.snake_to_camel(key)] = val

        for key, val in _kwargs.items():
            final_params[util.snake_to_camel(key)] = val

        return final_params

    def build_offset_window_params(self, page_size: int, total_count: int) -> Iterator['EdFiParams']:
        """
        Iterate offset-stepping by `page_size` until `total_count` is reached.

        :param page_size:
        :param total_count:
        :return:
        """
        for offset in range(0, total_count, page_size):
            offset_params = self.copy()
            offset_params["limit"] = page_size
            offset_params["offset"] = offset

            yield offset_params

    def build_change_version_window_params(self, change_version_step_size: int) -> Iterator['EdFiParams']:
        """
        Iterate change-version-stepping by `change_version_step_size` until `max_change_version` is reached.

        :param change_version_step_size:
        :return:
        """
        if self.min_change_version is None or self.max_change_version is None:
            raise ValueError(
                "! Cannot paginate change version steps without specifying min and max change versions!"
            )

        change_version_step_windows = range(self.min_change_version, self.max_change_version, change_version_step_size)
        for idx, cv_window_start in enumerate(change_version_step_windows):
            cv_params = self.copy()
            cv_params['minChangeVersion'] = cv_window_start + bool(idx)  # Add one to prevent overlaps
            cv_params['maxChangeVersion'] = min(self.max_change_version, cv_window_start + change_version_step_size)

            yield cv_params

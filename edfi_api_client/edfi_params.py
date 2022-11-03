import logging

from typing import List, Optional

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

        # These parameters are only used during pagination. They must be explicitly initialized.
        self.page_size = None
        self.change_version_step_size = None


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



    ### Methods for preparing params for pagination and completing pagination
    def init_page_by_offset(self, page_size: int = 100):
        """

        :param page_size:
        :return:
        """
        self.page_size = page_size

        if 'limit' in self or 'offset' in self:
            logging.warning("The previously-defined limit and offset will be reset for paging.")

        self['limit'] = self.page_size
        self['offset'] = 0


    def init_page_by_change_version_step(self, change_version_step_size: int = 50000):
        """

        :param change_version_step_size:
        :return:
        """
        self.change_version_step_size = change_version_step_size

        if self.min_change_version is None or self.max_change_version is None:
            raise ValueError("! Cannot paginate change version steps without specifying min and max change versions!")

        # Reset maxChangeVersion in parameters to narrow the ingestion window to the step size.
        self['maxChangeVersion'] = min(
            self.max_change_version,
            self.min_change_version + self.change_version_step_size
        )


    def page_by_offset(self):
        """

        :return:
        """
        if self.page_size is None:
            raise ValueError("To paginate by offset, you must first prepare the class using `init_page_by_offset()`!")

        self['offset'] += self.page_size


    def page_by_change_version_step(self):
        """

        :return:
        """
        if self.change_version_step_size is None:
            raise ValueError("To paginate by offset, you must first prepare the class using `init_page_by_change_version_step()`!")

        # Increment min and max change version only if still within the max change version window.
        new_min_change_version = self['maxChangeVersion'] + 1
        if new_min_change_version > self.max_change_version:
            raise StopIteration

        else:
            self['minChangeVersion'] = new_min_change_version
            self['maxChangeVersion'] = min(
                self['maxChangeVersion'] + self.change_version_step_size,
                self.max_change_version
            )

            # Reset the offset counter for the next window of change versions.
            self['offset'] = 0

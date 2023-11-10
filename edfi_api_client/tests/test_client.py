import easecret
import pytest

from edfi_api_client import EdFiClient


###
master_secret = "edfi_scde_2023"


def test_unauthenticated_client(secret: str = master_secret):
    """

    :param secret:
    :return:
    """
    credentials = easecret.get_secret(secret)
    base_url = credentials.get('base_url')
    edfi = EdFiClient(base_url)

    ### Info Payload
    info_payload = edfi.get_info()
    payload_keys = ('apiMode', 'version', 'dataModels', 'urls',)
    assert all(key in info_payload for key in payload_keys)

    ### Swagger
    _ = edfi.get_swagger(component='resources')
    _ = edfi.get_swagger(component='descriptors')
    _ = edfi.get_swagger(component='composites')

    ### Authenticated methods
    with pytest.raises(ValueError):
        _ = edfi.get_newest_change_version()

    with pytest.raises(ValueError):
        _ = edfi.resource('students', minChangeVersion=0, maxChangeVersion=100000)

    with pytest.raises(ValueError):
        _ = edfi.descriptor('language_use_descriptors')

    with pytest.raises(ValueError):
        _ = edfi.composite('students')


def test_authenticated_client(secret: str = master_secret):
    """

    :param secret:
    :return:
    """
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials)

    _ = edfi.get_newest_change_version()

    ### Resource
    resource = edfi.resource('students', minChangeVersion=0, maxChangeVersion=500000)
    assert resource.ping().ok

    assert resource.description
    assert resource.has_deletes
    assert resource.fields
    assert resource.required_fields

    resource_count = resource.total_count()
    resource_output_path = f"./.output/{resource.name}.jsonl"

    _ = resource.get_to_json(resource_output_path, page_size=500, retry_on_failure=True, step_change_version=True)

    with open(resource_output_path, 'r') as fp:
        assert len(fp.readlines()) == resource_count

    ### Descriptor
    descriptor = edfi.descriptor('language_use_descriptors')
    assert descriptor.ping().ok
    assert descriptor.description
    assert descriptor.has_deletes
    assert descriptor.fields
    assert descriptor.required_fields

    descriptor_count = descriptor.total_count()
    descriptor_rows = descriptor.get_rows(page_size=500, step_change_version=False)
    assert len(list(descriptor_rows)) == descriptor_count

    ### Composite
    composite = edfi.composite('students')
    assert composite.ping().ok
    assert composite.description
    assert composite.has_deletes  # TODO: Should this be False?
    assert composite.fields
    assert composite.required_fields

    # Composites don't have total-counts.
    composite_count = 42 * 10
    composite_rows = []
    for idx, row in enumerate(composite.get_rows(page_size=100)):
        if idx == composite_count:
            break
        composite_rows.append(row)
    assert len(list(composite_rows)) == composite_count


if __name__ == '__main__':
    test_unauthenticated_client()
    test_authenticated_client()

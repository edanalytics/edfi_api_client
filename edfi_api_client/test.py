import easecret

from edfi_api_client import EdFiClient


###
master_secret = "edfi_scde_2024"

def test_client(secret: str = master_secret):
    """

    :param secret:
    :return:
    """
    credentials = easecret.get_secret(secret)
    base_url = credentials.get('base_url')


    ##### Unauthorized Client
    edfi = EdFiClient(base_url)

    ### Info Payload
    info_payload = edfi.get_info()
    payload_keys = ('apiMode', 'version', 'dataModels', 'urls',)
    assert all(key in info_payload for key in payload_keys)

    ### Swagger
    _ = edfi.get_swagger(component='resources')
    _ = edfi.get_swagger(component='descriptors')
    _ = edfi.get_swagger(component='composites')


    ##### Authorized Client
    edfi = EdFiClient(**credentials)
    _ = edfi.get_newest_change_version()

    ### Resource
    resource = edfi.resource('students', minChangeVersion=0, maxChangeVersion=100000)
    assert resource.ping().ok

    _ = resource.description
    _ = resource.has_deletes

    resource_count = resource.total_count()
    resource_rows = resource.get_rows(page_size=500, retry_on_failure=True, step_change_version=True)
    assert len(list(resource_rows)) == resource_count

    ### Descriptor
    descriptor = edfi.resource('language_use_descriptors')
    assert descriptor.ping().ok
    _ = descriptor.description
    _ = descriptor.has_deletes

    descriptor_count = descriptor.total_count()
    descriptor_rows = descriptor.get_rows(page_size=500, step_change_version=False)
    assert len(list(descriptor_rows)) == descriptor_count

    ### Composite
    composite = edfi.composite('students')
    assert composite.ping().ok
    _ = composite.description
    _ = composite.has_deletes

    # Composites don't have total-counts.
    composite_count = 42 * 10
    composite_rows = []
    for idx, row in enumerate(composite.get_rows(page_size=100)):
        if idx == composite_count:
            break
        composite_rows.append(row)
    assert len(list(composite_rows)) == composite_count


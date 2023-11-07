import easecret
import os

from edfi_api_client import EdFiClient


###
master_secret = "edfi_partnersc_2024"


def test_async_post(secret: str = master_secret):
    """

    :param secret:
    :return:
    """
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=False)

    scratch_dir = "./.scratch"
    os.makedirs(scratch_dir, exist_ok=True)

    async_get_kwargs = dict(
        retry_on_failure=True,
        page_size=500,
        pool_size=8
    )

    resources = (
        'students',
        'studentSectionAssociations',
        'studentAssessments',
        'studentSectionAttendanceEvents',
    )

    for resource in resources:
        output_path = os.path.join(scratch_dir, f"{resource}_async.jsonl")
        async_get_kwargs.update(path=output_path)

        endpoint = edfi.resource(resource)
        print(f"{resource}: {endpoint.total_count()}")

        # Get N rows to re-insert back into Ed-Fi
        endpoint.async_get_to_json(**async_get_kwargs)
        print(f"    Rows written to {output_path}")

        # Insert those rows back into the ODS.
        error_log = endpoint.async_post_from_json(output_path, pool_size=8)
        print(error_log)



if __name__ == '__main__':
    test_async_post()
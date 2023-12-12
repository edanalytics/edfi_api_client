import easecret
import os

from edfi_api_client import EdFiClient


### We have to copy data from the dev district into the testing ODS.
output_secret = "edfi_partnersc_2024"
input_secret = "edfi_eshara_test"


def test_async_post():
    """

    :param secret:
    :return:
    """
    output_edfi = EdFiClient(**easecret.get_secret(output_secret), verbose=False)
    input_edfi  = EdFiClient(**easecret.get_secret(input_secret) , verbose=False)

    async_get_kwargs = dict(
        retry_on_failure=True,
        page_size=500,
        pool_size=8
    )

    resources = [
        ('ed-fi', 'students'),
        ('ed-fi', 'localEducationAgencies'),
        ('ed-fi', 'schools'),
        # ('ed-fi', 'studentSchoolAssociations'),
        # ('ed-fi', 'studentAssessments'),
        # ('ed-fi', 'studentSectionAttendanceEvents'),
    ]

    for namespace, rr in output_edfi.descriptors + resources:
        try:
            output_path = os.path.join(scratch_dir, f"{rr}_async.jsonl")
            async_get_kwargs.update(path=output_path)

            # Get all rows to insert back into Ed-Fi
            output_endpoint = output_edfi.resource((namespace, rr))
            print(f"{namespace}/{rr}: {output_endpoint.total_count()}")

            output_endpoint.async_get_to_json(**async_get_kwargs)
            print(f"    Rows written to {output_path}")

            # Insert those rows back into the ODS.
            input_endpoint = input_edfi.resource(rr)
            
            if rr == 'localEducationAgencies':
                print("Testing a synchronous post")
                error_log = input_endpoint.post_from_json(output_path)
            else:
                error_log = input_endpoint.async_post_from_json(output_path, pool_size=8)
            
            print(error_log)

        except Exception as err:
            print(f"ERROR: {err}")
            
        # Insert those rows back into the ODS.
        input_endpoint = input_edfi.resource(rr)


if __name__ == '__main__':
    test_async_post()

import asyncio
import easecret
import os

from edfi_api_client import EdFiClient

from test_util import format_print


# TODO: Test post(), post_rows(), async_post_rows()

### We have to copy data from the dev district into the testing ODS.
async def test_async_post(output_secret: str, input_secret: str, verbose: bool = False):
    """

    :param secret:
    :return:
    """
    sync_resources = [
        'schools',
    ]

    resources = [
        # ('ed-fi', 'localEducationAgencies'),
        ('ed-fi', 'schools'),
        # ('ed-fi', 'students'),
        # ('ed-fi', 'studentSchoolAssociations'),
        # ('ed-fi', 'studentAssessments'),
        # ('ed-fi', 'studentSectionAttendanceEvents'),
    ]

    scratch_dir = "./.scratch"
    os.makedirs(scratch_dir, exist_ok=True)

    #
    output_edfi = EdFiClient(**easecret.get_secret(output_secret), verbose=verbose)
    input_edfi = EdFiClient(**easecret.get_secret(input_secret), verbose=verbose)

    async with \
        output_edfi.async_connect(retry_on_failure=True, pool_size=8), \
        input_edfi.async_connect(retry_on_failure=True, pool_size=8):

        for namespace, rr in [
            # *output_edfi.descriptors,
            *resources
        ]:
            try:
                # Get all rows to insert back into Ed-Fi
                output_endpoint = output_edfi.resource(rr, namespace=namespace)
                input_endpoint = input_edfi.resource(rr, namespace=namespace)

                format_print(f"{namespace}/{rr}: {output_endpoint.get_total_count()}")

                # post_rows()
                if rr in sync_resources:
                    print("Testing a synchronous post")
                    row_generator = output_endpoint.get_rows(retry_on_failure=True, page_size=500)
                    error_log = input_endpoint.post_rows(row_generator)
                    print(error_log)

                print("Testing an asynchronous post")
                row_generator = output_endpoint.async_get_rows(page_size=500)
                error_log = await input_endpoint.async_post_rows(row_generator)
                print(error_log)

                # post_from_json()
                if rr in sync_resources:
                    print("Testing a synchronous get and post from json")
                    output_path = os.path.join(scratch_dir, f"{rr}_sync.jsonl")
                    output_endpoint.get_to_json(output_path, retry_on_failure=True, page_size=500)
                    error_log = input_endpoint.post_from_json(output_path)
                    print(error_log)

                print("Testing an asynchronous get and post from json")
                output_path = os.path.join(scratch_dir, f"{rr}_async.jsonl")
                await output_endpoint.async_get_to_json(output_path, page_size=500)
                error_log = await input_endpoint.async_post_from_json(output_path)

                print(error_log)

            except Exception as err:
                print(f"ERROR: {err}")


if __name__ == '__main__':
    OUTPUT_SECRET = "edfi_sc_cougar_2024"
    INPUT_SECRET = "edfi_eshara_test"
    VERBOSE = True

    asyncio.run(test_async_post(OUTPUT_SECRET, INPUT_SECRET, verbose=VERBOSE))

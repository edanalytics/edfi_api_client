import asyncio
import easecret
import json
import os
import time

from edfi_api_client import EdFiClient

from test_util import format_print


### We have to copy data from the dev district into the testing ODS.
async def test_async_delete(output_secret: str, input_secret: str, verbose: bool = False):
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


                print("Testing a synchronous get, post, and delete from json")
                start_time = time.time()
                output_path = os.path.join(scratch_dir, f"{rr}_sync.jsonl")
                output_endpoint.get_to_json(output_path, retry_on_failure=True, page_size=500)
                error_log = input_endpoint.post_from_json(output_path)
                print(error_log.count_messages())

                rows_iterator = input_endpoint.get_rows(retry_on_failure=True, page_size=500)
                error_log = input_endpoint.delete_ids((row['id'] for row in rows_iterator))
                print(error_log.count_messages())
                end_time = time.time()
                print(f"\n    Runtime: {end_time - start_time}")


                print("Testing an asynchronous get, post, and delete from json")
                start_time = time.time()
                output_path = os.path.join(scratch_dir, f"{rr}_async.jsonl")
                await output_endpoint.async_get_to_json(output_path, page_size=500)
                error_log = await input_endpoint.async_post_from_json(output_path)
                print(error_log.count_messages())

                rows_iterator = input_endpoint.async_get_rows(retry_on_failure=True, page_size=500)
                error_log = await input_endpoint.async_delete_ids((row['id'] async for row in rows_iterator))
                print(error_log.count_messages())
                end_time = time.time()
                print(f"\n    Runtime: {end_time - start_time}")

            except Exception as err:
                print(f"ERROR: {err}")


if __name__ == '__main__':
    OUTPUT_SECRET = "edfi_sc_cougar_2024"
    INPUT_SECRET = "edfi_eshara_test"
    VERBOSE = True

    asyncio.run(test_async_delete(OUTPUT_SECRET, INPUT_SECRET, verbose=VERBOSE))

import easecret
import itertools
import os
import time

from edfi_api_client import EdFiClient

from test_util import format_print, time_it

###
def test_async(secret: str, verbose: bool = False):
    """

    :param secret:
    :param verbose:
    :return:
    """
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)

    # Map resources to estimate change versions to extract N * 1000 rows.
    # Source: `edfi_scde_2023`
    max_change_versions = {
        "students": {
            20 : 2725200,  # TODO: Re-find these counts.
            40 : 3650700,
            # 80 : 1596723,
            # 160: 2928422,
            # 320: 4649473,
            # 640: 11579986,
        },
        "studentSectionAttendanceEvents": {
            20 : 217100,
            40 : 2250700,
            # 80 : 70926490,
            # 160: 71174025,
            # 320: 72151990,
            # 640: 73499849,
        },
    }
    pool_sizes = (4, 8, 16, 32,)

    scratch_dir = "./.scratch"
    os.makedirs(scratch_dir, exist_ok=True)

    async_kwargs = dict(
        retry_on_failure=True,
        page_size=500,
        step_change_version=True,
        change_version_step_size=100000,
    )

    for resource, cv_row_counts in max_change_versions.items():
        output_path = os.path.join(scratch_dir, f"{resource}_async.jsonl")
        async_kwargs.update(path=output_path)

        for k_row_count, max_change_version in cv_row_counts.items():

            endpoint = edfi.resource(resource, minChangeVersion=0, max_change_version=max_change_version)
            endpoint_count = endpoint.get_total_count()


            # ## Synchronous Pull
            # print(f"\nResource: {resource}; Num rows: {k_row_count}k; Synchronous")
            # runtime, rows = time_it(endpoint.get_rows, wrap_func=list, **async_kwargs)
            #
            # # Get row count of written file.
            # # sync_count = sum(1 for _ in open(output_path))
            # if len(rows) != endpoint_count:
            #     print("    Number of extracted rows did not match:")
            #     print(f"    Expected: {endpoint_count} ; Pulled: {len(rows)}")
            #
            # print(f"    Runtime: {runtime} seconds")


            ### Asynchronous Pulls
            for pool_size in pool_sizes:
                async_kwargs.update(pool_size=pool_size)

                # Get row count of written file.
                print(f"\nResource: {resource}; Num rows: {k_row_count}k; Pool size: {pool_size}")
                runtime, resource_output_path = time_it(endpoint.async_get_to_json, **async_kwargs)

                with open(resource_output_path, 'r') as fp:
                    line_count = sum(1 for _ in fp)
                    assert line_count == endpoint_count

                print(f"    Runtime: {runtime} seconds")


if __name__ == '__main__':
    MASTER_SECRET = "edfi_sc_cougar_2024"
    VERBOSE = False

    test_async(MASTER_SECRET, verbose=VERBOSE)

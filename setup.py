import pathlib
import setuptools

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setuptools.setup(
      name='edfi_api_client',
      version='0.2.2',
      description='Ed-Fi API client and tools',
      license_files=['LICENSE'],
      url='https://github.com/edanalytics/edfi_api_client',

      author='Erik Joranlien, Jay Kaiser',
      author_email='ejoranlien@edanalytics.org, jkaiser@edanalytics.org',

      long_description=README,
      long_description_content_type='text/markdown',
      keyword='edfi, ed-fi, api, client, data',

      packages=['edfi_api_client'],
      install_requires=[
          'requests'
      ],
      zip_safe=False,
)

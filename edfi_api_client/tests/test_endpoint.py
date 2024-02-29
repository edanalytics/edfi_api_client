import asyncio
import easecret
import os
import requests

from edfi_api_client import EdFiClient

from test_util import format_print


def test_name_properties(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)

    resource = edfi.resource('schools')
    assert resource.raw == 'edFi/schools'
    assert resource.url == 'https://cougar-dw.districts-2324.sc.startingblocks.org/data/v3/ed-fi/schools'

    deletes = edfi.resource('students', get_deletes=True)
    assert deletes.raw == 'edFi/students'
    assert deletes.url == 'https://cougar-dw.districts-2324.sc.startingblocks.org/data/v3/ed-fi/students/deletes'

    descriptor = edfi.descriptor('attendance_event_category_descriptors')  # Test conversion from snake_case
    assert descriptor.raw == 'edFi/attendanceEventCategoryDescriptors'
    assert descriptor.url == 'https://cougar-dw.districts-2324.sc.startingblocks.org/data/v3/ed-fi/attendanceEventCategoryDescriptors'

    composite = edfi.composite('staffs')
    assert composite.raw == 'edFi/staffs'
    assert composite.url == 'https://cougar-dw.districts-2324.sc.startingblocks.org/composites/v1/ed-fi/enrollment/Staffs'

    extension = edfi.resource(('ed-fi-xassessment-roster', 'assessmentAdministrations'))  # Test extension and alternative argument init
    assert extension.raw == 'edFiXassessmentRoster/assessmentAdministrations'
    assert extension.url == 'https://cougar-dw.districts-2324.sc.startingblocks.org/data/v3/ed-fi-xassessment-roster/assessmentAdministrations'

def test_swagger_properties(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)

    resource = edfi.resource('schools')
    assert resource.has_deletes == True
    assert resource.fields == [
        'indicators', 'magnetSpecialProgramEmphasisSchoolDescriptor', 'webSite', 'schoolTypeDescriptor', 'addresses',
        'schoolCategories', 'identificationCodes', 'gradeLevels', 'localEducationAgencyReference',
        'titleIPartASchoolDesignationDescriptor', 'nameOfInstitution', '_ext', 'administrativeFundingControlDescriptor',
        'schoolId', 'charterApprovalSchoolYearTypeReference', 'charterApprovalAgencyTypeDescriptor',
        'institutionTelephones', 'shortNameOfInstitution', 'internetAccessDescriptor',
        'educationOrganizationCategories', 'internationalAddresses', 'operationalStatusDescriptor', 'charterStatusDescriptor'
    ]
    assert resource.required_fields == ['schoolId', 'nameOfInstitution', 'gradeLevels', 'educationOrganizationCategories']
    assert resource.description == 'This entity represents an educational organization that includes staff and students who participate in classes and educational activity groups.'

    deletes = edfi.resource('students', get_deletes=True)
    assert deletes.has_deletes == True
    assert deletes.fields == [
        'firstName', 'birthDate', 'personalTitlePrefix', 'citizenshipStatusDescriptor', 'personalIdentificationDocuments',
        'maidenName', 'visas', 'lastSurname', 'identificationDocuments', 'studentUniqueId', 'birthCity', 'birthSexDescriptor',
        'birthStateAbbreviationDescriptor', 'middleName', 'otherNames', 'birthInternationalProvince', 'generationCodeSuffix',
        'multipleBirthStatus', 'dateEnteredUS', 'personReference', 'birthCountryDescriptor'
    ]
    assert deletes.required_fields == ['studentUniqueId', 'birthDate', 'firstName', 'lastSurname']
    assert deletes.description == 'This entity represents an individual for whom instruction, services, and/or care are provided in an early childhood, elementary, or secondary educational program under the jurisdiction of a school, education agency or other institution or program. A student is a person who has been enrolled in a school or other educational institution.'

    descriptor = edfi.descriptor('attendance_event_category_descriptors')  # Test conversion from snake_case
    assert descriptor.has_deletes == True
    assert descriptor.fields == [
        'priorDescriptorId', 'codeValue', 'description', 'effectiveEndDate', 'effectiveBeginDate', 'shortDescription',
        'namespace', 'attendanceEventCategoryDescriptorId'
    ]
    assert descriptor.required_fields == ['codeValue', 'namespace', 'shortDescription']
    assert descriptor.description == 'This descriptor holds the category of the attendance event (e.g., tardy). The map to known enumeration values is required.'

    composite = edfi.composite('staffs')
    assert composite.has_deletes == True
    assert composite.fields == [
        'races', 'firstName', 'telephones', 'staffUniqueId', 'hispanicLatinoEthnicity', 'personalIdentificationDocuments',
        'credentials', 'identificationCodes', 'lastSurname', '_ext', 'languages', 'middleName', 'internationalAddresses',
        'tribalAffiliations', 'personReference', 'highestCompletedLevelOfEducationDescriptor', 'birthDate',
        'personalTitlePrefix', 'recognitions', 'citizenshipStatusDescriptor', 'addresses', 'sexDescriptor', 'maidenName',
        'visas', 'identificationDocuments', 'yearsOfPriorTeachingExperience', 'loginId', 'oldEthnicityDescriptor',
        'ancestryEthnicOrigins', 'otherNames', 'generationCodeSuffix', 'highlyQualifiedTeacher',
        'yearsOfPriorProfessionalExperience', 'electronicMails'
    ]
    assert composite.required_fields == ['staffUniqueId', 'firstName', 'lastSurname']
    assert composite.description == 'This entity represents an individual who performs specified activities for any public or private education institution or agency that provides instructional and/or support services to students or staff at the early childhood level through high school completion. For example, this includes:    1. An "employee" who performs services under the direction of the employing institution or agency is compensated for such services by the employer and is eligible for employee benefits and wage or salary tax withholdings    2. A "contractor" or "consultant" who performs services for an agreed upon fee or an employee of a management service contracted to work on site    3. A "volunteer" who performs services on a voluntary and uncompensated basis    4. An in-kind service provider    5. An independent contractor or businessperson working at a school site.'

    extension = edfi.resource(('ed-fi-xassessment-roster', 'assessmentAdministrations'))  # Test extension and alternative argument init
    assert extension.has_deletes == True
    assert extension.fields is None  # TODO: Is this a bug?
    assert extension.required_fields is None  # TODO: Is this a bug?
    assert extension.description == 'The anticipated administration of an assessment under the purview of an EducationOrganization.'

def test_ping(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)

    resource = edfi.resource('schools')
    assert resource.ping().ok

    # Test fake resource
    fake = edfi.resource('schools2')
    with pytest.raises(requests.exceptions.HTTPError):
        fake.ping()

    deletes = edfi.resource('students', get_deletes=True)
    assert deletes.ping().ok

    descriptor = edfi.descriptor('attendance_event_category_descriptors')
    assert descriptor.ping().ok

    composite = edfi.composite('staffs')
    assert composite.ping().ok

    extension = edfi.resource(('ed-fi-xassessment-roster', 'assessmentAdministrations'))
    assert extension.ping().ok

def test_get_total_count(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)

    resource = edfi.resource('schools')
    assert resource.get_total_count(params={'minChangeVersion': 0, 'maxChangeVersion': 2000000}) == 5

    deletes = edfi.resource('students', get_deletes=True)
    assert deletes.get_total_count()

    descriptor = edfi.descriptor('attendance_event_category_descriptors')
    assert descriptor.get_total_count()

    composite = edfi.composite('staffs')
    assert composite.get_total_count()

    extension = edfi.resource(('ed-fi-xassessment-roster', 'assessmentAdministrations'))
    assert extension.get_total_count()


def test_gets(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)
    pass


def test_posts(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)
    pass


def test_deletes(secret: str, verbose: bool = False):
    credentials = easecret.get_secret(secret)
    edfi = EdFiClient(**credentials, verbose=verbose)
    pass








if __name__ == '__main__':
    OUTPUT_SECRET = "edfi_sc_cougar_2024"
    INPUT_SECRET = "edfi_eshara_test"
    VERBOSE = True

    asyncio.run(test_async_post(OUTPUT_SECRET, INPUT_SECRET, verbose=VERBOSE))

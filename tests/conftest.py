import pytest
import datetime
import pytest
import pyodbc
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from utils import constants
from azure.storage.filedatalake import DataLakeServiceClient


def pytest_addoption(parser):
    # keyvault name
    parser.addoption("--keyvault", action="store",
                     default="devpocvault")
    # synapse name
    parser.addoption("--synapse", action="store",
                     default="devpocworkspace")
    # target location to upload
    parser.addoption("--basepath", action="store",
                     default="integrationtest/sampledata")

    # Storage Account Name
    parser.addoption("--storage_account_name", action="store",
                     default="ocstorageaccount")

    parser.addoption("--container_name", action="store",
                     default="pocstorageaccount")
    # pipeline name
    parser.addoption("--pipeline", action="store",
                     default="MasterPipeline")

    # input location at landing zone
    parser.addoption("--client_id", action="store")
    # input location at landing zone
    parser.addoption("--client_secret", action="store")
    # input location at landing zone
    parser.addoption("--tenant", action="store")
    # input file name
    parser.addoption("--input_sample_filename", action="store",
                     default="employee_sample_integration_test.parquet")

@pytest.fixture()
def input_sample_file_name(pytestconfig) -> str:
    return pytestconfig.getoption("input_sample_filename")


@pytest.fixture()
def keyvaultname(pytestconfig):
    return pytestconfig.getoption("keyvault")


@pytest.fixture()
def pipeline_name(pytestconfig) -> str:
    return pytestconfig.getoption("pipeline")


@pytest.fixture()
def base_path(pytestconfig) -> str:
    return pytestconfig.getoption("basepath")


@pytest.fixture()
def storage_account_name(pytestconfig) -> str:
    return pytestconfig.getoption("storage_account_name")


@pytest.fixture()
def container_name(pytestconfig) -> str:
    return pytestconfig.getoption("container_name")


@pytest.fixture()
def file_time(pytestconfig) -> datetime:
    return datetime.datetime.now()


@pytest.fixture()
def synapse_endpoint(pytestconfig) -> str:
    synapse_name = pytestconfig.getoption("synapse")
    endpoint = f"https://{synapse_name}.dev.azuresynapse.net"
    return endpoint


@pytest.fixture()
def azure_credential(pytestconfig):
    client_id = pytestconfig.getoption("client_id")
    client_secret = pytestconfig.getoption("client_secret")
    tenant_id = pytestconfig.getoption("tenant")
    if client_id is None or client_secret is None or tenant_id is None:
        print(f"###########using default")
        credentials = DefaultAzureCredential()
        return credentials
    else:
        credentials = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id)
        return credentials


@pytest.fixture
def secretclient(azure_credential, keyvaultname) -> SecretClient:
    keyvault_Uri = f"https://{keyvaultname}.vault.azure.net"
    client = SecretClient(vault_url=keyvault_Uri, credential=azure_credential)
    return client


@pytest.fixture()
def adls_connection_client(secretclient):
    conn_str = secretclient.get_secret(
        constants.ADLS_KEYVAULT_SECRET_NAME).value
    return DataLakeServiceClient.from_connection_string(
        conn_str=conn_str)


@pytest.fixture()
def sql_connection_client(secretclient):
    """Create a Connection Object object"""
    server = secretclient.get_secret(
        constants.DEDICATED_SQL_SERVER_ENDPOINT).value
    database = secretclient.get_secret(
        constants.DEDICATED_SQL_SERVER_DATABASE).value
    username = secretclient.get_secret(
        constants.DEDICATED_SQL_SERVER_USERNAME).value
    password = secretclient.get_secret(
        constants.DEDICATED_SQL_SERVER_PASSWORD).value
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server +
                           ';DATABASE='+database+';UID='+username+';PWD=' + password)
    yield cnxn
    cnxn.close()

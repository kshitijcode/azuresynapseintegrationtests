import pytest
import json
from dataconnectors import adls
from utils import pipelineutils, constants


CLEANUP_TABLES = []
CLEANUP_FOLDERS = []
database = "default"


@pytest.fixture(autouse=True)
def run_before_and_after_tests(adls_connection_client,
                               base_path: str,
                               container_name: str,
                               azure_credential,
                               synapse_endpoint):
    yield
    print("STARTING TO CLEAN UP .....")
    adls.cleanup_ADLS_files(adls_connection_client,
                            container_name, CLEANUP_FOLDERS)
    print(CLEANUP_TABLES)
    params = {"table_name": CLEANUP_TABLES}
    pipelineutils.run_and_observe_pipeline(
        azure_credential, synapse_endpoint, constants.INTEGRATIONTEST_CLEANUP_PIPELINE, params)


def test_source_to_processed_workflow(azure_credential,
                                 synapse_endpoint: str,
                                 pipeline_name: str,
                                 storage_account_name: str,
                                 container_name: str,
                                 base_path: str,
                                 input_sample_file_name: str,
                                 adls_connection_client,
                                 sql_connection_client):

    source_to_raw_tests(azure_credential,
                        synapse_endpoint,
                        pipeline_name,
                        storage_account_name,
                        container_name,
                        base_path,
                        input_sample_file_name,
                        adls_connection_client)

    raw_to_processed_tests(azure_credential,
                      synapse_endpoint,
                      pipeline_name,
                      sql_connection_client)


def source_to_raw_tests(azure_credential,
                        synapse_endpoint: str,
                        pipeline_name: str,
                        storage_account_name: str,
                        container_name: str,
                        base_path: str,
                        input_sample_file_name: str,
                        adls_connection_client):

    print("STARTING SOURCE TO RAW...\n")
    # Arrange
    target_path = "/integrationtest_employees/raw/"
    table_raw = "integrationtest_employees"
    target_table = json.dumps(
        {"name": table_raw, "overwrite": "false", "path": target_path, "partition_by": ""})

    CLEANUP_FOLDERS.append(base_path)

    # Act
    # Uploading File to Landing Zone
    out_file_name, out_file_path = adls.upload_to_ADLS(
        adls_connection_client, container_name, base_path, input_sample_file_name)

    # Trigger the Master Pipeline for Landing to Raw Zone
    masterpipeline_raw_params = {
        "basePath": base_path,
        "filePath": "employee_*.parquet",
        "targetTable": target_table,
        "badDataTable": "bad_employees",
        "containerName": container_name,
        "archivePath": "archive",
        "storageAccountName": storage_account_name,
        "readFromSparkTables": False,
        "database": database,
    }

    print(f"{pipeline_name} Source to Raw Parameters : {masterpipeline_raw_params}\n")

    CLEANUP_FOLDERS.append(target_path.split('/')[1])
    CLEANUP_TABLES.append(table_raw)

    pipeline_run_result = pipelineutils.run_and_observe_pipeline(
        azure_credential, synapse_endpoint, pipeline_name, masterpipeline_raw_params)

    assert pipeline_run_result == constants.PIPELINE_SUCCESS_STATUS

    # Check for Data in Raw Zone
    parquet_dataframe = adls.read_parquet_file_from_ADLS(
        adls_connection_client, container_name, target_path)
    num_of_rows = len(parquet_dataframe.index)

    # Assert
    print(f"Number of Rows Fetched : { num_of_rows }\n")
    assert num_of_rows == 10


def raw_to_processed_tests(azure_credential,
                      synapse_endpoint: str,
                      pipeline_name: str,
                      sql_connection_client):

    print("STARTING PROCESSED TO processed TEST...\n")

    table_raw = "integrationtest_employees"
    table_processed = "processed_integrationtest_employee"

    # Trigger the Master Pipeline for Processed to processed Zone
    masterpipeline_processed_params = {
        "lookUpTables": [{
            "SourceTableSchemaName": "dbo",
            "SourceTableName": table_raw,
            "SinkTableSchemaName": "dbo",
            "SinkTableName": table_processed,
            "HasIncrementalData": "false"
        }],
        "sourceDatabase": database,
    }

    pipeline_name = constants.COPY_TO_DEDICATE_SQL_PIPELINE

    print(f"{pipeline_name} Parameters : {masterpipeline_processed_params}\n")

    pipeline_run_result = pipelineutils.run_and_observe_pipeline(
        azure_credential, synapse_endpoint, pipeline_name, masterpipeline_processed_params)

    assert pipeline_run_result == constants.PIPELINE_SUCCESS_STATUS

    # Check for Data in processed Zone
    cursor = sql_connection_client.cursor()
    cursor.execute(
        "SELECT COUNT(*) AS COUNT FROM [dbo].[{0}]".format(table_processed))
    row = cursor.fetchone()
    assert row is not None
    assert int(row.COUNT) == 10

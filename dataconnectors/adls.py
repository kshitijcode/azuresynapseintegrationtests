from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from utils import constants
from io import BytesIO
import pandas as pd


def upload_to_ADLS(adls_connection_client,
                   container: str, base_path: str, input_file_name: str):
    """ Upload file to ADLS

    Args:
        adls_connection_client ([type]): ADLS Connection Object
        container (str): Container Name where file needs to be uploaded
        base_path (str): Base Folder Path in ADLS where file will be uploaded
        input_file_name (str): File Name to Upload

    Returns:
        [str]: Full Path of uploaded file
    """

    print(f"STARTING TO UPLOAD FILE TO ADLS container:{container}\n")
    sample_data = f"data/{input_file_name}"

    try:

        file_system_client = adls_connection_client.get_file_system_client(
            file_system=container)
        directory_client = file_system_client.get_directory_client(base_path)
        file_client = directory_client.create_file(f"{input_file_name}")
        local_file = open(sample_data, 'rb')
        file_contents = local_file.read()
        file_client.append_data(
            data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        print(f"UPLOADED {input_file_name} to ADLS\n")
        full_path = f"{container}/{base_path}"
        return input_file_name, full_path
    except Exception as e:
        print(e)
        raise


def read_parquet_file_from_ADLS(adls_connection_client,
                                container: str, base_path: str):
    """ Upload file to ADLS

    Args:
        adls_connection_client ([type]): ADLS Connection Object
        container (str): Container Name
        base_path (str): Base Folder Path in ADLS to read parquet files from


    Returns:
        [str]: DataFrame Of Read File
    """
    try:
        file_system_client = adls_connection_client.get_file_system_client(
            file_system=container)
        if file_system_client.get_directory_client(base_path).exists():
            print(f"{base_path} exists")
            paths = file_system_client.get_paths(path=base_path)
            parent_directory_client = file_system_client. \
                get_directory_client(base_path)
            for path in paths:
                if path.name.endswith(".parquet"):
                    print("READING CONTENT FROM ADLS\n")
                    file_client = parent_directory_client.get_file_client(
                        path.name.split('/')[-1])
                    download = file_client.download_file()
                    downloaded_bytes = download.readall()
                    stream = BytesIO(downloaded_bytes)
                    processed_df = pd.read_parquet(
                        stream, engine='pyarrow')
                    return processed_df
    # TODO: If multiple parquet files are avaialble in ADS we need to tweak code for getting count
    except Exception as e:
        print(e)


def cleanup_ADLS_files(adls_connection_client, container, cleanup_folders):
    """[summary]

    Args:
        adls_connection_client ([type]): ADLS Connection Object
        container (str): Container Name
        cleanup_folders (arr): Folders to be Deleted  in ADLS
    """

    for folder in cleanup_folders:
        _delete_folder(adls_connection_client, container, folder)


def _delete_folder(datalake_service_client, container, folder):
    """[summary]

    Args:
        datalake_service_client ([type]): ADLS connection client
        container ([type]): Container Name
        folder ([type]):Folder to be deleted
    """
    try:

        print(f"DELETING FOLDER {folder} from ADLS")
        file_system_client = datalake_service_client.get_file_system_client(
            file_system=container)
        directory_client = file_system_client.get_directory_client(folder)
        directory_client.delete_directory()

    except Exception as e:
        print(f"Failed on deleting folder {container}/"
              f"{folder} with {e}")

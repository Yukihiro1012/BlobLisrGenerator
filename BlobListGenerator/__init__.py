import logging
import csv
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContainerClient


def main(mytimer: func.TimerRequest) -> None:
    # Create the BlobServiceClient object which will be used to create a container client
    connect_str = os.getenv('BlobListGeneratorInputStorage')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    containners = blob_service_client.list_containers()
    for container in containners:
        input_container_client = blob_service_client.get_container_client(container['name'])
        output_and_upload_blob_list_csv(input_container_client, container['name'])


def output_and_upload_blob_list_csv(container_client: ContainerClient, container_name: str) -> None:
    result_file_name = f"datalake_blob_list_{container_name}.csv"
    output_dir = os.getenv('local_output_dir_agent')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    local_result_file = os.path.join(output_dir, result_file_name)
    with open(local_result_file, 'w+', newline="", encoding="cp932") as f:
        writer = csv.writer(f)
        writer.writerow(['name', 'size(MB)', 'last_modified'])
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            if not blob.deleted:
                writer.writerow(
                    [blob.name, blob.size / 1024 / 1024, blob.last_modified])

    # Create the BlobServiceClient object which will be used to create a container client
    connect_str = os.getenv('BlobListGeneratorOutputStorage')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container='blob-list-generator', blob=result_file_name)
#        container='data', blob=result_file_name)

    # Upload the created file
    with open(local_result_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
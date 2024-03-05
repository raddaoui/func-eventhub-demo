import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os, io
from PIL import Image
from io import BytesIO

# transform image from png to jpg
def transform_image(img_png_bytes):
    png_image = Image.open(io.BytesIO(img_png_bytes))
    jpg_stream = BytesIO()
    png_image.convert('RGB').save(jpg_stream, format='JPEG')
    jpg_stream.seek(0)
    return jpg_stream



app = func.FunctionApp()


@app.blob_trigger(arg_name="myblob", path="rawimages",connection="SAIN_CONN") 
@app.event_hub_output(arg_name="event", event_hub_name="testeventhub", connection="alaeh1_mydevpolicy_EVENTHUB")
def BlobTrigger(myblob: func.InputStream, event: func.Out[str]):
    logging.info(f">> Python blob trigger function processed blob"
                f">> Name: {myblob.name}"
                f">> Blob Size: {myblob.length} bytes")
    event.set(myblob.name)
    
                 

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="testeventhub",
                               connection="alaeh1_mydevpolicy_EVENTHUB", consumer_group="$Default")
def EventHubTrigger(azeventhub: func.EventHubEvent):

    logging.info('>> Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))


    # get input storage account connection string
    input_connect_str = os.getenv('SAIN_CONN')
    # Get the blob path from the event
    blob_path = azeventhub.get_body().decode('utf-8')
    # Split the blob path by "/" and get the container name and blob name
    parts = blob_path.split("/")
    input_container_name = parts[0]
    input_blob_name = "/".join(parts[1:])
    # Create the input BlobServiceClient object
    input_blob_service_client = BlobServiceClient.from_connection_string(input_connect_str)
    input_container_client = input_blob_service_client.get_container_client(container=input_container_name)
    # Download the PNG image as bytes
    input_blob_data = input_container_client.download_blob(input_blob_name).readall()
    logging.info(">> Successfully Read input blob data")

    
    # Transform the PNG image to a JPG image
    jpg_stream = transform_image(input_blob_data)

    
    # get output Azure Storage Account connection string
    output_connect_str = os.getenv('SAOUT_CONN')
    # we will store the processed images in a container called processedimages
    output_container_name = 'processedimages'
    # remove the file extension from the input blob name and replace it with jpg
    output_blob_name = input_blob_name.split('.')[0] + '.jpg'
    logging.info(">> Output_blob_path: " + output_container_name + "/" + output_blob_name)
    # Create the output BlobServiceClient object
    output_blob_service_client = BlobServiceClient.from_connection_string(output_connect_str)
    output_blob_client = output_blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)
    # Upload the transformed image to the output container
    output_blob_client.upload_blob(data=jpg_stream)
    logging.info(">> Successfully Stored transformed blob data")
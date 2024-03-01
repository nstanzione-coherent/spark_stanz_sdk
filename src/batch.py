import requests
import json


## SET CONSTANTS
spark_bearer_token = ''
spark_service = 'INSERT Folder/Service'
spark_service_version = 'INSERT Service Verison ID'
tenant = 'INSERT tenant name'
environment = 'uat.us'

url_batch = f"https://excel.{environment}.coherent.global/{tenant}/api/v4/batch"


def create_batch():
    """
    Helper to both create the batch and send input data
    Returns batch_id to be used in downstream functions
    
    """
    create_response = requests.post(url_batch, headers=headers_batch, json=payload_batch)
    cr = create_response.json()
    batch_id = cr['id']
    
    return batch_id, cr


def send_batch_inputs(batch_id, inputs):
    """
    Helper to send input data
    Returns records submitted to be used in downstream functions
    
    """
    input_url = f'{url_batch}/{batch_id}/data'
    input_payload = {
        'inputs': inputs
    }
    input_response = requests.post(input_url, headers=headers_batch, json=input_payload)
    ir = input_response.json()
    
    return ir


def get_batch_status(batch_id):
    """
    Helper to grab the current status of the batch
    
    """
    status_url = f'{url_batch}/{batch_id}/status'
    status_response = requests.get(status_url, headers=headers_batch)
    sr = status_response.json()
    
    return sr


def get_batch_results(batch_id):
    """
    Helper to grab the outputs from the batch run
    
    """
    output_url = f'{url_batch}/{batch_id}/result'
    output_response = requests.get(output_url, headers=headers_batch, params={'max':1000})
    out = output_response.json()
    count = out['count']
    outputs = out['outputs']
    
    return count, outputs, out


def add_chunks(batch_id, chunks):
    """
    Helper to send chunks
    Returns api response to be used to validate properly submitted chunk
    
    """
    chunk_url = f'{url_batch}/{batch_id}/chunks'
    chunk_payload = {
        'chunks': chunks
    }
    chunk_response = requests.post(chunk_url, headers=headers_batch, json=chunk_payload)
    cr = chunk_response.json()
    
    return cr


def get_chunk_results(batch_id, max_results: int = 100):
    """
    Helper to retrieve chunk results
    Returns records submitted to be used in downstream functions
    
    """
    chunk_url = f'{url_batch}/{batch_id}/chunksresults?max={max_results}'
    chunk_response = requests.get(chunk_url, headers=headers_batch)
    cr = chunk_response.json()
    
    return cr


def close_batch(batch_id):
    """
    Helper to close the batch
    
    """
    close_url = f'{url_batch}/{batch_id}/'
    close = {"batch_status" : "closed"}
    close_response = requests.patch(close_url, headers=headers_batch, json=close)
    cr = close_response.json()
    status = cr['batch_status']
    
    return status
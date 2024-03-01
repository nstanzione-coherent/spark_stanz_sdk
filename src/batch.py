import requests


## SET CONSTANTS
spark_bearer_token = ''
spark_service = 'INSERT Folder/Service'
spark_service_version = 'INSERT Service Verison ID'
tenant = 'INSERT tenant name'
environment = 'uat.us'


## DEFINE VARIABLES 
url_batch = f"https://excel.{environment}.coherent.global/{tenant}/api/v4/batch"

headers_batch = {
    'Content-Type': 'application/json',
    'Authorization': spark_bearer_token
}

payload_batch = {
    'service_uri': spark_service, 
    'version_id': spark_service_version, 
    'source_system': 'batch_source', 
    'call_purpose': 'batch_api'
}


## FUNCTIONS
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
import pandas as pd
import numpy as np
import aiohttp
import asyncio
import json


def pd_to_arr(df, flag=0):
    """
    Helper to transform dataframe into V4 format
    Specific function to inner.py

    Note the specificity is around the position (last column) of the scenario in the dataframe
    Future imporvement: Make the position of the json columns as an input array into the function 
    
    """
    output = [] # Initialize array output
    cols = df.columns.tolist() # Gather columns from dataframe
    output.append(cols)
    for i in range(len(df)): # Loop through records of dataframe
        rec = df.iloc[i].tolist() # Convert current row elements into a list
        if flag == -1: # Specifc for GenerateInners.py. Converts the first element in row into an embedded array.
            yc = rec.pop(0) # Grab the first element of the list
            js_yc = json.loads(yc)
            y = json_to_arr(js_yc)
            rec.insert(0, y)           
        if flag == 1: # Specifc for inner.py. Converts the last element in row into an embedded array.
            scen = rec.pop() # Grab the last element of the list
            js = json.loads(scen)
            x = json_to_arr(js)
            rec.append(x)
        output.append(rec)   
    
    return output

# Get JSON into V4 format
def json_to_arr(js):
    """
    Helper to transform JSON into V4 format
    
    """
    output = []
    cols = list(js[0].keys())
    output.append(cols)
    for i in range(len(js)):
        rec = []
        dict = js[i]
        keys = dict.keys()
        for k in keys:
            val = dict[k]
            rec.append(val)
        output.append(rec)
    
    return output

def to_json(inputs, request_meta):
    """
     Turn dataframe into JSON data structure
    
    """
    data_js = inputs.to_json(orient='records')
    data_ls = eval(data_js)
    #Create array of JSON requests
    req = []
    for i in range(len(inputs)):
        request_data = {}
        y = data_ls[i]
        request_data['inputs'] = y
        request = {
            'request_data': request_data,
            'request_meta': request_meta
        }
        req.append(request)
    return req



def calc_cte(array, percentile, tail='left'):
    """
    Helper to caluclate Conditional Tail Expectation (CTE) of a list of values
    
    This function takes an array, percentile integer, and tail as an input
    The list and percentile are self-explanatory
    The tail input is used to understand the direction of the tail:
        'left' signifies a left tail or that the goal is to find the expectation of minimum values
        'right' signifies a right tail or the goal us the expectation of maximum values
    
    Returns the expected value of the tail specified.

    Default: Left tail since we are concerned with negative values. 
    For instance, for this use case of 'left' CTE70, we want the average of the smallest 30% of values.
    
    """
    array.sort()
    if tail == 'left':
        pct = (100-percentile)
    else:
        pct = percentile
    index = int(len(array)*(pct/100))
    smallest = array[:index]
    cte = np.mean(smallest)

    return cte

#aSync Functionality
async def spark_call(url, headers, data_list):
    async with aiohttp.ClientSession() as session:
        tasks =[session.post(url, headers=headers, json=data) for data in data_list]
        responses = await asyncio.gather(*tasks)
        results = []
        for resp in responses:
            results.append(await resp.json())
        return results


def write_results(x):
    #Write results to array of JSON repsonses
    data = ['LIST OF SCALAR OUTPUTS']
    meta = ['call_id']
    results = []
    proj = []
    for i in range(len(x)):
        arr = {}
        #Generate outputs from xoutputs
        for d in data:
            arr[d] = x[i]['response_data']['outputs'][d]
        #Log necessary meta data
        for m in meta:
            arr[m] = x[i]['response_meta'][m]
        # Append to results array                   
        results.append(arr)   
        
        #Tabular output handling
        tbl = x[i]['response_data']['outputs']['TBL OUTPUT']
        tbl = pd.json_normalize(tbl)
        proj.append(tbl)

    return results, proj




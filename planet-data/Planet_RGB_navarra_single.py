#python Planet/split_roi.py
#adapted from https://github.com/planetlabs/training-workshop/blob/master/OrdersAPI/ordering_and_delivery.ipynb

#import asyncio
import os
import json
import pathlib
import time
import re

import requests
from requests.auth import HTTPBasicAuth
import json
import timeit
start = timeit.timeit()

input_file=open('Planet/download-grid_ESP_NAV/grid.geojson', 'r')

json_decode=json.load(input_file)
square = json_decode['features'][3]['properties']['name']
print(square)

ROI = json_decode['features'][3]['geometry']['coordinates']


API_KEY = '' #Enter API key here

auth = HTTPBasicAuth(API_KEY, '')

orders_url = 'https://api.planet.com/compute/ops/orders/v2'


# set content type to json
headers = {'content-type': 'application/json'}

request = {
  "name":"Navarra",
  "source_type":"basemaps",
  "products":[
    {
      "mosaic_name":"global_monthly_2021_04_mosaic",
      "geometry":{
        "type":
    "Polygon",
    "coordinates": ROI
      }
    }
  ],
  "tools":[
    {
      "clip":{
  
      }
    },
    {
      "merge":{
       
      }
    }#,
    #{
    #  "bandmath":{
    #    "b1": "b1",
    #    "b2": "b2",
    #    "b3": "b3",
    #  }
    
 #}
  ]
}



def place_order(request, auth):
    response = requests.post(orders_url, data=json.dumps(request), auth=auth, headers=headers)
    if response.status_code > 299:
        print(response.text)
    #print(response.json())
    order_id = response.json()['id']
    #print(order_id)
    order_url = orders_url + '/' + order_id
    return order_url
    return(response.json())

order_url = place_order(request, auth)

def poll_for_success(order_url, auth, num_loops=30):
    count = 0
    while(count < num_loops):
        count += 1
        r = requests.get(order_url, auth=auth)
        response = r.json()
        state = response['state']
        #print(state)
        end_states = ['success', 'failed', 'partial']
        if state in end_states:
            print(state)
            break
        time.sleep(5)
        
poll_for_success(order_url, auth)        
poll_for_success(order_url, auth)

time.sleep(15)
r = requests.get(order_url, auth=auth)
response = r.json()
results = response['_links']['results']
#print(results)

def download_results(results, overwrite=False):
    results_urls = [r['location'] for r in results]
    results_names = [r['name'] for r in results]
    print('{} items to download'.format(len(results_urls)))
    print(results_names)

    #path = pathlib.Path(os.path.join('Planet','Data', square, square+'.tif'))
        
    #print('downloading {} to {}'.format(results_names[4], path))
    #r = requests.get(results_urls[4], allow_redirects=True)
    #path.parent.mkdir(parents=True, exist_ok=True)
    #open(path, 'wb').write(r.content)

    
    for url, name in zip(results_urls, results_names):
        path = pathlib.Path(os.path.join('Planet','Data', square, re.search('[^\/]+$', name).group(0)))
        
        if overwrite or not path.exists():
            print('downloading {} to {}'.format(name, path))
            r = requests.get(url, allow_redirects=True)
            path.parent.mkdir(parents=True, exist_ok=True)
            open(path, 'wb').write(r.content)
        else:
            print('{} already exists, skipping {}'.format(path, square))

download_results(results)

end = timeit.timeit()
print(end - start)


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
from planet import api
import click

def call_and_wrap(func, *args, **kw):
    '''call the provided function and wrap any API exception with a click
    exception. this means no stack trace is visible to the user but instead
    a (hopefully) nice message is provided.
    note: could be a decorator but didn't play well with click
    '''
    try:
        return func(*args, **kw)
    except api.exceptions.APIException as ex:
        click_exception(ex)

pretty = click.option('-pp/-r', '--pretty/--no-pretty', default=None,
                      is_flag=True, help='Format JSON output')

def echo_json_response(response, pretty, limit=None, ndjson=False):
    '''Wrapper to echo JSON with optional 'pretty' printing. If pretty is not
    provided explicity and stdout is a terminal (and not redirected or piped),
    the default will be to indent and sort keys'''
    indent = None
    sort_keys = False
    nl = False
    if not ndjson and (pretty or (pretty is None and sys.stdout.isatty())):
        indent = 2
        sort_keys = True
        nl = True
    try:
        if ndjson and hasattr(response, 'items_iter'):
            items = response.items_iter(limit)
            for item in items:
                click.echo(json.dumps(item))
        elif not ndjson and hasattr(response, 'json_encode'):
            response.json_encode(click.get_text_stream('stdout'), limit=limit,
                                 indent=indent, sort_keys=sort_keys)
        else:
            res = response.get_raw()
            if len(res) == 0:  # if the body is empty, just return the status
                click.echo("status: {}".format(response.response.status_code))
            else:
                res = json.dumps(json.loads(res), indent=indent,
                                 sort_keys=sort_keys)
                click.echo(res)
            if nl:
                click.echo()
    except IOError as ioe:
        # hide scary looking broken pipe stack traces
        raise click.ClickException(str(ioe))




input_file=open('Planet/download-grid_ESP_NAV/grid.geojson', 'r')

json_decode=json.load(input_file)
square = json_decode['features'][0]['properties']['name']
print(square)

ROI = json_decode['features'][0]['geometry']['coordinates']
#print(ROI[0]) #[[[-2.137801154737411, 41.91165227563432], [-2.133894198572697, 42.19982998708105], [-1.746365396301348, 42.19624920108312], [-1.752019536800884, 41.908107278433114], [-2.137801154737411, 41.91165227563432]]]

API_KEY = '' #Enter API key here


auth = HTTPBasicAuth(API_KEY, '')

orders_url = 'https://api.planet.com/compute/ops/orders/v2'

client = api.ClientV1('') #after entering in cmd line: export PL_API_KEY 'bla'

#print(client.get_mosaic_by_name("global_monthly_2021_04_mosaic"))

mosaic, = client.get_mosaic_by_name("global_monthly_2021_05_mosaic").items_iter(1)
#print(mosaic)

quads = client.get_quads(mosaic, (-2.137801154737411, 41.908107278433114, -1.746365396301348, 42.19982998708105)).get()
#print(quads['items'][0]['_links']['items'])
print(len(quads['items']))

#scenes = client.get_quad_contributions(quads)
scenes = call_and_wrap(client.get_quad_contributions, quads['items'][0])
#echo_json_response(scenes, pretty)
#print(scenes)
#get each item after the slash and before the hash

# set content type to json
headers = {'content-type': 'application/json'}

request = {
  "name": "RGBNIR_order",
  "products": [
    {
      "item_ids": [
        "20210508_110308_07_2401" #"20210508_100950_89_245", 
      ],
      "item_type": "PSScene",
      "product_bundle": "analytic_udm2"
    }
  ],
  "tools": [
#   {
#      "clip":{"aoi": {"type": "Polygon", "coordinates": ROI }  } },
#{
#  "merge":{
#       
#  }
#},
{
  "bandmath":{
    "b1": "b1",
    "b2": "b2",
    "b3": "b3",
    "b4": "b4",
  }
}

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


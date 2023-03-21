#!! Limitations: Throws out a lot of data where time periods have more than 5% "black fill". 
#!! Instances where no suitable imagery available will halt workflow
#!! Activation will take 20 mins before download starts. Revise to only activate and then have another script to poll and download later
#!! Using depreciated v1 item type
#!! UDM2 does not identify black fill - :(
import os
import csv
import json
import requests
from requests.auth import HTTPBasicAuth
import datetime
import time
from operator import itemgetter
from PIL import Image
import geojson
import rasterio
from planet.api import filters


# Define API key and base URL
PLANET_API_KEY = ""
BASE_URL = "https://api.planet.com/data/v1"

auth = HTTPBasicAuth(PLANET_API_KEY, '')

# Define dates for the three months
start_dates = [
    datetime.date(year=2022, month=5, day=10),
    datetime.date(year=2022, month=6, day=10),
    datetime.date(year=2022, month=7, day=10)#,
    #datetime.date(year=2022, month=8, day=10)
]
end_dates = [
    datetime.date(year=2022, month=5, day=20),
    datetime.date(year=2022, month=6, day=20),
    datetime.date(year=2022, month=7, day=20)#,
    #datetime.date(year=2022, month=8, day=20)
]


# Load geometries from JSON file
with open('Planet/download-grid_ESP_NAV/grid.geojson') as f:
    geometries = json.load(f)["features"]

roi_id = range(0, len(geometries))

# Initialize list to keep track of results
results = []

# Iterate over geometries and download images
for roi_id, geometry in enumerate(geometries[:2]):
    roi_name = geometry["properties"]["name"]
    roi_directory = os.path.join("Planet/Data", roi_name)
    if not os.path.exists(roi_directory):
        os.makedirs(roi_directory)


    # Iterate over the three months
    missing_periods = []
    for i in range(3):
        # get images that overlap with our AOI 
        geometry_filter = {
          "type": "GeometryFilter",
          "field_name": "geometry",
          "config": 
                {
                "type":
                "Polygon",
                "coordinates": geometry["geometry"]["coordinates"]}
        }

        # get images acquired within a date range
        date_range_filter = {
          "type": "DateRangeFilter",
          "field_name": "acquired",
          "config": {
            "gte": start_dates[i].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "lte": end_dates[i].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
          }
        }

        # only get images which have <10% cloud coverage
        cloud_cover_filter = {
          "type": "RangeFilter",
          "field_name": "cloud_cover",
          "config": {
            "lte": 0.5
          }
        }
        

        # Set up filter for ortho tile assets
        #ortho_filter = {
        #"type": "RangeFilter", 
        #"field_name": "quality_category",
        #"config": { "eq": "standard"}
        #}


        # combine our geo, date, cloud filters
        combined_filter = {
          "type": "AndFilter",
          "config": [geometry_filter, date_range_filter, cloud_cover_filter]#, ortho_filter]
        }

        search_params = {
          #"name": "Ortho", #needed for v2
          "item_types": ["PSOrthoTile"], #["PSScene"], 
          "filter": combined_filter
        }
        #print(search_params)

        
        # Update search parameters with the dates for the current month
        #search_params["date_from"] = start_dates[i].strftime("%Y-%m-%d")
        #search_params["date_to"] = end_dates[i].strftime("%Y-%m-%d")

        # Create search request
        search_url = "{}/quick-search".format(BASE_URL) #'https://api.planet.com/compute/ops/orders/v2' # 
        search_request = requests.post(
            search_url,
            auth=auth,
            json=search_params, headers= {'content-type': 'application/json'}
        )

        print(search_request.status_code)

        # Check if the search request was successful
        if search_request.status_code == 200:
            
            results_json = search_request.json()["features"]
            #print(results_json)

            order_id = results_json[0]['id']
            order_url = BASE_URL + '/' + order_id
            #print(order_url)

            #print(results_json)
            if len(results_json) > 0:
                #print("OK2")
                time.sleep(5)

                r = requests.get(order_url, auth=auth)
                print(r)
                response = r#.json()

                # Sort the results by cloud cover
                results_json_sorted = sorted(results_json, key=lambda d: d["properties"]["cloud_cover"])#, key=itemgetter("properties","cloud_cover"))

                black_fill_threshold = 0.05 #Needs to be close to 0 otherwise there will be inconsistencies between time periods. Risks missing obs.

                # Filter results based on cloud cover threshold
                results_json_sorted = [item for item in results_json_sorted if item["properties"]["black_fill"] <= black_fill_threshold]

                # Download the lowest cloud cover image
                result = results_json_sorted[0]
                #print(result)
                asset_url = "{}?item_type=PSOrthoTile".format(result["_links"]["assets"])
                asset_request = requests.get(
                    asset_url,
                    auth=(PLANET_API_KEY, "")
                )
                #print(asset_request.json())
                asset = asset_request.json()["analytic"]
                print(asset)

                activate = requests.get(
                    asset["_links"]["activate"], 
                    auth=auth
                )


                active = None
                while active is None or active != "active":
                    time.sleep(10)
                    response = requests.get(asset["_links"]["_self"], auth=auth)
                    active = json.loads(response.content)["status"]
                    #print(active)

                download_url = asset["location"]

                # Download the multispectral image
                #print(download_url)
                download_request = requests.get(
                    download_url, allow_redirects=True#, 
                    #auth=auth
                )
                

                # Save the image to a file
                filename = "{}_{}.tif".format(start_dates[i].strftime("%Y-%m-%d"),result["id"])
                #download_results(download_url, roi_directory)
                filepath = os.path.join(roi_directory, filename)
                if not os.path.exists(filepath):
                  with open(filepath, "wb") as f:
                     f.write(download_request.content)
                else:
                  print('Already exists, skipping')

                #print(download_request.content)


                # Download the UDM2 product for cloud and shadow masks
                asset = asset_request.json()["udm2"]

                activate = requests.get(
                    asset["_links"]["activate"], 
                    auth=auth
                )



                active = None
                while active is None or active != "active":
                    time.sleep(10)
                    response = requests.get(asset["_links"]["_self"], auth=auth)
                    active = json.loads(response.content)["status"]
                    print(active)

                download_url = asset['location']#["_links"]["_self"] #['results']

                download_request = requests.get(
                    download_url, allow_redirects=True, 
                    auth=auth
                )

                # Save the image to a file
                filename = "udm2_{}_{}.tif".format(start_dates[i].strftime("%Y-%m-%d"), result["id"])
                #download_results(download_url, roi_directory)
                filepath = os.path.join(roi_directory, filename)
                if not os.path.exists(filepath):
                  with open(filepath, "wb") as f:
                     f.write(download_request.content)
                else:
                  print('Already exists, skipping')

                

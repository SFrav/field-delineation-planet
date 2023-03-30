import os
import json
import pathlib
import requests
from requests.auth import HTTPBasicAuth
import json

def get_contributing_scenes_for_basemap_region(basemap, region, name, API_KEY, path, save=False):
    '''
    Gets contributing scenes metadata for a defined region in a basemap.
    
    :param string basemap: The basemap name or ID.
    :param string region: The area of interest or region geometry. 
                          Can be a dict containing a GeoJSON geometry or a GeoJSON Feature.
    :param bool save: Whether or not to save an output GeoJSON file.
                      By default, does not save an output file.
    :returns dict: A GeoJSON FeatureCollection dict with contributing scenes
    '''
    #API_KEY = '' #Enter API key here
    # Create a session for our API requests

    s = requests.Session()
    s.auth = HTTPBasicAuth(API_KEY, '')
    
    BASE_URL = 'https://api.planet.com/basemaps/v1/mosaics/'
    
    # Use the mosaic name to retrieve the basemap
    res = s.get(url=BASE_URL, params={'name__is': basemap}).json() 

    if len(res['mosaics']) >= 1:
        # Get the first result from the response, this should be our basemap
        mosaic = res['mosaics'][0]

    else:
        # Use a mosaic id to retrieve the basemap
        mosaic = s.get(url=BASE_URL + basemap).json()
        
        if 'message' in mosaic:
            print(mosaic['message'])
            return
    
    print('Mosaic Name: {}\n  Mosaic Id: {}\n   API Link: {}'.format(mosaic['name'], mosaic['id'], mosaic['_links']['_self']))
   
    # Get the Geometry, if region is a string, open a file
    if isinstance(region, str):
        with open(region, 'r') as f:
            aoi = json.load(f)['geometry']
    elif isinstance(region, dict):
        aoi = region
    else:
        print("Please provide a valid region (JSON dict or path to GeoJSON file)!")
        return
    
    # Construct the quad search url and pass our aoi as the json body to the POST request
    # https://api.planet.com/basemaps/v1/mosaics/BASEMAP_ID/quads/search
    quads_res = s.post(url=BASE_URL + mosaic['id'] + '/quads/search', json=aoi).json()

    # The "items" list in the response contains the list on intersected quads
    quads = quads_res['items']
    
    # Handle pagination (when there are many quad results... default page_size is 50 items per page)
    next_page = quads_res["_links"].get("_next")
    while next_page:
        paged_res = s.get(url=next_page).json()
        quads = quads + paged_res['items']
        next_page = paged_res["_links"].get("_next")

    print("\nFound {} quads that intersect region.".format(len(quads)))
    
    # Setup a GeoJSON FeatureCollection to contain all contributing scenes
    contributing_scenes = {
      "type": "FeatureCollection",
      "features": []
    }
    
    # Get all contributing scenes for each quad
    # Loop through our intersecting quads
    for quad in quads:

        # Get the contributing scenes links for each quad
        contrib_scenes_url = quad['_links']['items']
        contrib_scenes_links = requests.get(url=contrib_scenes_url).json()['items']

        # Get each of the contributing scenes' metadata
        for link in contrib_scenes_links:
            scene_metadata = s.get(url=link['link']).json()
            
            # Add each scene metadata to the FeatureCollection
            contributing_scenes['features'].append(scene_metadata)
    
    print("Found {} contributing scenes".format(len(contributing_scenes['features'])))
    
    if save:
        # Save the contributing scenes list to a file
        filename = path + name + mosaic['name'] + '-contrib-scenes.geojson'
        with open(filename, 'w') as outfile:
            json.dump(contributing_scenes, outfile)
        print("Saved contributing scenes to file: {}".format(filename))
    else:
        # Report output
        print('\nContributing Scenes GeoJSON FeatureCollection:')
        
    return
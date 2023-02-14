import argparse
import numpy as np
import geojson
import fiona
from fiona.crs import from_epsg
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

  
def load_geojson(geojson_file, crs):
    data = gpd.read_file(geojson_file)
    data.crs = {"init": "epsg:{}".format(crs)}
    data = data.to_crs(epsg=crs)
    #print(data.total_bounds)
    #export reprojected data as roi
    #data.to_file(driver = 'GeoJSON', filename= output_file+'_'+roi+'/roi.geojson')

    return data

def get_extent(data):
    return data.total_bounds


def get_polygons(extent, length, overlap, roi):
    polygons = []
    k = 0
    for i in range(int(np.ceil((extent[3]-extent[1])/(length-overlap)))): 
        for j in range(int(np.ceil((extent[2]-extent[0])/(length-overlap)))): 
            polygons.append({
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [extent[0]+j*length-overlap, extent[1]+i*length-overlap],
                            [extent[0]+j*length+length+overlap, extent[1]+i*length-overlap],
                            [extent[0]+j*length+length+overlap, extent[1]+i*length+length+overlap],
                            [extent[0]+j*length-overlap, extent[1]+i*length+length+overlap]
                        ]
                    ]
                },
                "properties": {
                    "id": k,
                    "name": roi + str(i) + str(j)
                }
            })
            k += 1
    return {
        "type": "FeatureCollection",
        "features": polygons
    }



def export_vector_data(data, crs, output_file):
    my_schema = {
        "geometry": "Polygon",
        "properties": {
            "id": "int",
            "name": "str"
        }
    }
    with fiona.open(output_file, "w", "ESRI Shapefile", my_schema, crs=from_epsg(crs)) as output: #4326
        for feature in data['features']:
            #use shapely intersection here instead of geopandas. Also reset index and polygon names
            output.write(feature)




def plot_vector_layers(data1, data2, crs, output_file):
    data1 = gpd.read_file(data1)
    data1 = data1.to_crs(epsg=crs)
    data2 = gpd.read_file(data2)
    #print(data1.total_bounds)
    #print(data2.total_bounds)
    fig, ax = plt.subplots(1, figsize=(10,10))
    data1.plot(ax=ax,color='red')
    data2.plot(ax=ax,color='blue', alpha=0.5)
    #plt.savefig(output_file)
    plt.show()


#define main function to run script 
def main(geojson_file, crs, output_file, length, overlap, roi, output_fig):
    data = load_geojson(geojson_file, crs)
    extent = get_extent(data)

    polygons = get_polygons(extent, length, overlap, roi)
    
    ##Remove squares outside the roi
    #!how to remove this intermediate export step?
    export_vector_data(polygons, crs, output_file+'_'+roi)
    
    #polygons = pd.DataFrame.from_dict(polygons, orient='index').reset_index()
    #print(polygons)
    #polygons.columns = ['id','geometry']
    #convert polygon dataframe to geopandas
    #polygons = gpd.GeoDataFrame(polygons, geometry='geometry', crs="EPSG:3346")


    polygons = gpd.read_file(output_file+'_'+roi)
    #check if there are multiple polygons in data and union if there are
    if len(data['geometry']) > 1:
        data = data.unary_union

    polygons = polygons[polygons.intersects(data.geometry.iloc[0])]

    polygons['id'] = range(len(polygons))
    polygons.reset_index(drop=True, inplace=True)
    #Replace name in polygon with a concatinated string of roi and id
    polygons['name'] = roi + polygons['id'].astype(str)
    #print(polygons.name)

    polygons = polygons.to_crs(epsg=4326)

    ##Exports
    #polygons.to_file(driver = 'ESRI Shapefile', filename= output_file+'_'+roi)
    ##export polygons to gpkg file

    polygons.to_file(driver = 'GeoJSON', filename= output_file+'_'+roi+'/grid.geojson')

    plot_vector_layers(geojson_file, output_file+'_'+roi, crs, output_fig)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Divide vector data into polygons of 11000 x 11000 meters and an overlap of 500 meters")
    #parser.add_argument("--geojson_file", type=str, help="Path to geojson file", required=True)
    #parser.add_argument("--output_file", type=str, help="Path to output file", required=True)
    #parser.add_argument("--length", type=int, help="Length of each polygon", required=True)
    #parser.add_argument("--overlap", type=int, help="Overlap of each polygon", required=True)
    #args = parser.parse_args()
    #main(args.geojson_file, args.output_file, args.length, args.overlap)

    main('Planet/Navarra_spain.geojson', 32630, 'Planet/download-grid', 31000, 500, 'ESP_NAV', 'fig.jpg') #31000 = 9129px #ROI in ISO_3166-1_alpha-3 format or 3 letter addition for sub national
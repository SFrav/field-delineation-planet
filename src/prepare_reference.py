import argparse
import numpy as np
import fiona
from shapely.geometry import shape
#from shapely.validation import is_valid
from shapely.validation import make_valid
import matplotlib.pyplot as plt
import pprint


def filter_reference_data(file_path, file, LU_col, out_file, area_threshold, ratio_threshold, land_use):
    #Open the file using fiona.
    with fiona.open(file_path + file) as f:
        
        #pprint.pprint(f.schema)
        #!print(np.unique(f[LU_col]))

        with fiona.open('input-data/' + out_file, 'w', driver = 'GPKG', crs = f.crs, schema = f.schema) as output:
            if land_use != "all":
                for feature in f:
                        if feature['properties'][LU_col] in land_use:
                            area = np.array([shape(feature['geometry']).area])
                            perimeter = np.array([shape(feature['geometry']).length])
                            ratio = area/perimeter
                            #valid = shape(feature['geometry']).is_valid #@try to make valid polygons?
                            #if not valid:
                            #    feature['geometry'] = make_valid(feature['geometry'])

                            if area > area_threshold and ratio > ratio_threshold: # and valid:
                                output.write(feature)

            else:
                for i in range(feature.size):
                    area = np.array([shape(feature['geometry']).area])
                    perimeter = np.array([shape(feature['geometry']).length])
                    ratio = area/perimeter
                    #valid = shape(feature['geometry']).is_valid
                    if area > area_threshold and ratio > ratio_threshold: # and valid:
                        output.write(feature)



if __name__ == "__main__":

#    parser = argparse.ArgumentParser(description = "Prepare reference data for use in the model.")

#    parser.add_argument("-f", "--file_path", help = "Path to the folder containing the reference data.", required = True)
#    parser.add_argument("-o", "--output_path", help = "Path to the folder to save the prepared reference data.", required = True)
#    parser.add_argument("-l", "--land_use", help = "Land use to filter by. Default is crops.", default = ["crops"])
#    parser.add_argument("-a", "--area_threshold", help = "Area threshold in m2 - whole number)
#    parser.add_argument("-r", "--ratio_threshold", help = "Ratio threshold. Remove slithers. Default is 4.")
#    parser.add_argument("-c", "--column_name", help = "Column name to filter by. Default is LU_Col.", default = "LU_Col")

#    args = parser.parse_args()
    LU = ["TIERRA ARABLE", "PASTIZAL", "PASTO ARBUSTIVO", "PASTO CON ARBOLADO", "HUERTA", "OLIVAR", "FRUTAL", "VIÑEDO", "FRUTOS SECOS", "FRUTOS SECOS Y OLIVAR", "ASOC. FRUTAL-FRUTOS SECOS", "ASOC. OLIVAR-FRUTAL", "ASOC. OLIVAR-VIÑEDO", "ASOC. FRUTAL-VIÑEDO", "VIÑA-FRUTOS SECOS", "ZONA CONCENTRADA"]
    filter_reference_data('reference-data/', 'SIGPAC_Pol_Recinto.shp', 'USO21', 'fields_pasture.gpkg', area_threshold=2, ratio_threshold=4, land_use = LU)
    
    #Navarra Spain, all = ["TIERRA ARABLE", "PASTIZAL", "PASTO ARBUSTIVO", "PASTO CON ARBOLADO", "HUERTA", "OLIVAR", "FRUTAL", "VIÑEDO", "FRUTOS SECOS", "INVERNADEROS Y CULTIVOS BAJO PLÁSTICO", "FRUTOS SECOS Y OLIVAR", "ASOC. FRUTAL-FRUTOS SECOS", "ASOC. OLIVAR-FRUTAL", "ASOC. OLIVAR-VIÑEDO", "ASOC. FRUTAL-VIÑEDO", "VIÑA-FRUTOS SECOS", "ZONA CONCENTRADA"]
    






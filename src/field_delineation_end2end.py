import csv
import logging
import json
import os
import pickle
from distutils.dir_util import copy_tree
import glob

import numpy as np
import geopandas as gpd
os.environ['MPLCONFIGDIR'] = os.getcwd() + "/configs/"
import matplotlib.pyplot as plt
import pandas as pd
import fiona
from fiona.crs import from_epsg
from shapely.geometry import mapping
from shapely.wkt import loads

from eolearn.core import EOPatch
from sentinelhub import SHConfig
from shapely.geometry import Polygon
from fs.copy import copy_file
from fd.scripts.download import batch_download
from fd.scripts.tiffs_to_eopatches import convert_tiff_to_eopatches
from fd.scripts.vector_to_raster import rasterise_gsaa
from fd.scripts.sampling import sample_patchlets
from fd.scripts.patchlets_to_npz import patchlets_to_npz_files
from fd.scripts.normalization import calculate_normalization_factors
from fd.scripts.k_folds_split import k_fold_split
from fd.scripts.train import train_k_folds
from fd.scripts.predict import run_prediction
from fd.scripts.postprocessing import run_post_processing
from fd.scripts.vectorization import vectorise
from fd.scripts.utm_zone_merging import merge_zones
from fd.utils import BaseConfig, prepare_filesystem
from fd.utils_plot import (draw_vector_timeless,
                           draw_true_color,
                           draw_bbox,
                           draw_mask,
                           get_extent
                           )


def write_patchlets_extents_to_shapefile(path_to_patchlets, out_shp_fname, patchlets_crs=32630):
    """
        for data validation purposes dump to a shapefile the extent of each of the patchlets sampled from the patches
    """

    my_schema = {
        "geometry": "Polygon",
        "properties": {
            "id": "int",
            "patchlet": "str"
        }
    }

    my_driver = "ESRI Shapefile"
    my_crs = from_epsg(patchlets_crs)

    if os.path.exists(path_to_patchlets):
        with fiona.open(out_shp_fname, "w", driver=my_driver, crs=my_crs, schema=my_schema) as my_collection:
            id = 1
            for root, folder, files in os.walk(path_to_patchlets):
                for fn in files:
                    if fn == 'bbox.pkl':
                        pth_to_patchlet_bbox_pkl_fn = os.path.join(root, fn)
                        patchlet_name = os.path.split(root)[-1]
                        with open(pth_to_patchlet_bbox_pkl_fn, 'rb') as inpf:
                            bbox = pickle.load(inpf)
                            min_x, min_y, max_x, max_y = bbox.min_x, bbox.min_y, bbox.max_x, bbox.max_y
                            wkt = """POLYGON(({0} {1},{2} {3},{4} {5},{6} {7},{0} {1}))""".format(
                                min_x,
                                min_y,
                                max_x,
                                min_y,
                                max_x,
                                max_y,
                                min_x,
                                max_y
                            )
                            my_collection.write({
                                "geometry": mapping(loads(wkt)),
                                "properties": {
                                    "id": id,
                                    "patchlet": patchlet_name
                                }
                            })
                            id += 1

logging.getLogger().setLevel(logging.ERROR)


# Setup credentials
# The procedure assumes access to a AWS S3 bucket from which it loads and stores data.
BUCKET_NAME = ''
AWS_REGION = ''
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
SH_CLIENT_ID = ''
SH_CLIENT_SECRET = ''
# sh_config = SHConfig()
# sh_config.sh_client_id = SH_CLIENT_ID
# sh_config.sh_client_secret = SH_CLIENT_SECRET
# sh_config.aws_secret_access_key = AWS_SECRET_ACCESS_KEY
# sh_config.aws_access_key_id = AWS_ACCESS_KEY_ID
# sh_config.save()
#
# base_config = BaseConfig(bucket_name=BUCKET_NAME,
#                          aws_region=AWS_REGION,
#                          aws_access_key_id=AWS_ACCESS_KEY_ID,
#                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#
# filesystem = prepare_filesystem(base_config)


# Global constants
PROJECT_DATA_ROOT = 'field-delineation/input-data'  # Local folder where project related  files are/will be stored !!
INPUT_AOI_FILEPATH = os.path.join(PROJECT_DATA_ROOT, 'aoi.geojson')
GRID_PATH = os.path.join(PROJECT_DATA_ROOT, 'grid.gpkg')
REFERENCE_DATA_FILEPATH = os.path.join(PROJECT_DATA_ROOT, 'fields.gpkg')
TIME_INTERVAL = ['2021-08-03', '2021-11-19']  # Set the time interval for which the data will be downloaded YYYY-MM-DD
EOPATCHES_FOLDER = os.path.join(PROJECT_DATA_ROOT, 'eopatches')  # Location on the bucket to which EOPatches will be saved.
BATCH_TIFFS_FOLDER = os.path.join(PROJECT_DATA_ROOT, 'tiffs') # Location on the bucket where downloaded TIFF images will be stored
PATCHLETS_FOLDER = os.path.join(PROJECT_DATA_ROOT, 'patchlets')  # Location on the bucket to which sampled patchlets will be saved.
NPZ_FILES_FOLDER = os.path.join(PROJECT_DATA_ROOT, 'patchlets_npz')  # Location on the bucket to which the NPZ files will be saved.
METADATA_DATAFRAME = os.path.join(PROJECT_DATA_ROOT, 'patchlet-info.csv')  # Filepath to which the metadata dataframe will be saved as a CSV
LOCAL_MODEL_FOLDER = os.path.join(PROJECT_DATA_ROOT, 'niva-cyl-models')  # Local path to the folder where models are saved
S3_MODEL_FOLDER = PROJECT_DATA_ROOT  # Path to the bucket folder  models are saved
N_FOLDS = 3  # number of folds to use for cross validation
RASTER_RESULTS_FOLDER = os.path.join(PROJECT_DATA_ROOT, 'results', 'Spain')  # Define folder where rasterized predictions will be saved to
MAX_WORKERS = 1 # os.cpu_count() #- 2  # 

if not os.path.exists(PATCHLETS_FOLDER):
    os.makedirs(PATCHLETS_FOLDER)

def check_grid():
    """
       Check grid
    """
    grid = gpd.read_file(GRID_PATH)
    aoi = gpd.read_file(INPUT_AOI_FILEPATH)
    fig, ax = plt.subplots(figsize=(10, 10))
    grid.boundary.plot(ax=ax, color='r')
    aoi.plot(ax=ax)
    ax.legend()


def convert_to_eopatches():
    """
       [1] Convert to EOPatches
    """
    tiffs_to_eop_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "grid_filename": GRID_PATH,
        "tiffs_folder": BATCH_TIFFS_FOLDER,
        "eopatches_folder": EOPATCHES_FOLDER,
        "band_names": ["B02", "B03", "B04", "B08", "trees_prob", "water_prob"],#, "crop_prob", "grass_prob" "bare_prob", "trees_prob", "water_prob"], #Need to edit td_data_utils Line 201 and normalization.py script & prediction.py line 134
        "mask_name": "dataMask",
        "data_name": "BANDS",
        "is_data_mask": "IS_DATA",
        "clp_name": "CLP",
        "clm_name": "CLM",
        "max_workers": MAX_WORKERS
    }
    convert_tiff_to_eopatches(tiffs_to_eop_config)

    # Check if EOPatches have been written
    #eops = filesystem.listdir(EOPATCHES_FOLDER)
    #print(eops)

    # Load a sample EOPatch to check the values
    #eop = EOPatch.load(os.path.join(EOPATCHES_FOLDER, eops[0]), filesystem=filesystem)
    #print(eop)

    #tidx = 3 # select one timestamp between 0 and number of timestamps in the EOPatch
    #viz_factor = 2.5

    #fig, axs = plt.subplots(figsize=(15, 5), ncols=3, sharey=True)
    #axs[0].imshow(viz_factor * eop.data['BANDS'][tidx][..., [2,1,0]]/10000)
    #axs[0].set_title('RGB bands')
    #axs[1].imshow(eop.data['CLP'][tidx].squeeze()/255, vmin=0, vmax=1)
    #axs[1].set_title('Cloud probability')
    #axs[2].imshow(eop.mask['IS_DATA'][tidx].squeeze(), vmin=0, vmax=1)
    #axs[2].set_title('Valid data');


def add_reference_data_to_patches():
    """
    [2] Add GSAA reference data to patches

    This step adds GSAA reference data to each EOPatch in vector and raster format.

    The following features are added to eopatches:

     * original vector data
     * raster mask from pixelated vector data, 10m buffer
     * boundary mask (buffered raster minus raster pixelated)
     * normalised distance transform

     Note: in this localised version, in a departure from original codebase, original vector data is loaded in from
     a GeoPackage rather than from a PostGIS database. This approach may not scale if the original vector data is large
     e.g. country-wide. If this is the case the approach of storing/accessing the data from a PostGIS database should
     probably be used
    """
    rasterise_gsaa_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "database": "",
        "user": "",
        "password": "",
        "host": "",
        "port": "",
        #"crs": "epsg:4326",
        "crs": "epsg:32630",
        "grid_filename": GRID_PATH,
        "eopatches_folder": EOPATCHES_FOLDER,
        "vector_feature": ["vector_timeless", "GSAA_ORIGINAL"],
        "extent_feature": ["mask_timeless", "EXTENT"],
        "boundary_feature": ["mask_timeless", "BOUNDARY"],
        "distance_feature": ["data_timeless", "DISTANCE"],
        "height": 998, #1201
        "width": 998, #1201
        "buffer_poly": -10,
        "no_data_value": 0,
        "disk_radius": 2,
        "max_workers": MAX_WORKERS,
        "roi_gpkg_filename": GRID_PATH,
        "reference_data_gpkg_filename": REFERENCE_DATA_FILEPATH
    }

    # TODO - check if it`s the grid or the roi that is being used here
    rasterise_gsaa(rasterise_gsaa_config)


def check_ref():
    # Check the contents of the EOPatches to see if adding reference data was successful.
    eops= os.listdir('field-delineation/input-data/eopatches/')
    #eops = filesystem.listdir(EOPATCHES_FOLDER)
    eop = EOPatch.load(os.path.join(EOPATCHES_FOLDER, eops[0]), lazy_loading=True)
    print(type(eop))
    print(eop.data)
    fig, ax = plt.subplots(ncols=2, figsize=(15, 15))
    draw_true_color(ax[0], eop, time_idx=1, factor=3.5/10000, feature_name='BANDS', bands=(2, 1, 0), grid=False)
    draw_bbox(ax[0], eop)
    draw_vector_timeless(ax[0], eop, vector_name='GSAA_ORIGINAL', alpha=.3)
    #
    draw_true_color(ax[1], eop, time_idx=1, factor=3.5/10000, feature_name='BANDS', bands=(2, 1, 0), grid=False)
    draw_bbox(ax[1], eop)
    draw_mask(ax[1], eop, time_idx=None, feature_name='EXTENT', alpha=.3)
    plt.savefig('field-delineation/Ref_summary.jpg')


def sample_patchlets_from_eopatches():
    """
    [3] Sample Patchlets from EOPatches

    This part samples image chips out of the larger `EOPatches`. A maximum number of chips is sampled randomly from the
    `EOPatch`, depending on the fraction of reference `EXTENT` pixels. A buffer where patchlets are not sampled from
    can also be specified. Image chips containing only valid image data and a cloud coverage lower than
    a threshold are sampled.
    """
    sampling_patchlets_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "eopatches_location": EOPATCHES_FOLDER,
        "output_path": PATCHLETS_FOLDER,
        "sample_positive": True,
        "grid_definition_file": GRID_PATH,
        "area_geometry_file": INPUT_AOI_FILEPATH,
        "sampled_feature_name": "BANDS",
        "mask_feature_name": "EXTENT",
        "buffer": 50,
        "patch_size": 256,
        "num_samples": 10,
        "max_retries": 500,
        "fraction_valid": 0.4, #Relates to valid satellite data, not reference data
        "cloud_coverage": 0.05,
        "max_workers": MAX_WORKERS
    }

    negatives_examples_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "eopatches_location": EOPATCHES_FOLDER,
        "output_path": os.path.join(PROJECT_DATA_ROOT, 'patchlets_neg'),
        "sample_positive": False,
        "grid_definition_file": GRID_PATH,
        "area_geometry_file": INPUT_AOI_FILEPATH,
        "sampled_feature_name": "BANDS",
        "mask_feature_name": "EXTENT",
        "buffer": 250,
        "patch_size": 256,
        "num_samples": 1,
        "max_retries": 2,
        "fraction_valid": 0.4, #Relates to valid satellite data, not reference data
        "cloud_coverage": 0.05,
        "max_workers": MAX_WORKERS
    }

    sample_patchlets(sampling_patchlets_config)
    sample_patchlets(negatives_examples_config)

    

    # **Check what was written, take a look at one of the created patchlets to see if the contents make sense**
    # patchlets = filesystem.listdir(PATCHLETS_FOLDER)
    # patchlet = EOPatch.load(os.path.join(PATCHLETS_FOLDER, patchlets[11]), filesystem=filesystem)
    # patchlet.data['BANDS'].shape
    # fig, ax = plt.subplots(ncols=2, figsize=(15, 15))
    # draw_true_color(ax[0], eop, time_idx=3, factor=3.5/10000, feature_name='BANDS', bands=(2, 1, 0), grid=False)
    # draw_mask(ax[1], eop, time_idx=None, feature_name='EXTENT', alpha=1, grid=False)


def create_npz_file_from_patchlets():
    """
    [4] Create .npz files from patchlets

    This steps creates a series of `.npz` files which join the data and labels sampled in patchlets from the previous
    iteration. A dataframe is created to keep track of the origin of the patchlets, namely which eopatch they come from
    and at which position they were sampled. This dataframe is later used for the cross-validation splits.
    """
    patchlets_to_npz_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "patchlets_folder": PATCHLETS_FOLDER,
        "output_folder": NPZ_FILES_FOLDER,
        "output_dataframe": METADATA_DATAFRAME,
        "chunk_size": 10,
        "max_workers": MAX_WORKERS
    }

    patchlets_to_npz_files(patchlets_to_npz_config)

    patchlets_to_npz_config_neg = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "patchlets_folder": os.path.join(PROJECT_DATA_ROOT, 'patchlets_neg'),
        "output_folder": os.path.join(PROJECT_DATA_ROOT, 'patchlets_npz_neg'),
        "output_dataframe": os.path.join(PROJECT_DATA_ROOT, 'patchlet-info-neg.csv'),
        "chunk_size": 10,
        "max_workers": MAX_WORKERS
    }

    patchlets_to_npz_files(patchlets_to_npz_config_neg)

    patchpos = pd.read_csv(os.path.join(PROJECT_DATA_ROOT, 'patchlet-info.csv'))
    patchneg = pd.read_csv(os.path.join(PROJECT_DATA_ROOT, 'patchlet-info-neg.csv'))
    patchneg['chunk'] = patchneg['chunk'].str.replace('.npz', '_neg.npz')
    ##patchneg = patchneg.replace(regex=['delineation_'], value ='delineation_neg_')
    patchpos = patchpos.append(patchneg)
    patchpos.to_csv (os.path.join(PROJECT_DATA_ROOT, 'patchlet-info.csv'), index = False, header=True)
    
    for f in glob.glob(r'field-delineation/input-data/patchlets_npz_neg/*.npz'):
        os.rename(f, f.replace('.npz', '_neg.npz'))

    copy_tree(os.path.join('field-delineation', 'input-data', 'patchlets_npz_neg'), os.path.join('field-delineation', 'input-data', 'patchlets_npz')) 

    


    # Make some sanity checks on the created NPZ files
    #filesystem.listdir(NPZ_FILES_FOLDER)
    #npzfile = np.load(filesystem.openbin(os.path.join(NPZ_FILES_FOLDER, 'patchlets_field_delineation_0.npz')))
    #print(list(npzfile.keys()))
    #print(pd.read_csv(filesystem.openbin(METADATA_DATAFRAME)).head())

    #filesystem.listdir(os.path.join(PROJECT_DATA_ROOT, 'patchlets_npz_neg'))
    #npzfile = np.load(os.path.join(os.path.join(PROJECT_DATA_ROOT, 'patchlets_npz_neg'), 'patchlets_field_delineation_0.npz'))
    #print(list(npzfile.keys()))
    #print(pd.read_csv(os.path.join(PROJECT_DATA_ROOT, 'patchlet-info-neg.csv')).head())


def calculate_normalization_stats_per_timestamp():
    """
    [5] Calculate normalization stats per timestamp

    This step computes the normalisation factors per band per month for the `.npz` files obtained so far.
    These normalisation factors are saved to `.csv` file and will be used at training and validation of the model.
    """

    # Check that normalization factors were added to the metadata dataframe
    #print(pd.read_csv(filesystem.openbin(METADATA_DATAFRAME)).head())

    calculate_normalization_factors_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "npz_files_folder": NPZ_FILES_FOLDER,
        "metadata_file": METADATA_DATAFRAME,
        "nbands": 9,
        "max_workers": MAX_WORKERS
    }

    calculate_normalization_factors(calculate_normalization_factors_config)

    #calculate_normalization_factors_config_neg = {
    #    "bucket_name": BUCKET_NAME,
    #    "aws_access_key_id": AWS_ACCESS_KEY_ID,
    #    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    #    "aws_region": AWS_REGION,
    #    "npz_files_folder": os.path.join(PROJECT_DATA_ROOT, 'patchlets_npz_neg'),
    #    "metadata_file": os.path.join(PROJECT_DATA_ROOT, 'patchlet-info-neg.csv'),
    #    "max_workers": MAX_WORKERS
    #}

    #calculate_normalization_factors(calculate_normalization_factors_config_neg)


def split_patchlets_for_cross_validation():
    """
    [6] Split patchlets for k-fold cross-validation

    This step:

     * loads the dataframe with the patchlets descriptions
     * splits the eopatches and corresponding patchlets into k-folds
     * updates the info csv file with fold information
    """
    k_fold_split_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "metadata_path": METADATA_DATAFRAME,
        "npz_folder": NPZ_FILES_FOLDER,
        "n_folds": N_FOLDS,
        "seed": 2,
        "max_workers": 1
    }

    k_fold_split(k_fold_split_config)

    # Some sanity checks:
    #for fold in range(N_FOLDS):
    #    print(f'In Fold {fold+1} :')
    #    print(os.listdir(os.path.join(NPZ_FILES_FOLDER, f'fold_{fold+1}')))
    #    print('\tCount:{0}'.format(len(os.listdir(os.path.join(NPZ_FILES_FOLDER, f'fold_{fold+1}')))))


def train_resunet_model():
    """
    [7] Train a ResUnetA model

    This step  performs training of the `ResUnetA` architecture using the `.npz` files prepared in the previous steps:

    This steps:

     * creates TensorFlow datasets in a k-fold cross-validation scheme, using the npz files previously created.
       The datasets allow manipulation and loading on the fly, to reduce RAM load and processing of large AOIs
     * performs training of the k-fold models
     * test the models predictions on a validation batch


    This workflow can load the `.npz` files from the S3 bucket, or locally. The training will be faster if files are
    copied and loaded from local disk. Change the `npz_from_s3` flag in the data loader function.
    """
    CHKPT_FOLDER = None # "field-delineation/input-data/niva-cyl-models/resunet-a_fold-1_2022-07-01-21-09-39/checkpoints" #None  # Path to pretrained model if exists

    # TODO - check these params
    train_k_folds_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "wandb_id": None,
        "npz_folder": NPZ_FILES_FOLDER,
        #"npz_from_s3": True,
        "npz_from_s3": False,  # TODO - check this is appropriate
        #"num_parallel": 100,
        "num_parallel": 8,  # TODO - check this is appropriate
        "metadata_path": METADATA_DATAFRAME,
        "model_folder": LOCAL_MODEL_FOLDER,
        "model_s3_folder": S3_MODEL_FOLDER,
        "chkpt_folder": CHKPT_FOLDER,
        "input_shape": [256, 256, 6],
        "n_classes": 2,
        "batch_size": 8, #Max for 8 GPUs = 4 # More info here: https://www.pugetsystems.com/labs/hpc/GPU-Memory-Size-and-Deep-Learning-Performance-batch-size-12GB-vs-32GB----1080Ti-vs-Titan-V-vs-GV100-1146/
        #"iterations_per_epoch": 1500,  # Change based on the size of the AOI
        "iterations_per_epoch": 75,  #400 = (n obs / batch size) * 2 - See advice here: https://stackoverflow.com/questions/49922252/choosing-number-of-steps-per-epoch
        #"num_epochs": 30,
        "num_epochs": 50,  
        "model_name": "resunet-a",
        "reference_names": ["extent","boundary","distance"],
        "augmentations_feature": ["flip_left_right", "flip_up_down", "rotate", "brightness"],
        "augmentations_label": ["flip_left_right", "flip_up_down", "rotate"],
        "normalize": "to_medianstd",
        "n_folds": N_FOLDS,
        "fill_value": -2,
        "seed": 42,
        "wandb_project": "",
        "model_config": {
            "learning_rate": 0.0001,
            "n_layers": 3,
            "n_classes": 2,
            "keep_prob": 0.8,
            "features_root": 32,
            "conv_size": 3,
            "conv_stride": 1,
            "dilation_rate": [1, 3, 15, 31],
            "deconv_size": 2,
            "add_dropout": True,
            "add_batch_norm": False,
            "use_bias": False,
            "bias_init": 0.0,
            "padding": "SAME",
            "pool_size": 3,
            "pool_stride": 2,
            "prediction_visualization": True,
            "class_weights": None
        }
    }

    train_k_folds(train_k_folds_config)

    # **A model is trained for each fold and then the weights of these models are averaged out to create
    # a single model that is applied to the whole AOI.**
    AVG_MODELS = [x for x in os.listdir(LOCAL_MODEL_FOLDER) if 'avg' in x]
    AVG_MODEL = AVG_MODELS[0]
    print('After training AVG_MODEL is: {0}'.format(AVG_MODEL))


def make_prediction():
    """
    [8] Make prediction
    """
    # find the name of the avg model that the training step created
    # resunet-a-avg_YYYY-MM-DD-HH-MM-SS under the LOCAL_MODEL_FOLDER
    AVG_MODEL = None
    AVG_MODELS = [x for x in os.listdir(LOCAL_MODEL_FOLDER) if 'avg' in x]
    AVG_MODEL = AVG_MODELS[0]
    #AVG_MODEL = "resunet-a_fold-real3_2022-07-03-00-25-41"

    if AVG_MODEL is not None:
        print('AVG_MODEL: ', AVG_MODEL)
        prediction_config = {
            "bucket_name": BUCKET_NAME,
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "aws_region": AWS_REGION,
            "grid_filename": GRID_PATH,
            "eopatches_folder": EOPATCHES_FOLDER,
            "feature_bands": ["data", "BANDS"],
            "feature_extent": ["data", "EXTENT_PREDICTED"],
            "feature_boundary": ["data", "BOUNDARY_PREDICTED"],
            "feature_distance": ["data", "DISTANCE_PREDICTED"],
            "model_path": LOCAL_MODEL_FOLDER,
            "model_name": AVG_MODEL,
            "model_version": "v1",
            "temp_model_path": LOCAL_MODEL_FOLDER,
            "normalise": "to_meanstd",
            "height":1128, # 1250, #1128,  # SF - Pyramiding requires input of 1128 or 1250 ...
            "width": 1128, # 1250, #1128  # as above
            "pad_buffer": 130, # 49, #130  #SF - Model training h&w = 1201. Buffer for pyramiding 1201+(24.5*2)=1250
            "crop_buffer": 130, #48 #130
            "n_channels": 6,
            "n_classes": 2,
            "metadata_path": METADATA_DATAFRAME,
            "batch_size": 4
        }
        logging.getLogger().setLevel(logging.INFO)
        pred = run_prediction(prediction_config)
        logging.getLogger().setLevel(logging.ERROR)
    else:
        print('AVG_MODEL not found')


def post_processing():
    """
    [9] Postprocessing

    Predictions for each timestamp within the requested period have been saved to the EOPatches. We need to
    temporally merge these predictions to get one prediction for each area.

    The following steps are executed:
    * merge predictions over a time interval using percentile statistics (median is used)
    * join the extent and boundary predictions derived from previous step
    * iteratively smooth and upscale the joined prediction for a visually smoother output
    * export the resulting map as .tif to be used for vectorisation
    """
    postprocessing_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "grid_filename": GRID_PATH,
        #"time_intervals": {"MAY": ["2021-05-01", "2021-05-31"]},
        "time_intervals": {"AUTUMN21": ["2021-03-01", "2021-07-01"]},  # we have data from 2021-08-03 to 2021-11-19
        "eopatches_folder": EOPATCHES_FOLDER,
        "tiffs_folder": RASTER_RESULTS_FOLDER,
        "feature_extent": ["data", "EXTENT_PREDICTED"],
        "feature_boundary": ["data", "BOUNDARY_PREDICTED"],
        "model_version": "v1",
        "max_cloud_coverage": 0.10,  # TODO on denmark data we upped this to 0.25, set this accordingly?
        "percentile": 50,
        "scale_factor": 2,
        "disk_size": 2,
        "max_workers": MAX_WORKERS
    }

    run_post_processing(postprocessing_config)

    # copy output for next processing step
    from_directory = RASTER_RESULTS_FOLDER
    to_directory = os.path.join(PROJECT_DATA_ROOT, 'fd-predictions')
    copy_tree(from_directory, to_directory)


def create_vectors():
    """
    [10] Creating vectors

    The following steps are executed to vectorise and spatially merge the vectors:
    * Create a weights.tiff file, based on upscaled tiff dimension and overlap size that is used to assign gradual
    weights to enable seemless merging across EOPatches.
    * list tiffs that should be in the vrt (all tiffs should be from same UTM zone)
    * create vrt file
    * run process over whole area, split into small tiles:
      * for each row run:
         * extract small tiff file from vrt (gdal_transform)
         * contour it (gdal_contour)
         * merge extracted contours with existing merged tiles of the row
      * (run rows in multiprocess mode to speed up processing)
      * merge rows using the same approach
    """
    vectorize_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "tiffs_folder": RASTER_RESULTS_FOLDER,
        #"time_intervals": ["MAY"],  # TODO - what do we put here? - seems to be the AUTUMN21 key from time_intervals dict in prev step
        "time_intervals": ["AUTUMN21"],
        #"utms": ["32634"], # List all the different UTM zones within the AOI
        "utms": ["32630"],  # TODO - needs to be 32630 in our case?
        "shape": [4810, 4810], #4400 height/width *4?
        "buffer": [200, 200], #200
        "weights_file": os.path.join(PROJECT_DATA_ROOT, "weights.tiff"),
        "vrt_dir": '.',
        "predictions_dir": os.path.join(PROJECT_DATA_ROOT, "fd-predictions"),
        "contours_dir": os.path.join(PROJECT_DATA_ROOT, "fd-contours"),
        "max_workers": MAX_WORKERS,
        "chunk_size": 500,
        "chunk_overlap": 10,
        "threshold": 0.6,  # do we need to modify the value of this?
        "cleanup": True,
        "skip_existing": True,
        "rows_merging": True
    }

    # JRCC - do some validation to make sure we have created/populated required output folders for the vectorization
    # i.e.
    # ~/Work/FieldBoundaries/spain_split_up_data/fd-contours
    # ~/Work/FieldBoundaries/spain_split_up_data/fd-predictions/AUTUMN21

    have_contours_dir = False
    have_predictions_dir = False

    if os.path.exists(vectorize_config["contours_dir"]):
        have_contours_dir = True

    if os.path.exists(vectorize_config["predictions_dir"]):
        if os.path.exists(os.path.join(vectorize_config["predictions_dir"], vectorize_config["time_intervals"][0])):
            have_predictions_dir = True
        else:
            print("predictions_dir: {0} exists but subfolder: {1} not found - please copy from results folder".format(
                vectorize_config["predictions_dir"],
                os.path.join(vectorize_config["predictions_dir"], vectorize_config["time_intervals"][0])
            )
            )
    else:
        print("predictions_dir: {0} does not exist - please create".format(vectorize_config["predictions_dir"]))

    # we have the required folders so can proceeed
    if have_predictions_dir and have_contours_dir:
        vectorise(vectorize_config)
    else:
        print("ABORTED as required predictions_dir and contours_dir are missing or unpopulated...")

    # Check the vector file
    # vectors = gpd.read_file(os.path.join(PROJECT_DATA_ROOT, 'fd-contours', 'merged_MAY_32634.gpkg'))
    # fig, ax = plt.subplots(figsize=(15, 15))
    # vectors.plot(ax=ax)


def merge_utm_zones():
    """
    [11] Merge UTM zones

     <div class="alert alert-block alert-info"><b>This step is only needed if the AOI spans multiple UTM zones</b>

    The procedure outline is:
    * define geometries for two UTM zones and their overlap
    * load the two single-UTM-zone vector predictions
    * split them into parts: non-overlapping (completely within UTM zone) and overlapping
    * merge the overlaps by:
      * transform them to single CRS (WGS84)
      * spatial join of the overlapping geodataframes from the two zones
      * finding geometries that do not overlap (and keeping them)
      * unary_union-ize the polygons that intersect and merge them to the geometries from previous step
    * transform everything to resulting (common) CRS
    * clean up the results (remove geometries with area larger than X * max allowed size of polygon)
    * simplify geometries
    """
    utm_merging_config = {
        "bucket_name": BUCKET_NAME,
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
        "grid_definition_file": GRID_PATH,
        "time_intervals": ["MAY"],
        "utms": ["32634", "32635"],
        "contours_dir": os.path.join(PROJECT_DATA_ROOT, "fd-contours"),
        "resulting_crs": "",  # !! Choose an appropriate meter-based CRS  that covers the AOI !!
        "max_area": 4153834.1,
        "simplify_tolerance": 2.5,
        "n_workers": MAX_WORKERS,
        "overlap_buffer": -0.0001,
        "zone_buffer": 0.00001
    }

    merge_zones(utm_merging_config)


def run_end_to_end_workflow():
    check_grid()
    print("[Step 1 - CONVERTING DATA TO EOPATCHES")
    #convert_to_eopatches()
    #
    print("[Step 2 - ADDING REFERENCE DATA TO EOPATCHES")
    #add_reference_data_to_patches()
    #

    #check_ref()

    print("[Step 3- SAMPLE PATCHLETS FROM EOPATCHES")
    #sample_patchlets_from_eopatches()
    #write_patchlets_extents_to_shapefile(
    #    path_to_patchlets=PATCHLETS_FOLDER,
    #    out_shp_fname=os.path.join(PROJECT_DATA_ROOT, 'patchlets_extents.shp')
    #)

    print("[Step 4- CREATING .NPZ FILES FROM PATCHLETS")
    #create_npz_file_from_patchlets()
    #npzfile = np.load(os.path.join(os.path.join(PROJECT_DATA_ROOT, 'patchlets_npz_neg'), 'patchlets_field_delineation_0.npz'))
    #print(list(npzfile.keys()))


    print("[Step 5- CALCULATE NORMALIZATION STATS PER TIMESTAMP")
    #calculate_normalization_stats_per_timestamp()

    print("[Step 6- SPLIT PATCHLETS FOR K-FOLD CROSS-VALIDATION")
    #split_patchlets_for_cross_validation()

    print("[Step 7- TRAIN RESUNET MODEL")
    #train_resunet_model()

    print("[Step 8- MAKE PREDICTION")
    make_prediction()

    print("[Step 9- POST PROCESSING")
    post_processing()

    print("[Step 10- CREATE VECTORS")
    create_vectors()

    # skip this step for now
    #merge_utm_zones()


if __name__ == "__main__":
    run_end_to_end_workflow()



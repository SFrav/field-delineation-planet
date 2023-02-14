import os
import rasterio
from rasterio.windows import Window
from rasterio.mask import mask

#list all tifs in the directory and subdirectories
def list_tiffs(directory):
    tiffs = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".tif"):
                tiffs.append(os.path.join(root, file))
    return tiffs

#Define a function to loop through tiffs and clip each raster to 998 in width and 998 in height for all bands then overwrite raster
def clip_tiffs(input_dir):
    tiffs = list_tiffs(input_dir)

    for tiff in tiffs:
        with rasterio.open(tiff) as src:
            out_meta = src.meta.copy()
            #Loop through bands in src and apply the window to each band
            #for i in range(src.count):
            out_meta.update({"driver": "GTiff",
                                 "height": 998,
                                 "width": 998,
                                 "transform": src.window_transform(Window(0, 0, 998, 998))})
                #Create output file name
                #out_name = os.path.join(output_dir, os.path.basename(tiff))
                #Create output file
            with rasterio.open(tiff, "w", **out_meta) as dest:
                    #Write the data to the output file
                    dest.write(src.read(window=Window(0, 0, 998, 998)))

            #with rasterio.open(output_dir + tiff.split("/")[-1], "w", **out_meta) as dest:
            #    dest.write(src, window=Window(0, 0, 998, 998))
            #repeat mask for all bands
            #for i in range(1, src.count + 1):
            #out_image = src.read(1, window=Window(0, 0, 998, 998))
            #out_image, out_transform = mask(src, [998,998], crop=True)
            #out_meta = src.meta.copy()
            #out_meta.update({"driver": "GTiff",
            #                     "height": out_image.shape[1],
            #                     "width": out_image.shape[2],
            #                     "transform": out_transform})
            #with rasterio.open(output_dir + tiff.split("/")[-1], "w", **out_meta) as dest:
            #    dest.write(out_image, 1)


if __name__ == "__main__":
    clip_tiffs('input-data/tiffs')
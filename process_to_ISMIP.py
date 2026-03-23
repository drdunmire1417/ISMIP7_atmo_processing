import os
from glob import glob
from concurrent.futures import ProcessPoolExecutor
from config_reader import read_config_file
from DataRegridder import DataRegridder
from Climatology import Climatology
from Anomalies import Anomalies
import xarray as xr
import json
import warnings
from functools import partial
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=xr.SerializationWarning)

def initialize_worker(config, var): #Initializes the DataRegridder class locally on each CPU core.
    global _worker_regridder
    _worker_regridder = DataRegridder(config, var)

def wrapper_regrid(filepath):
    return _worker_regridder.regrid_single_file(filepath)

print("\nReading config file...")
my_config = read_config_file()

if __name__ == '__main__':
    for var in my_config.var_list:
        print('Working on variable:', var)
        with open(f'attrs/{my_config.method}.json', 'r') as f:
            j = json.load(f)
            src_var = j[var]['src_folder']

        src_dir = f'{my_config.src_dir}{src_var}/'
        source_files = glob(f'{src_dir}*')
        _worker_regridder = None
        local_pipeline = DataRegridder(my_config,var)

        ## 1 - Compute weights if necessary
        if my_config.normal: 

            if os.path.exists(my_config.weights_path):
                print("Weights file already exists")
            else:
                print("Weights file does not exist")
                print("\n### -------------- STEP 1 Compute weights -------------- ###\n")
                print(f"Loading source grid from: {src_dir}")
                if not source_files:
                    raise FileNotFoundError(f"No files found in {src_dir}")   

                source_file = source_files[0]
                print("Computing Weights ... ...")
                local_pipeline.compute_weights(source_file)

            ## 2 - Regrid files in folder
            print("\n### -------------- STEP 2 Regrid files -------------- ###\n")
            NUM_WORKERS = my_config.NUM_WORKERS 

            print(f"Starting parallel processing on {NUM_WORKERS} cores... ")
            with ProcessPoolExecutor(max_workers=NUM_WORKERS, 
                                    initializer=initialize_worker, 
                                    initargs=(my_config,var)) as executor:
                futures = [
                    executor.submit(wrapper_regrid, file_path)
                    for file_path in glob(f'{src_dir}*')
                ]  
                for future in futures:
                    print(future.result())     

            # 3 Compute climatology if scenario = historical
            if my_config.scenario=='historical':
                print("\n### -------------- STEP 3 Climatology -------------- ###\n")
                print("Computing Climatology ... ")
                clim = Climatology(var, my_config.icesheet, my_config.gcm, my_config.method, my_config.version, my_config.out_dir)
                files = clim.get_climatology_files()
                climatology = clim.compute_climatology(files)
                clim.save_climatology(climatology)

                if var == 'tas':
                    clim = Climatology('ts', my_config.icesheet, my_config.gcm, my_config.method, my_config.version, my_config.out_dir)
                    files = clim.get_climatology_files()
                    climatology = clim.compute_climatology(files)
                    clim.save_climatology(climatology)


            # 4 compute anomalies
            print("\n### -------------- STEP 4 Anomalies -------------- ###\n")
            print("Computing Anomalies ... ")
            anom = Anomalies(var, my_config.icesheet, my_config.gcm, my_config.method, my_config.scenario, my_config.version, my_config.out_dir)
            clim = anom.get_climatology()
            files = glob(f'{my_config.out_dir}{my_config.icesheet}/{my_config.gcm}/{my_config.scenario}/{my_config.method}_processed/{var}/v{my_config.version}/*.nc')
            
            NUM_WORKERS = my_config.NUM_WORKERS 
            print(f"Starting parallel anomaly processing on {NUM_WORKERS} cores... ")
            with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
                list(executor.map(partial(anom.compute_anomalies_file, clim), files))
            print("Done!")

            if var == 'tas':
                anom = Anomalies('ts', my_config.icesheet, my_config.gcm, my_config.method, my_config.scenario, my_config.version, my_config.out_dir)
                clim = anom.get_climatology()
                files = glob(f'{my_config.out_dir}{my_config.icesheet}/{my_config.gcm}/{my_config.scenario}/{my_config.method}_processed/ts/v{my_config.version}/*.nc')
            
                NUM_WORKERS = my_config.NUM_WORKERS 
                print(f"Starting parallel anomaly processing on {NUM_WORKERS} cores... ")
                with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
                    list(executor.map(partial(anom.compute_anomalies_file, clim), files))
            print("Done!")




#     # ## 3 - Regrid gradients
#     # if my_config.gradients:
#     #     os.makedirs(my_config.gradient_out_dir, exist_ok=True)
#     #     print('Processing gradients...')
#     #     local_pipeline.regrid_gradients()
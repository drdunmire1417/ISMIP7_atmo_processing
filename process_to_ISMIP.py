import os
from glob import glob
from concurrent.futures import ProcessPoolExecutor
from config_reader import read_config_file
from DataRegridder import DataRegridder
from GradientRegridder import GradientRegridder
from Climatology import Climatology
from Anomalies import Anomalies
from regridding_fns import copy_last_year
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

    os.makedirs('weights/', exist_ok=True)
    os.makedirs('masks/', exist_ok=True)
    
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

            if my_config.scenario == 'ssp126' or my_config.scenario == 'ssp534-over' or my_config.scenario == 'ssp585':
                out_dir = f'{my_config.out_dir}{my_config.icesheet}/{my_config.gcm}/{my_config.scenario}/{my_config.method}_processed/{var}/v{my_config.version}/'
                last_file = f'{var}_{my_config.icesheet}_{my_config.gcm}_{my_config.scenario}_{my_config.method}_v{my_config.version}_2300.nc'
                if not os.path.exists(out_dir+last_file):
                    copy_last_year(out_dir, var, last_file)

            # 3 Compute climatology if scenario = historical
            if my_config.scenario=='historical':
                print("\n### -------------- STEP 3 Climatology -------------- ###\n")
                print("Computing Climatology ... ")
                clim = Climatology(var, my_config.icesheet, my_config.gcm, my_config.method, my_config.version, my_config.out_dir)
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

            # if var == 'tas':
            #     anom = Anomalies('ts', my_config.icesheet, my_config.gcm, my_config.method, my_config.scenario, my_config.version, my_config.out_dir)
            #     clim = anom.get_climatology()
            #     files = glob(f'{my_config.out_dir}{my_config.icesheet}/{my_config.gcm}/{my_config.scenario}/{my_config.method}_processed/ts/v{my_config.version}/*.nc')
            
            #     NUM_WORKERS = my_config.NUM_WORKERS 
            #     print(f"Starting parallel anomaly processing on {NUM_WORKERS} cores... ")
            #     with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            #         list(executor.map(partial(anom.compute_anomalies_file, clim), files))
            # print("Done!")

    ## 5 - Regrid gradients
    if my_config.gradients:
        print("\n### -------------- STEP 5 Regridding Gradients -------------- ###\n")
        print("Processing Anomalies ... ")
        for var in my_config.grad_var_list:
            print('Working on variable:', var)
            gradient_regridder = GradientRegridder(my_config,var)
            gradient_regridder.regrid_gradients()


        # print('Processing gradients...')
        # local_pipeline.regrid_gradients()
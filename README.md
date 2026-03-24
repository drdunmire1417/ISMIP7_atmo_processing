This package processes atmospheric forcing data for ISMIP7. It will regrid to the ISMIP7 projection, compute climatology and anomalies, and format the output with the correct naming convention.

1) Clone the repository
   
2) If it does not already exist, you may need to create an attribute file for the source dataset. This file should be named ```<method>.json``` and should be located in the ```attrs/``` directory. For each variable from the source dataset to process, it should contain the following information:
    ```
    "acabf": { # ISMIP7 variable name
      "dest_units": "kg m-2 s-1", #ISMIP7 variable units
      "src_units": "mmwe", #variable units from source dataset
      "src_var": "smb_rec", #variable name from source dataset
      "src_folder": "smb_rec" #name of folder where output is located (sometimes the same as src_var, but not necessarily)
    }
    ```

3) You also need a mask file for both the source dataset and the output. The mask files should be netcdf files with the following naming convention:

source data mask file: ```<icesheet>_mask_<method>.nc```, where ```<icesheet>``` is either ```AIS``` for Antarctica, or ```GrIS``` for Greenland

output data mask file: ```<icesheet>_mask_ISMIP_<resolution>.nc``` where ```<resolution>``` is the resolution (in meters) of the output data file.

In both source and output mask files, the netcdf variable should be "mask". For ISMIP7 forcing data, it is best if the output mask file is dilated a bit to include areas outside the ice sheet bounds.

4) Edit the configuration file: ```config.ini```. Below are some notes on the configuration file:

    ```
    [Regridding]
    icesheet = AIS  # Target Ice Sheet (AIS or GrIS)
    target_res = 2000 # if not set, AIS = 2000, GrIS = 1000
    weights_dir = weights/ #path to where weights files are stored
    masks_dir = masks/ #path to where mask files are stored
    num_workers = 12 #number of cores to run in parallel
    regrid_scheme = conservative #method to use for regridding
    
    [Source]
    src_dir = /projects/grid/ghub/ISMIP7/prep/AIS/CESM2/historical/SDBN1_raw/v1/ #directory where source data is stored
    src_epsg = 3031 #EPSG code for source data. Should be a 4-digit code. TODO: add functionality for "curvilinear"
    gcm = CESM2-WACCM #name of GCM
    scenario = historical #name of projection
    method = SDBN1 #SMB downscaling method name
    
    [Output]
    output_dir = /projects/grid/ghub/ISMIP6/devon/ISMIP7_atmo_processing/OUTPUT/ #base directory to store output
    scratch_dir = /vscratch/grp-ghub/devon-temp/temp/ #temporary scratch directory to store temporary files, these files will be deleted
    var_list = acabf, mrro, pr, snowf, snm, tas #list of ISMIP7 variable names to regrid
    version = 1 #version number for output
    
    [Cases]
    normal = yes #yes or no to regrid the output files
    gradients = no  #yes or no to regrid the gradient files
    ```


   

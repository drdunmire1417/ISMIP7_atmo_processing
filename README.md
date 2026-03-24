This package processes atmospheric forcing data for ISMIP7. It will regrid to the ISMIP7 projection, compute climatology and anomalies, and format the output with the correct naming convention

1) clone the repository
2) If it does not already exist, you may need to create an attribute file for the source dataset. This file should be named <method>.json and should be located in the attrs/ directory. For each variable to process, it should contain the following information:
    """ "acabf": { # ISMIP7 variable name
      "dest_units": "kg m-2 s-1", #ISMIP7 variable units
      "src_units": "mmwe", #variable units from source dataset
      "src_var": "smb_rec", #variable name from source dataset
      "src_folder": "smb_rec" #name of folder where output is located (sometimes the same as src_var, but not necessarily)
    } """

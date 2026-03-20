import numpy as np
import os
import xarray as xr
from pathlib import Path
from regridding_fns import update_attributes
import netCDF4
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

class Climatology:
    def __init__(self, var, icesheet, gcm, method, version, out_dir):
        """Initialize the regridder with the specific configuration object."""
        self.dest_var = var
        self.icesheet = icesheet
        self.gcm = gcm
        self.method = method
        self.version = version

        self.out_dir = f'{out_dir}{icesheet}/{gcm}/historical/{method}_processed/{var}/v{version}/'
        self.out_dir_clim = f'{out_dir}{icesheet}/{gcm}/historical/{method}_processed/extra/climatology/{var}/v{version}/'  
        os.makedirs(self.out_dir_clim, exist_ok=True)
        self.FILL_VALUE = netCDF4.default_fillvals['f4']

    def get_climatology_files(self):
        print('     Getting climatology files')
        years = np.arange(1960, 1990)
        files = []
        folder = self.out_dir
        for y in years:
            file = f'{self.dest_var}_{self.icesheet}_{self.gcm}_historical_{self.method}_v{self.version}_{y}.nc'
            path = os.path.join(folder, file)
            if os.path.exists(path):
                files.append(path)
            else: raise ValueError(f'{path} not found')
        
        return files

    def compute_climatology(self, files):
        ds = xr.open_mfdataset(files, decode_times = True, use_cftime=True)
        ds = ds.groupby(ds.time.dt.month).mean()
        ds = update_attributes(ds, self.dest_var)
        ds.attrs.update({
            'title': f'{self.dest_var} climatology 1960-1989'
        })

        return ds

    def save_climatology(self, ds):
        path = f'{self.out_dir_clim}{self.dest_var}_{self.icesheet}_{self.gcm}_historical_{self.method}_v{self.version}_1960-1989.nc'
        vars_to_encode = list(ds.data_vars) + ['x', 'y', 'month']
        encoding_dict = {var: {'dtype': 'float32', '_FillValue': self.FILL_VALUE, 'missing_value': self.FILL_VALUE} for var in vars_to_encode}
        for coord in ['x', 'y', 'month']:
            if coord in ds.coords:
                encoding_dict[coord] = {'_FillValue': None}
        ds.to_netcdf(path,encoding=encoding_dict,unlimited_dims=['month'])
        print(f'     Climatology saved at {path}')


    


    
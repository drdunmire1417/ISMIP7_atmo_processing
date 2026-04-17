import numpy as np
import os
import xarray as xr
import netCDF4
from glob import glob

class Anomalies:
    def __init__(self, var, icesheet, gcm, method, scenario, version, out_dir):
        """Initialize the regridder with the specific configuration object."""
        self.dest_var = var
        self.anom_var = f'{var}-anomaly'
        self.icesheet = icesheet
        self.gcm = gcm
        self.method = method
        self.version = version
        self.scenario = scenario

        self.out_dir = f'{out_dir}' 
        self.out_dir_anom = f'{out_dir}{icesheet}/{gcm}/{scenario}/{method}_processed/{var}-anomaly/v{version}/'    
        os.makedirs(self.out_dir_anom, exist_ok=True)
        self.FILL_VALUE = netCDF4.default_fillvals['f4']

    def get_climatology(self):
        print('     Getting climatology')
        folder = f'{self.out_dir}{self.icesheet}/{self.gcm}/historical/{self.method}_processed/extra/climatology/{self.dest_var}/v{self.version}/' 
        file = f'{self.dest_var}_{self.icesheet}_{self.gcm}_historical_{self.method}_v{self.version}_1960-1989.nc'
        path = os.path.join(folder, file)
        if os.path.exists(path):
            clim = xr.open_dataset(path)
            return clim
        else: 
            print('Make sure you run the historical period first (to compute climatology)')
            raise ValueError(f'{path} not found')

    def compute_anomalies_file(self, clim, file):
        y = int(file.replace('.nc', '').split('_')[-1])
        ds = xr.open_dataset(file)
        anomaly = ds - clim[self.dest_var].values
        anomaly = anomaly.rename({self.dest_var:self.anom_var})
        anomaly.attrs.update({'title':f'{self.dest_var} anomaly relative to 1960-1989 climatology'})

        fname = f'{self.anom_var}_{self.icesheet}_{self.gcm}_{self.scenario}_{self.method}_v{self.version}_{y}.nc'
        vars_to_encode = list(anomaly.data_vars) + ['x', 'y', 'time']
        encoding_dict = {var: {'dtype': 'float32', '_FillValue': self.FILL_VALUE, 'missing_value': self.FILL_VALUE} for var in vars_to_encode}
        for coord in ['x', 'y', 'time']:
            if coord in ds.coords:
                encoding_dict[coord] = {'_FillValue': None}
        anomaly.to_netcdf(f'{self.out_dir_anom}{fname}',encoding=encoding_dict,unlimited_dims=['time'])

       



    
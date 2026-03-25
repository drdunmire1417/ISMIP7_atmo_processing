import xesmf as xe
import numpy as np
import xarray as xr
import pyproj
import tempfile
import os
import gzip
import shutil
import netCDF4
from glob import glob
from regridding_fns import *


class GradientRegridder:
    def __init__(self, config, var):
        """Initialize the regridder with the specific configuration object."""
        self.config = config
        self.target_grid = create_target_grid(self.config.icesheet, self.config.res)

        if self.config.src_epsg=='4326': self.periodic = True
        else: self.periodic = False
        self.regridder_obj = None

        self.grad_dest_var = f'd{var}dz'
        with open(f'attrs/{self.config.method}.json', 'r') as f:
            j = json.load(f)
            self.dest_units = j[var]['dest_units']
            self.src_units = j[var]['src_units']
            self.grad_src_var = j[var]['grad_var_name']
            grad_file = j[var]['grad_file_name']
            try:
                self.grad_file = glob(f'{self.config.grad_dir}{grad_file}*')[0]
            except: raise ValueError(f'ERROR: Check gradient folder. No files found for {grad_file}')

        self.out_dir = f'{self.config.out_dir}{self.config.icesheet}/{self.config.gcm}/{self.config.scenario}/{self.config.method}_processed/{self.grad_dest_var}/v{self.config.version}/'        
        os.makedirs(self.out_dir, exist_ok=True)

        self.FILL_VALUE = netCDF4.default_fillvals['f4']

    def regrid_gradients(self):
        syear, eyear = find_yearrange_from_filename(self.grad_file)
        final_out_file = f'{self.grad_dest_var}_{self.config.icesheet}_{self.config.gcm}_{self.config.scenario}_{self.config.method}_v{self.config.version}_{syear}-{eyear}.nc'
        final_output_path = os.path.join(self.out_dir, final_out_file)

        if os.path.exists(final_output_path):
            print(f'Skipping {final_output_path}... already exists')
            return

        print(f'     Regridding gradient file for {self.grad_dest_var} in 10-year chunks...')
        
        ds_full = xr.open_dataset(self.grad_file, decode_times=False)
        total_years = eyear - syear + 1
        
        with tempfile.TemporaryDirectory(dir=self.config.scratch_dir) as tmp_dir:
            chunk_files = []
            
            for chunk_start in range(syear, eyear + 1, 10):
                chunk_end = min(chunk_start + 9, eyear)
                print(f"       Processing chunk: {chunk_start} to {chunk_end}")
                start_idx = chunk_start - syear
                end_idx = (chunk_end - syear) + 1
                ds_chunk = ds_full.isel(time=slice(start_idx, end_idx))
                ds_chunk = ds_chunk[[self.grad_src_var]]
                ds_out = ds_chunk.interp_like(self.target_grid, method="linear")
                ds_out = add_time_noleap_annual(ds_out, chunk_start, chunk_end)
                ds_out = configure_variables(ds_out, self.grad_src_var, self.grad_dest_var) 
                if self.src_units == 'mmwe' and self.dest_units == 'kg m-2 s-1':ds_out = convert_mmwe_flux(ds_out, monthly = False) 
                ds_out = ds_out.fillna(self.FILL_VALUE) 
                ds_out = update_attributes(ds_out, self.grad_dest_var)
                
                chunk_path = os.path.join(tmp_dir, f"chunk_{chunk_start}.nc")
                save_netdf(ds_out, chunk_path)
                chunk_files.append(chunk_path)
                ds_out.close()
                del ds_out

            print(f"     Concatenating {len(chunk_files)} chunks into final file...")
            with xr.open_mfdataset(chunk_files, combine='nested', concat_dim='time', decode_times=False) as ds_final:
                save_netdf(ds_final, final_output_path,fix_time = False)
            
        print(f"     Successfully saved: {final_output_path}")










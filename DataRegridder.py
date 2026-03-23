import xesmf as xe
import numpy as np
import xarray as xr
import pyproj
import tempfile
import os
import gzip
import shutil
import netCDF4
from regridding_fns import *


class DataRegridder:
    def __init__(self, config, var):
        """Initialize the regridder with the specific configuration object."""
        self.config = config
        self.target_grid = self.create_target_grid()

        if self.config.src_epsg=='4326': self.periodic = True
        else: self.periodic = False
        self.regridder_obj = None

        self.dest_var = var
        with open(f'attrs/{self.config.method}.json', 'r') as f:
            j = json.load(f)
            self.dest_units = j[var]['dest_units']
            self.src_units = j[var]['src_units']
            self.src_var = j[var]['src_var']
            self.src_folder = j[var]['src_folder']

        self.out_dir = f'{self.config.out_dir}{self.config.icesheet}/{self.config.gcm}/{self.config.scenario}/{self.config.method}_processed/{self.dest_var}/v{self.config.version}/'        
        os.makedirs(self.out_dir, exist_ok=True)

        self.FILL_VALUE = netCDF4.default_fillvals['f4']

    def create_target_grid(self): #TODO - add Greenland
        """
        Creates the target grid for regridding for AIS or GIS
        """
        if self.config.icesheet=='AIS':
            x_coords = np.arange(-3040000, 3040000+self.config.res, self.config.res)
            y_coords = np.arange(-3040000, 3040000+self.config.res, self.config.res)

            ds_target = xr.Dataset(
                data_vars=dict(
                    dummy=(["y", "x"], np.zeros((y_coords.shape[0], x_coords.shape[0]))),
                ),
                coords=dict(
                    x=("x", x_coords),
                    y=("y", y_coords),
                ),
            )
            ds_target2 =  add_coords(ds_target, 3031)
        else:
            x_coords = np.arange(-720000, 960000+self.config.res, self.config.res)
            y_coords = np.arange(-3450000, -570000+self.config.res, self.config.res)

            ds_target = xr.Dataset(
                data_vars=dict(
                    dummy=(["y", "x"], np.zeros((y_coords.shape[0], x_coords.shape[0]))),
                ),
                coords=dict(
                    x=("x", x_coords),
                    y=("y", y_coords),
                ),
            )
            ds_target2 =  add_coords(ds_target, 3413)


        return ds_target2          

    def do_regridding(self, ds_src, filename):
        year = find_year_from_filename(filename)
        out_file = f'{self.dest_var}_{self.config.icesheet}_{self.config.gcm}_{self.config.scenario}_{self.config.method}_v{self.config.version}_{year}.nc'
        output_path = os.path.join(self.out_dir, out_file)
        if os.path.exists(output_path): print(f'Skipping {output_path}... already exists')
        #print(self.config.src_mask)
        else:
            ds_src = ds_src[[self.src_var]]
            if self.dest_var == 'tas': 
                ds_src = fill_nearest_2d_only(ds_src, self.src_var, self.config.src_mask, mask_temp = True)
                ds_out = ds_src.interp_like(self.target_grid, method="linear")

            else: 
                ds_src = fill_nearest_2d_only(ds_src, self.src_var, self.config.src_mask)
                ds_src_bounded = add_coords(ds_src, self.config.src_epsg)
                if self.regridder_obj is None:
                    self.regridder_obj = make_regridder(ds_src_bounded, self.target_grid, self.config.regrid_scheme, True, self.config.weights_path, self.periodic)

                ds_out = self.regridder_obj(ds_src_bounded, keep_attrs=True)
            ds_out = mask_output(ds_out, self.config.masks_path)

            ds_out = add_time_noleap(ds_out, year)
            ds_out = configure_variables(ds_out, self.src_var,self.dest_var)
            ds_out = ds_out.fillna(self.FILL_VALUE) 
            if self.src_units == 'mmwe' and self.dest_units == 'kg m-2 s-1': ds_out = convert_mmwe_flux(ds_out)
            ds_out = update_attributes(ds_out, self.dest_var)
            ds_out.attrs.update({'title':f'{self.dest_var} regridded onto ISMIP grid'})
            save_netdf(ds_out, output_path)
            
            if self.dest_var == 'tas':
                ds_ts = ds_out.copy()
                ds_ts['tas'] = ds_out['tas'].where(ds_out['tas'] < 273.15, 273.15)
                ds_ts = ds_ts.rename({'tas':'ts'})
                ds_ts = update_attributes(ds_ts, 'ts')
                out_file_ts = f'ts_{self.config.icesheet}_{self.config.gcm}_{self.config.scenario}_{self.config.method}_v{self.config.version}_{year}.nc'
                out_dir_ts = self.out_dir.replace("/tas/", "/ts/")
                os.makedirs(out_dir_ts, exist_ok=True)
                output_path_ts = os.path.join(out_dir_ts, out_file_ts)
                save_netdf(ds_ts, output_path_ts, fix_time = False)

    def compute_weights(self, source_file):
        if source_file.endswith('.gz'):
            with tempfile.NamedTemporaryFile(dir=self.config.scratch_dir, suffix='.nc') as tmp:
                with gzip.open(source_file, 'rb') as f_in:
                    shutil.copyfileobj(f_in, tmp)
                tmp.flush() 
                with xr.open_dataset(tmp.name, decode_times = False) as ds:
                    source_grid = add_coords(ds, self.config.src_epsg)
                    regridder = make_regridder(source_grid, self.target_grid, self.config.regrid_scheme, False, self.config.weights_path, self.periodic)

        elif source_file.endswith('.nc') or source_file.endswith('.nc4'):
            ds =  xr.open_dataset(source_file, decode_times = False)
            source_grid = add_coords(ds, self.config.src_epsg)
            regridder = make_regridder(source_grid, self.target_grid, self.config.regrid_scheme, False, self.config.weights_path, self.periodic)

        else: raise ValueError(f'ERROR: File {filepath} must end in  .gz or .nc')
        
        print(f"    Weights saved to {self.config.weights_path}")

    def regrid_single_file(self, filepath):
        if filepath.endswith('.gz'):
            filename = os.path.basename(filepath).replace('.gz', '')
            try:    
                with tempfile.NamedTemporaryFile(dir=self.config.scratch_dir, suffix='.nc') as tmp:
                    with gzip.open(filepath, 'rb') as f_in:
                        shutil.copyfileobj(f_in, tmp)
                    tmp.flush() 
                    with xr.open_dataset(tmp.name, decode_times = False) as ds_in:
                        self.do_regridding(ds_in, filename)
                return f"Success: {filename}"   
            except Exception as e:
                return f"ERROR on {filename}: {str(e)}"

        elif filepath.endswith('.nc') or filepath.endswith('.nc4'):
            filename = os.path.basename(filepath).replace('.nc', '')
            try:
                ds_in = xr.open_dataset(filepath, decode_times = False)
                self.do_regridding(ds_in, filename)      
                return f"Success: {filename}"   
            except Exception as e:
                return f"ERROR on {filename}: {str(e)}"

        else: raise ValueError(f'ERROR: File {filepath} must end in  .gz or .nc')

    def regrid_gradients(self):
        grad_file = self.config.gradient_in_file
        syear, eyear = find_yearrange_from_filename(grad_file)

        grad_var = f'd{self.dest_var}dz'
        final_out_file = f'{grad_var}_{self.config.icesheet}_{self.config.gcm}_{self.config.scenario}_{self.config.method}_v{self.config.version}_{syear}-{eyear}.nc'
        final_output_path = os.path.join(self.config.gradient_out_dir, final_out_file)

        if os.path.exists(final_output_path):
            print(f'Skipping {final_output_path}... already exists')
            return

        print(f'     Regridding gradient file for {grad_var} in 10-year chunks...')
        
        ds_full = xr.open_dataset(grad_file, decode_times=False)
        total_years = eyear - syear + 1
        
        with tempfile.TemporaryDirectory(dir=self.config.scratch_dir) as tmp_dir:
            chunk_files = []
            
            for chunk_start in range(syear, eyear + 1, 10):
                chunk_end = min(chunk_start + 9, eyear)
                print(f"       Processing chunk: {chunk_start} to {chunk_end}")
                start_idx = chunk_start - syear
                end_idx = (chunk_end - syear) + 1
                ds_chunk = ds_full.isel(time=slice(start_idx, end_idx))
                ds_chunk = ds_chunk[[self.config.gradient_src_var]]
                ds_chunk = fill_nearest_2d_only(ds_chunk, self.config.gradient_src_var, self.config.src_mask)

                print('here')
                ds_out = ds_chunk.interp_like(self.target_grid, method="linear")
                ds_out = mask_output(ds_out, self.config.masks_path)
                ds_out = add_time_noleap_annual(ds_out, chunk_start, chunk_end)
                ds_out = configure_variables(ds_out, self.config.gradient_src_var, grad_var)
                ds_out = ds_out.fillna(self.FILL_VALUE)  
                if self.src_units == 'mmwe' and self.dest_units == 'kg m-2 s-1':ds_out = convert_mmwe_flux(ds_out, monthly = False) 
                ds_out = update_attributes(ds_out, grad_var)
                
                chunk_path = os.path.join(tmp_dir, f"chunk_{chunk_start}.nc")
                save_netdf(ds_out, chunk_path)
                chunk_files.append(chunk_path)
                ds_out.close()
                del ds_out

            print(f"     Concatenating {len(chunk_files)} chunks into final file...")
            with xr.open_mfdataset(chunk_files, combine='nested', concat_dim='time', decode_times=False) as ds_final:
                save_netdf(ds_final, final_output_path)
            
        print(f"     Successfully saved: {final_output_path}")

def regrid_CMIP(self):
        grad_file = self.config.gradient_in_file
        syear, eyear = find_yearrange_from_filename(grad_file)

        grad_var = f'd{self.dest_var}dz'
        final_out_file = f'{grad_var}_{self.config.icesheet}_{self.config.gcm}_{self.config.scenario}_{self.config.method}_v{self.config.version}_{syear}-{eyear}.nc'
        final_output_path = os.path.join(self.config.gradient_out_dir, final_out_file)

        if os.path.exists(final_output_path):
            print(f'Skipping {final_output_path}... already exists')
            return

        print(f'     Regridding gradient file for {grad_var} in 10-year chunks...')
        
        ds_full = xr.open_dataset(grad_file, decode_times=False)
        total_years = eyear - syear + 1
        
        with tempfile.TemporaryDirectory(dir=self.config.scratch_dir) as tmp_dir:
            chunk_files = []
            
            for chunk_start in range(syear, eyear + 1, 10):
                chunk_end = min(chunk_start + 9, eyear)
                print(f"       Processing chunk: {chunk_start} to {chunk_end}")
                start_idx = chunk_start - syear
                end_idx = (chunk_end - syear) + 1
                ds_chunk = ds_full.isel(time=slice(start_idx, end_idx))
                ds_chunk = ds_chunk[[self.config.gradient_src_var]]
                ds_chunk = fill_nearest_2d_only(ds_chunk, self.config.gradient_src_var, self.config.src_mask)

                print('here')
                ds_out = ds_chunk.interp_like(self.target_grid, method="linear")
                ds_out = mask_output(ds_out, self.config.masks_path)
                ds_out = add_time_noleap_annual(ds_out, chunk_start, chunk_end)
                ds_out = configure_variables(ds_out, self.config.gradient_src_var, grad_var)
                ds_out = ds_out.fillna(self.FILL_VALUE) 
                if self.src_units == 'mmwe' and self.dest_units == 'kg m-2 s-1':ds_out = convert_mmwe_flux(ds_out, monthly = False) 
                ds_out = update_attributes(ds_out, grad_var)
                
                chunk_path = os.path.join(tmp_dir, f"chunk_{chunk_start}.nc")
                save_netdf(ds_out, chunk_path)
                chunk_files.append(chunk_path)
                ds_out.close()
                del ds_out

            print(f"     Concatenating {len(chunk_files)} chunks into final file...")
            with xr.open_mfdataset(chunk_files, combine='nested', concat_dim='time', decode_times=False) as ds_final:
                save_netdf(ds_final, final_output_path)
            
        print(f"     Successfully saved: {final_output_path}")










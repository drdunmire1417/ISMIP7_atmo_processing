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
        self.target_grid = create_target_grid(self.config.icesheet, self.config.res)

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

        self.out_dir = f'{self.config.out_dir}{self.config.icesheet}/{self.config.gcm}/{self.config.scenario}/{self.config.method}/{self.dest_var}/v{self.config.version}/'        
        os.makedirs(self.out_dir, exist_ok=True)

        self.FILL_VALUE = netCDF4.default_fillvals['f4']

    def create_target_grid(self):
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
        print(filename)

        out_file = f'{self.dest_var}_{self.config.icesheet}_{self.config.gcm}_{self.config.scenario}_{self.config.method}_v{self.config.version}_{year}.nc'
        output_path = os.path.join(self.out_dir, out_file)

        if os.path.exists(output_path):
            print(f'Skipping {output_path}... already exists')
        else:
            ds_src = ds_src[[self.src_var]]
            ds_src = apply_src_mask(ds_src, self.src_var, self.config.src_mask)

            if self.config.regrid:
                if self.dest_var == 'tas' or self.dest_var == 'ts':
                    ds_out = ds_src.interp_like(self.target_grid, method="linear")
                else:
                    ds_src_bounded = add_coords(ds_src, self.config.src_epsg)
                    if self.regridder_obj is None:
                        self.regridder_obj = make_regridder(
                            ds_src_bounded, self.target_grid,
                            self.config.regrid_scheme, True,
                            self.config.weights_path, self.periodic
                        )
                    ds_out = masked_conservative_regrid(
                        self.regridder_obj, ds_src_bounded, self.src_var
                    )
            else:
                ds_out = ds_src


            ds_out = fill_nearest_output(ds_out, self.src_var, self.config.masks_path)
            ds_out = add_time_noleap(ds_out, year)
            ds_out = configure_variables(ds_out, self.src_var, self.dest_var)
            if self.src_units == 'mmwe' and self.dest_units == 'kg m-2 s-1':
                ds_out = convert_mmwe_flux(ds_out)
            ds_out = ds_out.fillna(self.FILL_VALUE)
            ds_out = update_attributes(ds_out, self.dest_var, self.config.res)
            ds_out.attrs.update({'title': f'{self.dest_var} regridded onto ISMIP grid'})
            save_netdf(ds_out, output_path)

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
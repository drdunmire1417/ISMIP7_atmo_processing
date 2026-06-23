import xarray as xr
import pyproj
import numpy as np
import calendar
import re
import cftime
from scipy import ndimage
import xesmf as xe
import json
import netCDF4
import os
from glob import glob
from datetime import timedelta

FILL_VALUE = netCDF4.default_fillvals['f4']

def create_target_grid(icesheet, res):
    """
    Creates the target grid for regridding for AIS or GIS
    """
    if icesheet=='AIS':
        x_coords = np.arange(-3040000, 3040000+res,res)
        y_coords = np.arange(-3040000, 3040000+res,res)

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
        x_coords = np.arange(-720000, 960000+res,res)
        y_coords = np.arange(-3450000, -570000+res,res)

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

def month_bounds(times, units='days since 1850-01-01 00:00:00'):
    starts, ends = [], []
    for t in times:
        start = cftime.datetime(t.year, t.month, 1, calendar='standard')
        if t.month == 12:
            end = cftime.datetime(t.year + 1, 1, 1, calendar='standard')
        else:
            end = cftime.datetime(t.year, t.month + 1, 1, calendar='standard')
        end = end - timedelta(days=1)
        starts.append(start)
        ends.append(end)
    starts_num = cftime.date2num(starts, units=units, calendar='standard')
    ends_num   = cftime.date2num(ends,   units=units, calendar='standard')
    return np.stack([starts_num, ends_num], axis=1)

def configure_variables(ds, old_var, new_var):
    try: 
        for var in ['lat','lon','lat_b','lon_b']:
            ds = ds.drop_vars([var])
    except: x=1
    return ds.rename({old_var:new_var})

def convert_mmwe_flux(ds, monthly = True):
    if monthly:
        #convert from units mm.w.e./month to a flux (kg m-2 s-1)
        num_seconds = []
        for d in ds.time.values:
            days_in_month = calendar.monthrange(d.year, d.month)[1]
            num_seconds.append(days_in_month * 24 * 60 * 60)

        return ds / np.array(num_seconds)[:, None, None]
    else: return ds/(365*24*60*60)


def find_year_from_filename(filename):
    pattern = r'(?<!\d)(?:18|19|20|21|22|23)\d{2}(?!\d)'
    matches = re.findall(pattern, filename)
    if len(matches) >= 1: 
        return int(matches[-1])
    else: 
        raise ValueError(f"ERROR: Could not find year from file name: {filename}")

def find_yearrange_from_filename(filename):
    pattern = r'\b\d{4}-\d{4}\b'
    print(filename)
    matches = re.findall(pattern, filename)
    if len(matches) == 1: return int(matches[0].split('-')[0]),int(matches[0].split('-')[1])
    else: raise ValueError("ERROR: Could not find yearrange from file name")

def add_time_noleap(ds, year):
    # Create a list of cftime objects for the 15th of each month (1 through 12)
    time_array = [cftime.datetime(year, month, 15) for month in range(1, 13)]

    ds['time'] = np.array(time_array, dtype=object)
    return ds

def add_time_noleap_annual(ds, syear, eyear):
    num_years = len(ds.time)   
    time_array = [
        cftime.datetime(syear + i, 12, 31) 
        for i in range(num_years)
    ]   
    ds['time'] = np.array(time_array, dtype=object)
    return ds

def update_attributes(ds, var, res):

    with open('attrs/var_attributes.json', 'r') as f:
        attr_map = json.load(f)

    if var in attr_map:
        ds[var].attrs = attr_map[var]

    ds.y.attrs = {
        'long_name': 'Cartesian y-coordinate',
        'standard_name': 'projection_y_coordinate',
        'units': 'meter',
        'axis': 'Y'
    }

    ds.x.attrs = {
        'long_name': 'Cartesian x-coordinate',
        'standard_name': 'projection_x_coordinate',
        'units': 'meter',
        'axis': 'X'
    }

    ds.attrs.update({
        'spatial_resolution': f'{res} m',
        'comment': 'Prepared for ISMIP7 by Devon Dunmire using xesmf ddunmire@buffalo.edu',
        'source': 'SDBN1 - Brice Noel -  bnoel@uliege.be'
    })
    
    return ds


def save_netdf(ds, outpath, fix_time=True):
    time_units = 'days since 1850-01-01 00:00:00'

    if fix_time:
        day1850 = cftime.datetime(1850, 1, 1)
        time_int = [(t - day1850).days for t in ds.time.values]
        time_bnds_data = month_bounds(ds.time.values)  # compute before overwriting time
        ds['time'] = time_int
        ds.time.attrs.update({
            'standard_name': 'time',
            'long_name': 'time',
            'axis': 'T',
            'units': time_units,
            'calendar': 'standard',
            'bounds': 'time_bnds',
        })

    ds['crs'] = np.array(0, dtype='int32')
    ds['crs'].attrs = {
        'grid_mapping_name': 'polar_stereographic',
        'straight_vertical_longitude_from_pole': -45.0,
        'latitude_of_projection_origin': 90.0,
        'standard_parallel': 70.0,
        'false_easting': 0.0,
        'false_northing': 0.0,
        'semi_major_axis': 6378137.0,
        'inverse_flattening': 298.257223563
    }

    for var in ds.data_vars:
        ds[var].attrs['grid_mapping'] = 'crs'

    vars_to_encode = list(ds.data_vars) + ['x', 'y', 'time']
    encoding_dict = {var: {"zlib": True, "complevel": 5, 'shuffle': True, 'dtype': 'float32', '_FillValue': FILL_VALUE, 'missing_value': FILL_VALUE} for var in vars_to_encode}
    for coord in ['x', 'y', 'time', 'crs']:
        if coord in ds.coords:
            encoding_dict[coord] = {'_FillValue': None}

    ds.to_netcdf(outpath, encoding=encoding_dict, unlimited_dims=['time'])

    # Append time_bnds using netCDF4 directly
    with netCDF4.Dataset(outpath, 'a') as nc:
        if 'nv' not in nc.dimensions:
            nc.createDimension('nv', 2)
        tb = nc.createVariable('time_bnds', 'f8', ('time', 'nv'))
        tb[:] = time_bnds_data
        tb.units = time_units
        tb.calendar = 'standard'
        tb.long_name = 'time bounds'

def add_coords(ds, epsg_code): 
    def create_bounds(x,y,dx,dy):
        x_b = np.append(x - dx/2, x[-1] + dx/2)
        y_b = np.append(y - dy/2, y[-1] + dy/2)
        X_b, Y_b = np.meshgrid(x_b, y_b)
        return X_b,Y_b
    if epsg_code == 3031 or epsg_code==3413:
        proj = pyproj.Proj(f"EPSG:{epsg_code}")
        x = ds['x'].values
        y = ds['y'].values
        dx = x[1] - x[0]
        dy = y[1] - y[0]
        X, Y = np.meshgrid(x, y)
        lon, lat = proj(X, Y, inverse=True)
        X_b, Y_b = create_bounds(x,y,dx,dy)
        lon_b, lat_b = proj(X_b, Y_b, inverse=True)
        return ds.assign_coords({
            "lon": (('y', 'x'), lon),
            "lat": (('y', 'x'), lat),
            "lon_b": (("y_b", "x_b"), lon_b),
            "lat_b": (("y_b", "x_b"), lat_b)
        })
    elif epsg_code == 4326:
        lon = ds['lon'].values
        lat = ds['lat'].values
        d_lon = lon[1] - lon[0]
        d_lat = lat[1] - lat[0]
        lon_b_2d, lat_b_2d = create_bounds(lon,lat,d_lon,d_lat)

        return ds.assign_coords({
            "lon_b": (("lat_b", "lon_b"), lon_b_2d),
            "lat_b": (("lat_b", "lon_b"), lat_b_2d)
        })
    else: raise ValueError(f"ERROR: Functionality for EPSG: {epsg_code} not yet implimented")

def make_regridder(source, target, method, reuse_weights, weights_path, periodic = True):
    return xe.Regridder(
        source, 
        target, 
        method=method,
        unmapped_to_nan=True,
        reuse_weights=reuse_weights,
        filename=weights_path,
        periodic = periodic
    )

def copy_last_year(dirr, var, last_file, months = True):
    print(dirr, var)
    files = []
    for year in np.arange(2290,2300):
        f = glob(f'{dirr}/{var}_*_{year}.nc')[0]
        files.append(f)        

    print(files)   
    ds = xr.open_mfdataset(files, use_cftime=True, chunks={'time': 1})
    if months:
        ds = ds.groupby(ds.time.dt.month).mean().compute()
        ds = ds.rename({'month':'time'})
        ds['time'] = [cftime.datetime(2300, month, 15) for month in range(1, 13)]
    else:
        ds = ds.mean(dim = 'time').compute()
        ds = ds.drop_vars('time')
        new_time = [cftime.datetime(2300, 12, 31)]
        ds = ds.expand_dims({'time': new_time})

    ds.attrs.update({'comment':'Prepared for ISMIP7 by Devon Dunmire using xesmf ddunmire@buffalo.edu/nYear 2300 computed as average of years 2290-2299'})
    save_netdf(ds, dirr+last_file)

def apply_src_mask(ds, var, mask_file):
    """
    Apply the source ice sheet mask to a dataset variable, setting values
    outside the mask to NaN. Does NOT fill NaNs — they are intentionally
    left so the conservative regridder can handle them via renormalization.
    """
    if os.path.exists(mask_file):
        mask = xr.open_dataset(mask_file)
        ds[var] = ds[var].where(mask.mask.values != 0)
        return ds
    else:
        print("please provide a mask file for the source dataset")
        raise ValueError(f"ERROR: Could not find {mask_file}")


def masked_conservative_regrid(regridder, ds, var):

    """
    Conservative regrid that correctly handles NaN/masked source cells via
    renormalization. Avoids artifacts at ice sheet edges by never letting
    off-ice fill values contaminate the area-weighted average.

    Approach: regrid (data * valid) and (valid) separately, then divide.
    This is equivalent to xesmf's skipna but works on any version.
    """
    data = ds[var].values.copy()          # (time, y, x)
    valid = (~np.isnan(data)).astype(float)
    data_filled = np.where(np.isnan(data), 0.0, data)

    # Build two temporary datasets for the pair of regrid calls
    ds_data = ds.copy(deep=True)
    ds_data[var].values[:] = data_filled

    ds_valid = ds.copy(deep=True)
    ds_valid[var].values[:] = valid

    ds_out_data = regridder(ds_data, keep_attrs=True)
    ds_out_valid = regridder(ds_valid, keep_attrs=True)

    weight = ds_out_valid[var].values
    # Where weight > 0, renormalize; elsewhere leave as NaN
    result = ds_out_data[var].values / np.where(weight > 0, weight, np.nan)

    ds_out = ds_out_data.copy(deep=True)
    ds_out[var].values[:] = result
    return ds_out


def fill_nearest_output(ds, var, mask_file):
    """
    After regridding, fill any NaN values that fall within the output mask
    (which includes a small buffer zone beyond the strict ice sheet edge)
    using nearest-neighbor interpolation. Then apply the output mask so
    that everything outside the buffer is set to NaN.
    """
    if os.path.exists(mask_file):
        mask_ds = xr.open_dataset(mask_file)
        output_mask = mask_ds.mask.values  # 1 inside buffer, 0 outside

        data = ds[var].values.copy()  # (time, y, x)
        for t in range(data.shape[0]):
            slice_2d = data[t]
            nan_in_grid = np.isnan(slice_2d)

            # Only bother if there are NaNs inside the output mask
            fill_region = nan_in_grid & (output_mask == 1)
            if np.any(fill_region) and not np.all(nan_in_grid):
                # Find nearest valid neighbor for every NaN cell
                ind = ndimage.distance_transform_edt(
                    nan_in_grid, return_distances=False, return_indices=True
                )
                filled = slice_2d[tuple(ind)]
                # Apply fill only inside the output mask buffer
                data[t] = np.where(fill_region, filled, slice_2d)

        ds[var] = ds[var].copy(data=data)
        # Apply the output mask — zeros out everything outside the buffer
        return ds.where(output_mask == 1)
    else:
        print("please provide mask file for output")
        raise ValueError(f"ERROR: Could not find {mask_file}")
                        
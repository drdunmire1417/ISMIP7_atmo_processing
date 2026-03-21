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

FILL_VALUE = netCDF4.default_fillvals['f4']

def fill_nearest_2d(ds, var):
    """Fills NaNs in a DataArray using 2D nearest neighbor."""
    ds = ds.where(ds[var]!=0)
    da = ds[var]
    data = da.values
    mask = np.isnan(data)
    ind = ndimage.distance_transform_edt(mask, return_distances=False, return_indices=True)
    filled_data = data[tuple(ind)]
    ds[var] = da.copy(data=filled_data)
    return ds

def configure_variables(ds, old_var, new_var):
    try: ds = ds.drop_vars(['lat','lon','lat_b','lon_b'])
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
        cftime.datetime(syear + i, 1, 15) 
        for i in range(num_years)
    ]   
    ds['time'] = np.array(time_array, dtype=object)
    return ds

def update_attributes(ds, var):

    with open('/projects/grid/ghub/ISMIP6/devon/ISMIP_regridding/readmes/var_attributes.json', 'r') as f:
        attr_map = json.load(f)

    if var in attr_map:
        ds[var].attrs = attr_map[var]

    ds.y.attrs.update({
        'long_name': 'Cartesian y-coordinate',
        'standard_name': 'projection_y_coordinate',
        'units': 'meter',
        'axis': 'Y'
    })

    ds.x.attrs.update({
        'long_name': 'Cartesian x-coordinate',
        'standard_name': 'projection_x_coordinate',
        'units': 'meter',
        'axis': 'X'
    })

    ds.attrs.update({
        'comment': 'Prepared for ISMIP7 by Devon Dunmire using xesmf ddunmire@buffalo.edu',
        'source': 'SDBN1 - Brice Noel -  bnoel@uliege.be'
    })
    
    return ds

def mask_output(ds, mask_file):
    mask = xr.open_dataset(mask_file)
    return ds.where(mask.mask==1)

def save_netdf(ds, outpath):
    day1850 = cftime.datetime(1850,1,1)
    time_int = [(t - day1850).days for t in ds.time.values]
    ds['time'] = time_int
    ds.time.attrs.update({
            'standard_name': 'time',
            'long_name': 'time',
            'axis': 'T',
            'units': 'days since 1850-01-01 00:00:00',
            'calendar': 'standard'
        })

    vars_to_encode = list(ds.data_vars) + ['x', 'y', 'time']
    encoding_dict = {var: {'dtype': 'float32', '_FillValue': FILL_VALUE, 'missing_value': FILL_VALUE} for var in vars_to_encode}
    for coord in ['x', 'y', 'time']:
        if coord in ds.coords:
            encoding_dict[coord] = {'_FillValue': None}
    ds.to_netcdf(outpath, encoding=encoding_dict, unlimited_dims=['time'])

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
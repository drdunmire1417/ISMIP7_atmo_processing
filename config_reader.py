import configparser
from dataclasses import dataclass

# Define the structure and expected types of your config
@dataclass
class RegridConfig:
    icesheet: str
    res: int
    weights_path: str
    masks_path: str
    NUM_WORKERS: int
    regrid_scheme: str

    src_dir: str
    src_epsg: int
    gcm: str
    scenario: str
    method: str
    src_mask: str

    out_dir: str
    scratch_dir: str
    var_list: list
    version: int
    
    gradients: bool
    normal: bool

def read_config_file():
    config = configparser.ConfigParser()
    config.read('./config.ini')

    # Regridding information
    weights_dir = config['Regridding']['weights_dir']
    masks_dir = config['Regridding']['masks_dir']
    regrid_scheme = config['Regridding']['regrid_scheme']
    icesheet = config['Regridding']['icesheet']
    if icesheet == 'GIS': 
        icesheet = 'GrIS'
    if icesheet not in ['AIS', 'GrIS']:
        raise ValueError("Ice sheet must be AIS or GrIS")
    if icesheet == 'AIS': res = config.getint('Regridding', 'target_res', fallback=2000)
    elif icesheet == 'GrIS': res = config.getint('Regridding', 'target_res', fallback=1000)
    num_workers = config.getint('Regridding', 'num_workers', fallback=1)
    
    # Source information
    src_dir = config['Source']['src_dir']
    gcm = config['Source']['gcm']
    scenario = config['Source']['scenario']
    method = config['Source']['method']   
    try: 
        src_epsg = int(config['Source']['src_epsg'])
    except ValueError: 
        raise ValueError('Source epsg must be an integer')
    
    # Output information
    scratch_dir = config['Output']['scratch_dir']
    out_dir = config['Output']['output_dir']
    version = config.getint('Output', 'version', fallback=1)
    varlist_str = config.get('Output', 'var_list')
    var_list = [v.strip() for v in varlist_str.split(', ')]
    print('Variables to procces:', var_list)

    #Boolean cases
    normal = config.getboolean('Cases', 'normal', fallback=True)
    gradients = config.getboolean('Cases', 'gradients', fallback=False)

    weights_path = f'{weights_dir}{icesheet}_{method}_{res}_weights.nc'
    masks_path = f'{masks_dir}{icesheet}_mask_ISMIP_{res}.nc'
    src_mask = f'{masks_dir}{icesheet}_mask_{method}.nc'

    # Return the populated Dataclass object directly
    return RegridConfig(
        icesheet=icesheet,
        res=res,
        weights_path=weights_path,
        masks_path=masks_path,
        NUM_WORKERS=num_workers,
        regrid_scheme=regrid_scheme, 

        src_dir=src_dir,
        src_epsg=src_epsg,
        gcm=gcm,
        scenario=scenario,
        method=method,
        src_mask = src_mask,

        out_dir=out_dir,
        scratch_dir=scratch_dir,
        var_list=var_list,
        version=version,
        
        gradients=gradients,
        normal=normal,
    )
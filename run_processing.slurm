#!/bin/sh
#SBATCH --job-name=xesmf_regrid       # Name of the job
#SBATCH --output=log/regrid_%j.log        # Standard output and error log (%j becomes the job ID)
#SBATCH --nodes=1                     # Request 1 node
#SBATCH --ntasks=1                    # Request 1 task (the main Python script)
#SBATCH --cpus-per-task=24             # (MATCH THIS TO NUM_WORKERS!)
#SBATCH --mem=128G                     
#SBATCH --time=02:00:00               
#SBATCH --partition=general-compute
#SBATCH --qos=general-compute
#SBATCH --cluster=ub-hpc

export APPTAINERENV_LD_LIBRARY_PATH="/opt/conda/lib:$LD_LIBRARY_PATH"
export APPTAINERENV_ESMFMKFILE="/opt/conda/lib/esmf.mk"

CONTAINER="/projects/grid/ghub/ISMIP6/devon/cache/conda-x86_64.sif"
SCRIPT="/projects/grid/ghub/ISMIP6/devon/ISMIP_regridding/process_to_ISMIP.py"

# --- Execution ---
echo "Starting job on $(hostname) at $(date)"
apptainer exec --bind /vscratch/grp-ghub/devon-temp/,/projects/grid/ghub/ $CONTAINER python -u $SCRIPT
echo "Job finished at $(date)"
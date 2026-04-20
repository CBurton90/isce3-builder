#!/bin/bash
#SBATCH --job-name=isce3_gpu
#SBATCH --output=logs/slurm-%j.out
#SBATCH --error=logs/slurm-%j.err
#SBATCH --time=03:00:00
#SBATCH --gres=gpu:3g.40gb:1
#SBATCH --cpus-per-task=32
#SBATCH --mem=100G

echo "Job started at $(date)"
cd $SLURM_SUBMIT_DIR

mkdir -p logs

# -------------------------
# DEBUG / INFO
# -------------------------
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
nvidia-smi -L

# -------------------------
# START LOGGING
# -------------------------
echo "Starting CPU logging..."
vmstat -t 1 > logs/cpu_log_${SLURM_JOB_ID}.txt &
CPU_LOG_PID=$!

echo "Starting GPU logging..."
nvidia-smi \
  --query-gpu=timestamp,memory.used,memory.free,utilization.gpu \
  --format=csv -l 1 > logs/gpu_log_${SLURM_JOB_ID}.csv &
GPU_LOG_PID=$!

# -------------------------
# RUN APPTAINER CONTAINER
# -------------------------
echo "Running isce3 inside Apptainer..."

ml apptainer

apptainer exec --nv \
  --pwd /opt \
  --writable-tmpfs \
  --bind ./inputs:/opt/inputs,./outputs:/opt/outputs,./products:/opt/products,./dem:/opt/dem,./configs:/opt/configs,./qa:/opt/qa,./logs:/opt/logs,./patches/geocode_insar.py:/opt/isce3/packages/nisar/workflows/geocode_insar.py \
  isce3cuda.sif \
  bash -c "python3 -u /opt/isce3/packages/nisar/workflows/insar.py configs/insar-cuda-template.yaml > /opt/logs/isce3_log_${SLURM_JOB_ID}.txt 2>&1"

APP_EXIT_CODE=$?

# -------------------------
# CLEANUP LOGGING
# -------------------------
echo "Stopping CPU & GPU logging..."
kill $CPU_LOG_PID
kill $GPU_LOG_PID

wait $CPU_LOG_PID 2>/dev/null
wait $GPU_LOG_PID 2>/dev/null

echo "Job finished at $(date)"
echo "Apptainer exit code: $APP_EXIT_CODE"

exit $APP_EXIT_CODE
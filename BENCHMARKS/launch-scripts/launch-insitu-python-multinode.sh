#!/usr/bin/env bash

# The base from this script is taken from:
# https://github.com/theabm/bench-parflow/blob/main/scripts/run/g5k/start_multinode_doreisa.sh

# set -euo pipefail
set -x

mkdir -p .logs
exec > >(tee -a .logs/run.log) 2>&1

echo "[INFO] Starting at: `date`"

# ============== Resource Allocation ==============

NODES=($(cat "$OAR_NODE_FILE" | uniq))
N_NODES=${#NODES[@]} # count number of nodes ( @ expands array, # counts elems)

echo "[INFO] Execution using ${N_NODES} node(s)"
echo "[INFO] Nodes used: ${NODES[@]}"

HEAD_NODE=${NODES[0]}

if [[ $N_NODES -eq 1 ]]; then
  SIM_NODES=("${NODES[@]:0}")
else
  SIM_NODES=("${NODES[@]:1}")
fi

N_SIM_NODES=${#SIM_NODES[@]}


COMP_CORES=${2:-}
if [[ -z "${COMP_CORES}" ]]; then
    COMP_CORES=20
fi

PORT=4242
HEAD_ADDRESS=${HEAD_NODE}:$PORT
echo "[INFO] Head node (${HEAD_NODE}) at address: ${HEAD_ADDRESS}"
echo "[INFO] Left with ${N_SIM_NODES} simulation node(s): ${SIM_NODES[*]}"

# ============== ENVIRONMENT SETUP ==============

OLDPWD=$(pwd)
PROJECT_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

export OMP_PROC_BIND=spread
export OMP_PLACES=cores

nodes=$N_SIM_NODES

# base from:
# https://github.com/deisa-project/workflow-example/blob/main/launch-scripts/launch-insitu-python-local.sh

# (optional) nicer -x lines with timestamps
export PS4='+ $(date "+%F %T") ${BASH_SOURCE##*/}:${LINENO}: '

cd $PROJECT_ROOT
VENV="$PROJECT_ROOT/../.venv"
# export PYTHONWARNINGS=ignore # We like the warnings

# --- Select Python environment ---
if [[ -d "$VENV" ]]; then
  echo "[INFO] Using Python virtualenv at $VENV"
  export VIRTUAL_ENV="$VENV"
  export PATH="$VENV/bin:$PATH"
  PY="$VENV/bin/python"
else
  echo "[ERROR] No virtualenv found at $VENV. Please create it or insert the correct path." >&2
  exit 1
fi


# ============== Starting Ray Head ==============

start=$(date +%s)


mpirun --host "${HEAD_NODE}":1 \
       -x PATH \
       -x VIRTUAL_ENV \
       bash -c "ray start --head --port=$PORT --num-cpus=60" > .logs/ray-head.log 2>&1

end=$(date +%s)
echo "[INFO] Ray head node started at $(expr $end - $start)/s."

sleep 1 #?????? Well, it will continue here

# ============== Starting Ray Workers ==============


if [ "$N_NODES" -gt 1 ]; then # Only if we have more than one one...
  # Creates one ray instance ('worker') by node
    mpirun --host $(printf "%s:1," "${SIM_NODES[@]}" | sed 's/,$//') \
	   -x PATH \
	   -x VIRTUAL_ENV \
	   bash -c "ulimit -n 65536; ray start --address ${HEAD_ADDRESS} --num-cpus=30" > .logs/ray-workers.log 2>&1

  end=$(date +%s)
  echo "[INFO] Ray workers started and connected to head node at $(expr $end - $start)/s."
  sleep 10
fi

# ============== Starting Analytics ==============

end=$(date +%s)
ANALYTICS_START="$end"
echo "Launching Analytics at $(expr "$end" - "$start")/s."

TYPE=${1:-}
if [[ -z "${TYPE}" ]]; then
    TYPE=avg
fi


mpirun --host "${HEAD_NODE}":1 \
       -x PATH \
       -x VIRTUAL_ENV \
       bash -c "${PY} -m analytics.$TYPE" 2>.logs/analytics.e&

sleep 5

ANALYTICS_PID=$!
echo "[INFO] Analytics PID: $ANALYTICS_PID"

# ============== Starting Simulation ==============

echo "[INFO] Launching Simulation..."

mpirun --merge-stderr-to-stdout \
       -x PATH \
       -x VIRTUAL_ENV \
       -x MASTER_ADDR="${SIM_NODES[0]}" \
       --host $(printf "%s:$COMP_CORES," "${SIM_NODES[@]}" | sed 's/,$//') \
       "$PY" "$PROJECT_ROOT/python/sim-deisa-ray.py" \
       --steps 50 --print-every 0 --seed-mode local --periodic \
       --nx_local 1024 --ny_local 1024

end=$(date +%s)

echo "[INFO] Simulation finished at $(expr $end - $start)/s."

# ============== Finishing ==============

echo "[INFO] Waiting on analytics.."
end=$(date +%s)
wait $ANALYTICS_PID
echo "[INFO] Analytics finished at $(expr $end - $ANALYTICS_START)/s."

mpirun -hostfile $OAR_NODE_FILE \
       -x PATH \
       -x VIRTUAL_ENV \
       bash -c "ray stop" > /dev/null

echo "[INFO] Finishing at: `date`"

cd "$OLDPWD"
# set +eux

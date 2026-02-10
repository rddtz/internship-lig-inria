"""
Simple example of an analytics pipeline using the Doreisa framework and Dask arrays.

This script demonstrates how to:
- Initialize the Doreisa head node
- Define sliding-window array inputs for simulation data (e.g., fields "U" and "V")
- Register a callback function that is called at each timestep of the simulation
- Compute and report basic analytics (average values of V and U over time steps)

"""

import dask.array as da
from deisa.ray.window_handler import Deisa
from deisa.ray.types import WindowSpec, DeisaArray
import deisa.ray as deisa
import random

#deisa.config.enable_experimental_distributed_scheduling(enable_distributed_scheduling)

d = Deisa(n_sim_nodes=1)
random.seed(0)

def simulation_callback(
    V: list[DeisaArray],
    U: list[DeisaArray]
):

    # print(f"Callback triggered for step {U[0].t}", flush=True)
    # # Some computation with dask
    Vavg = V[0].dask.mean().compute()
    Uavg = U[0].dask.mean().compute()
    
    # Saving with 50% chance
    if(random.random() < 0.3):
        import h5py
        with h5py.File(f"data-{U[0].t}", "w") as f:
            f.create_dataset(f"U", data=U[0].dask)
    
    # # Print formatted analytics information for the current step
    # print(f"[ANALYTICS] Average at timestep {
    #       U[0].t}: V={Vavg}, U={Uavg}", flush=True)


# --- Main execution section ---

# Initialize the Doreisa head node
# and registers this process as the analytics controller.
print("Analytics Initialized", flush=True)


d.register_callback(
    simulation_callback,
    [
        WindowSpec("U", window_size=1),
        WindowSpec("V", window_size=1),
    ],
)


d.execute_callbacks()

print("Ending...", flush=True)

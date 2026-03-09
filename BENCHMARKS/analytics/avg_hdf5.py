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
import time
import os

# deisa.config.enable_experimental_distributed_scheduling(True)

d = Deisa()
random.seed(0)

def simulation_callback(
    V: list[DeisaArray],
    U: list[DeisaArray]
):

    start = time.perf_counter()
    U[0].to_hdf5(f"data-{U[0].t}.h5", "data")
    print(f"[ANALYTICS]: to_hdf5,{U[0].t},{time.perf_counter() - start}", flush=True)


    os.system(f"rm -f data*.h5 .data*.h5")
    
    
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

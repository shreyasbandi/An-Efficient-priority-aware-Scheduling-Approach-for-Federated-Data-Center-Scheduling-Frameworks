import os
import json
import sys

num_nodes = int(sys.argv[1])
num_partitions = int(sys.argv[2])
num_lms = int(sys.argv[3])
  # This value is always equal to the number of GMs

assert num_nodes % (num_lms * num_partitions) == 0, ("Nodes cannot be equally "
                                                     "divided amongst all "
                                                     "partitions")
partition_size = num_nodes // (num_lms * num_partitions)

cluster_config = {}
cluster_config["LMs"] = {}

for i in range(1, num_lms + 1):
    lm_id = str(i)
    cluster_config["LMs"][lm_id] = {"LM_id": str(i), "partitions": {}}
    part_dict = {}
    for j in range(1, num_partitions + 1):
        part_dict[str(j)] = {}
        partition_bit_string='0b'
        
        for k in range(0, partition_size):
            partition_bit_string+='1'

        part_dict[str(j)] = [partition_bit_string]

    cluster_config["LMs"][lm_id]["partitions"] = part_dict

string = json.dumps(cluster_config, indent=4)
config_file_path = os.path.join("config_G"+str(num_partitions)+"L"+str(num_lms)+"S"+str(num_nodes)+".json")
with open(config_file_path, "w") as file_handler:
        file_handler.write(f"{string}\n")

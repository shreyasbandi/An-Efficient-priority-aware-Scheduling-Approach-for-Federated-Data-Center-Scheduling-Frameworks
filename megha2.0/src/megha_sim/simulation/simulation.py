import pickle
import queue
import json
from typing import Dict


from local_master import LM
from global_master import GM
from job import Job
from simulation_logger import SimulationLogger
from simulator_utils.values import TaskDurationDistributions
from events import JobArrivalEvent, LMRequestUpdateEvent, InconsistencyEvent
from simulator_utils import debug_print

import os
import sys

WORKLOAD_FILE= sys.argv[1]

WORKLOAD_FILE_NAME = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
                                      .split("_")[-1])

logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


class Simulation(object):
    def __init__(
            self,
            workload,
            config,
            NUM_CONSTRAINTS
           ):

        # Each localmaster has one partition per global master so the total number of partitions in the cluster are:
        # NUM_GMS * NUM_LMS
        # Given the number of worker nodes per partition is PARTITION_SIZE
        # so the total_nodes are NUM_GMS*NUM_LMS*PARTITION_SIZE
        self.config = json.load(open(config))
        self.WORKLOAD_FILE = workload
        self.NUM_LMS=len(self.config["LMs"])
        
        self.NUM_GMS=len(self.config["LMs"]["1"]["partitions"]) 
       

        self.PARTITION_SIZE=len(self.config["LMs"]["1"]["partitions"]["1"][0])-2 #(-2 for the "0b" string in the constraint vector)
        self.total_nodes = self.NUM_GMS * self.NUM_LMS * self.PARTITION_SIZE
        self.NUM_CONSTRAINTS: int = NUM_CONSTRAINTS
        self.task_occurrence_type={} #weights for generating task placement constraints
        self.jobs = {}
        self.event_queue = queue.PriorityQueue()
        self.job_queue=[]
        self.gm_counter=0
        # initialise GMs
        self.gms = {}
        counter = 1
        while len(self.gms) < self.NUM_GMS:
            self.gms[str(counter)] = GM(self, str(counter), NUM_CONSTRAINTS, pickle.loads(
                pickle.dumps(self.config)))  # create deep copy
            counter += 1

        # initialise LMs
        self.lms: Dict[str, LM] = {}
        counter = 1

        while len(self.lms) < self.NUM_LMS:
            self.lms[str(counter)] = LM(self,
                                        str(counter),
                                        self.PARTITION_SIZE,
                                        self.NUM_CONSTRAINTS,
                                        pickle.loads(
                                            pickle.dumps(self.config["LMs"][str(counter)])))  # create deep copy

            # debug_print(f"LM - {counter} "
            #             f"{self.lms[str(counter)].get_free_cpu_count_per_gm()}")
            counter += 1

        self.shared_cluster_status = {}

        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False

     
        # print("Simulation instantiated")        
        
            
 # Simulation class
    def run(self):
        last_time = 0
    
        self.jobs_file = open(self.WORKLOAD_FILE, 'r')

        self.task_distribution = TaskDurationDistributions.FROM_FILE

        line = self.jobs_file.readline()  # first job
        new_job = Job(self.task_distribution, line, self)
        # starting the periodic LM updates
        self.event_queue.put((float(line.split()[0])-0.05, LMRequestUpdateEvent(self)))
        self.event_queue.put((float(line.split()[0]), JobArrivalEvent(
            self, self.task_distribution, new_job, self.jobs_file)))
        
        
        self.jobs_scheduled = 1

        # start processing events
        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            if(new_events is None):
                continue
            for new_event in new_events:
                if(new_event is None):
                    continue
                self.event_queue.put(new_event)

        # print("Simulation ending, no more events")
        # logger.info("Simulator Info , Simulation ending, no more events")
        self.jobs_file.close()

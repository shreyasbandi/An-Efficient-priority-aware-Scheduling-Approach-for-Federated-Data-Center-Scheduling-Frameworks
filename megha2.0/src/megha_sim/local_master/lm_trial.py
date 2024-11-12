import json
from typing import List, Tuple
from bitstring import BitArray
from simulator_utils.values import NETWORK_DELAY, InconsistencyType
import simulator_utils.globals
from simulator_utils import debug_print
from events import LaunchOnNodeEvent,LMStatusUpdateEvent, BatchedInconsistencyEvent,InconsistencyEvent, LMRequestUpdateEvent, TaskResponseEvent
from megha_sim.global_master.gm_types import LMResources
import pickle

class LM(object):

    def __init__(self, simulation, LM_id, partiton_size, NUM_CONSTRAINTS, LM_config):
        self.LM_id = LM_id
        self.partiton_size = partiton_size
        self.LM_config = {}
        self.changed_since_last_update=False
        self.LM_config["LM_id"]=LM_config["LM_id"]
        self.LM_config["partitions"]={}

        for partition_id in LM_config["partitions"]:
            self.LM_config["partitions"][partition_id]=BitArray(LM_config["partitions"][partition_id][0])

        
        self.NUM_CONSTRAINTS=NUM_CONSTRAINTS
        debug_print(f"LM {LM_id} initialised")
        self.simulation = simulation
        # we hold the key-value pairs of the list of tasks completed (value)
        # for each GM (key)
        self.tasks_completed = {}
        for GM_id in self.simulation.gms:
            self.tasks_completed[GM_id] = []

    def get_status(self) -> Tuple[LMResources,
                                      List[Tuple[str, str]]]:
        
        status_update=dict()
        for partition_id in self.LM_config["partitions"]:
            partition=self.LM_config["partitions"][partition_id]
            status_update[partition_id]=list()
            for index in range(0,len(partition)):
                if partition[index]:
                    status_update[partition_id].append(index)
        response = pickle.loads(pickle.dumps(status_update))
        return response

    def send_status_update(self,current_time):
        self.simulation.event_queue.put(
                    (current_time + NETWORK_DELAY, LMStatusUpdateEvent(self.get_status(), self.simulation,self)))


    # LM checks if GM's request is valid
    def verify_request(
            self,
            task,
            gm,
            node_id,
            current_time,
            external_partition=None,
            # setnode=False
            # 
            ):
        node_id=int(node_id)
        # debug_print(f"LM {self.LM_id} verifying request")
        # check if repartitioning

        # if setnode:
        #     self.LM_config["partitions"][task.partition_id][node_id]= True


        if(external_partition is not None):
            if (self.LM_config["partitions"][external_partition]
                    [node_id]):
                
                self.LM_config["partitions"][external_partition][node_id] = False
                task.node_id = node_id
                task.partition_id = external_partition
                task.lm = self
                task.GM_id = gm.GM_id
                
                # network delay as the request has to be sent from the LM to
                # the selected worker node
                
                self.simulation.event_queue.put(
                    (current_time + NETWORK_DELAY, LaunchOnNodeEvent(task, self.simulation)))
                return True
            else:  # if inconsistent
                simulator_utils.globals.inconsistencies+=1
                task.partition_id = gm.GM_id
                task.GM_id = gm.GM_id
                task.lm = self
                # print(self.LM_id,":InconsistencyEvent. node:",task.node_id," job/task:",task.job.job_id,"/",task.task_id)
                self.simulation.event_queue.put((current_time+ NETWORK_DELAY, InconsistencyEvent(
                    task, gm,self, InconsistencyType.EXTERNAL_INCONSISTENCY, self.get_status(),self.simulation)))
        # internal partition
        else:
            if (self.LM_config["partitions"][gm.GM_id][node_id]):

                # allot node to task
                self.LM_config["partitions"][gm.GM_id][node_id]= False
                task.node_id = node_id
                task.partition_id = gm.GM_id
                task.GM_id = gm.GM_id
                task.lm = self
                
                self.simulation.event_queue.put(
                    (current_time + NETWORK_DELAY,
                     LaunchOnNodeEvent(task,
                                       self.simulation)))
            else:  # if inconsistent
                # print(self.LM_id,":InconsistencyEvent. node:",task.node_id," job/task:",task.job.job_id,"/",task.task_id)
                simulator_utils.globals.inconsistencies+=1
                task.partition_id = gm.GM_id
                task.GM_id = gm.GM_id
                task.lm = self
                
                self.simulation.event_queue.put((current_time+ NETWORK_DELAY, InconsistencyEvent(
                    task, gm,self, InconsistencyType.INTERNAL_INCONSISTENCY,self.get_status(), self.simulation)))

    def verify_requests(
            self,
            task_mappings,
            gm,
            current_time):

        #LM_config["partitions"][task.partition_id][task.node_id]

        inconsistent_mappings=[]
        for task_mapping in task_mappings:
            task=task_mapping["task"]
            node_id=int(task.node_id)
            if "external_partition" in task_mapping: # repartition
                external_partition=task_mapping["external_partition"]
                if (self.LM_config["partitions"][external_partition]
                    [node_id]):
                    self.LM_config["partitions"][external_partition][node_id] = False
                    task.node_id = node_id
                    task.partition_id = external_partition
                    task.lm = self
                    task.GM_id = gm.GM_id
                    self.simulation.event_queue.put(
                        (current_time + NETWORK_DELAY, LaunchOnNodeEvent(task, self.simulation)))
                else:
                    simulator_utils.globals.inconsistencies+=1
                    inconsistent_mappings.append(task_mapping)   
            else:
                if (self.LM_config["partitions"][gm.GM_id][node_id]):
                    
                    # allot node to task
                    self.LM_config["partitions"][gm.GM_id][node_id]= False
                    task.node_id = node_id
                    task.partition_id = gm.GM_id
                    task.GM_id = gm.GM_id
                    task.lm = self
                    self.simulation.event_queue.put(
                        (current_time + NETWORK_DELAY,
                         LaunchOnNodeEvent(task,
                                           self.simulation)))
                else:
                    simulator_utils.globals.inconsistencies+=1
                    inconsistent_mappings.append(task_mapping)   
        if inconsistent_mappings:
            self.simulation.event_queue.put((current_time+ NETWORK_DELAY, BatchedInconsistencyEvent(
                    inconsistent_mappings, gm, self, self.simulation,self.get_status())))

    def task_completed(self, task):
        
        # reclaim resources
        # print("Before: Task completed:",task.job.job_id,task.task_id,"GM:",task.partition_id, "LM:",self.LM_id,"count:",self.LM_config["partitions"][task.partition_id].count(1))
        self.LM_config["partitions"][task.partition_id][task.node_id]= True
        # print("After: Task completed:",task.job.job_id,task.task_id,"GM:",task.partition_id, "LM:",self.LM_id, "count:",self.LM_config["partitions"][task.partition_id].count(1))
        # Append the details of the task that was just completed to the list of
        # tasks completed for the corresponding GM that sent it
        # note GM_id used here, not partition, in case of repartitioning
        # print(self.LM_id,"Task completed. node:",task.node_id," job/task:",task.job.job_id,"/",task.task_id)
        self.simulation.event_queue.put((task.end_time + NETWORK_DELAY,
                                         TaskResponseEvent(
                                             self.simulation,
                                             self.simulation.gms[task.GM_id],task)))

    def preempt_task(self,task):
        self.LM_config["partitions"][task.partition_id][task.node_id]= True

   
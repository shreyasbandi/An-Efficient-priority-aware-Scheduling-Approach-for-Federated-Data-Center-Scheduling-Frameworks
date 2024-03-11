"""
File containing the implementation of the Global master.

The file contains the implementation of the Global master module of the \
modified scheduler architecture.
"""

from __future__ import annotations
import json
import random
from typing import List, Dict, TYPE_CHECKING
from bitstring import BitArray

import simulator_utils.globals
from simulator_utils import debug_print
from simulator_utils.values import (LM_HEARTBEAT_INTERVAL, NETWORK_DELAY,
                                    InconsistencyType,
                                    TaskDurationDistributions)
from events import VerifyRequestEvent,VerifyRequestsEvent
from simulation_logger import (SimulationLogger, MATCHING_LOGIC_MSG,
							   CLUSTER_SATURATED_MSG,
							   MATCHING_LOGIC_REPARTITION_MSG)
from .gm_types import (PartitionKey, LMResources, ConfigFile,
					   OrganizedPartitionResources, NodeResources,
					   PartitionResources)
from watchpoints import watch
# Imports used only for type checking go here to avoid circular imports
if TYPE_CHECKING:
	from job import Job
	from local_master import LM

# Seed the random number generator
# random.seed(47)

import os
import sys
WORKLOAD_FILE: Final[str] = sys.argv[1]

WORKLOAD_FILE_NAME: Final[str] = (os.path.basename(WORKLOAD_FILE)
                                      .split(".")[0])

logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()


class GM:
	"""
	Class defining the implementation of the Global Master.

	This class provides the implementation of the different \
	interfaces provided by the Global Master to the rest of \
	the scheduler architecture.
	"""

	# Starting value for the seeds for the Random objects
	SEED_VALUE = 13

	def __init__(self, simulation, GM_id: str, NUM_CONSTRAINTS, config: ConfigFile):
		self.GM_id = GM_id
		self.jobs={}
		self.simulation = simulation
		self.RR_counter: int = 0
		# self.global_view: Dict[str, LMResources] = {}
		self.task_queue: List[Job] = []
		self.jobs_scheduled: List[Job] = []
		self.NUM_CONSTRAINTS=NUM_CONSTRAINTS
		self.random_obj = random.Random()
		self.random_obj.seed(GM.SEED_VALUE)
		GM.SEED_VALUE += 13

		
		self.internal_available_nodes = dict()
		self.external_available_nodes = dict()
		
		
		self.LMs_list=list(config["LMs"].keys())
		self.GMs_list=list(config["LMs"]["1"]["partitions"].keys())

		
		for LM_id in config["LMs"]:
			self.internal_available_nodes[LM_id]=list()
			self.external_available_nodes[LM_id]=dict()
			for partition_id in config["LMs"][LM_id]["partitions"]:

				if partition_id==self.GM_id:
					for i in range(0,len(config["LMs"][LM_id]["partitions"][partition_id][0])-2):#to get rid of 0b
						self.internal_available_nodes[LM_id].append(str(i))
				else:
					self.external_available_nodes[LM_id][partition_id]=list()
					for i in range(0,len(config["LMs"][LM_id]["partitions"][partition_id][0])-2):#to get rid of 0b
						self.external_available_nodes[LM_id][partition_id].append(str(i))
		debug_print(f"GM {self.GM_id} initialised")


	#called on arrival of jobs. Batches resource-task requests and sends to the LM
	def schedule_job_batched_all(self, job, current_time):
		simulator_utils.globals.jobs_arrived+=1
		job.gm = self
		job_id=job.job_id
		self.jobs[job_id]=job
		no_resources=False
		task_mapping_request_batch=[] # holds all requests per LM
		prev_LM=None

		for task_id in self.jobs[job_id].tasks:
			#if previous task placement was unsuccessful add tasks to task queue
			if no_resources:
				self.task_queue.insert(0,self.jobs[job_id].tasks[task_id])
				continue

			task_mapping_request=self.schedule_task(current_time,self.jobs[job_id].tasks[task_id])
			if not task_mapping_request:
				task_mapping_request=self.batched_repartition(current_time,self.jobs[job_id].tasks[task_id])
				if not task_mapping_request:
					no_resources=True
					if task_mapping_request_batch:
						self.simulation.event_queue.put(
							(current_time+NETWORK_DELAY, VerifyRequestsEvent(
							task_mapping_request_batch,
							self,
							self.simulation.lms[prev_LM],
							)))
					task_mapping_request_batch=[]
					self.task_queue.insert(0,self.jobs[job_id].tasks[task_id])
					continue
			if prev_LM == task_mapping_request["LM_id"]:
				task_mapping_request_batch.append(task_mapping_request)
			elif prev_LM is None:
				prev_LM=task_mapping_request["LM_id"]
				task_mapping_request_batch.append(task_mapping_request)
			else:
				self.simulation.event_queue.put(
					(current_time+NETWORK_DELAY, VerifyRequestsEvent(
					task_mapping_request_batch,
					self,
					self.simulation.lms[prev_LM],
					)))
				prev_LM=task_mapping_request["LM_id"]
				task_mapping_request_batch=[]
				task_mapping_request_batch.append(task_mapping_request)

			if len(task_mapping_request_batch)==100:
				self.simulation.event_queue.put(
					(current_time+NETWORK_DELAY, VerifyRequestsEvent(
					task_mapping_request_batch,
					self,
					self.simulation.lms[prev_LM],
					)))
				prev_LM=None
				task_mapping_request_batch=[]

		#if resources available, entire job scheduled. Send batch
		if not no_resources and task_mapping_request_batch:
			self.simulation.event_queue.put(
					(current_time+NETWORK_DELAY, VerifyRequestsEvent(
					task_mapping_request_batch,
					self,
					self.simulation.lms[prev_LM],
					)))

	def schedule_task(self, current_time: float,task):
		"""
		Search the internal partitions of the GM to find a free worker node.

		Args:
			current_time (float): The current time in the simulation.
			task : Task to be scheduled
		"""
		
		
		available_LMpartition=False
		
		for LM_id in self.LMs_list:
			if len(self.internal_available_nodes[LM_id])>0:
				available_LMpartition=LM_id
				break

		if not available_LMpartition:
			return None

		node_id=self.internal_available_nodes[available_LMpartition].pop(0)
		job=task.job
		task_id=task.task_id
		task.node_id=node_id
		
		job.tasks[task_id].scheduling_attempts+=1

		logger.info(f"{MATCHING_LOGIC_MSG} , "
					f"{self.GM_id}_{available_LMpartition}_{node_id} , "
					f"{job.job_id}_{task_id}")
		simulator_utils.globals.scheduling_attempts+=1
		return {"task":task,"LM_id":available_LMpartition}

	
	#search for nodes in external partitions
	def batched_repartition(self,current_time: float, task):
		available_partition=False
		for LM_id in self.LMs_list:
			for GM_id in self.GMs_list:
				if GM_id==self.GM_id:
					continue
				if len(self.external_available_nodes[LM_id][GM_id])>0:
					available_partition=(LM_id,GM_id)
					break

		if not available_partition:
			return None
		else:
			simulator_utils.globals.scheduling_attempts+=1
			task_id=task.task_id
			job=task.job
			node_id=self.external_available_nodes[available_partition[0]][available_partition[1]].pop(0)
			job.tasks[task_id].communication_delay+=NETWORK_DELAY
			job.tasks[task_id].repartitions+=1
			task.node_id=node_id
			job.tasks[task_id].scheduling_attempts+=1
			return{"task":task,"LM_id":available_partition[0],"external_partition":available_partition[1]}


	#This function is for testing purposes only
	def repartition(self, current_time: float,task):
		"""
		Search the external partitions for a free worker node.

		Args:
			current_time (float): The current time in the simulation.
		"""
		
		available_partition=False
		for LM_id in self.LMs_list:
			for GM_id in self.GMs_list:
				if GM_id==self.GM_id:
					continue
				if len(self.external_available_nodes[LM_id][GM_id])>0:
					available_partition=(LM_id,GM_id)
					break

		if not available_partition:
			return None
		
		task_id=task.task_id
		job=task.job
		simulator_utils.globals.scheduling_attempts+=1
		node_id=self.external_available_nodes[available_partition[0]][available_partition[1]].pop(0)
		job.tasks[task_id].communication_delay+=NETWORK_DELAY
		job.tasks[task_id].repartitions+=1
		task.node_id=node_id
	
	
		logger.info(f"{MATCHING_LOGIC_REPARTITION_MSG} , "
					f"{available_partition[1]}_{available_partition[0]}_{node_id} , "
					f"{job.job_id}_{task.task_id}")
		job.tasks[task_id].scheduling_attempts+=1

		# May need to add processing overhead here if required
		self.simulation.event_queue.put(
			(current_time+NETWORK_DELAY,
				VerifyRequestEvent(
					job.tasks[task_id],
					self,
					self.simulation.lms[available_partition[0]],
					node_id,
					external_partition=available_partition[1])))
		return available_partition[0]

	#communication from LM saying task completed execution
	def receive_task_response(self,current_time: float, task: Task):
		
		job_id=task.job.job_id
		job=self.jobs[job_id]
		if task in job.completed_tasks:
			print("Error. Duplication.", task.task_id,job.job_id)
			exit()
		job.completed_tasks.append(task)
		print(current_time,"Task completion:",job_id,"/",task.task_id,self.GM_id)
		if len(job.tasks) == len(job.completed_tasks):  # no more tasks left
			# NOTE:job completion time = end time of last task
			# === max of the task duration for a job
			assert task.end_time is not None
			assert job.completion_time is not None
			job.completion_time = current_time
			job.end_time = job.completion_time
			print(current_time,",JC,",job_id,",",job.completion_time-job.start_time-job.ideal_completion_time)
			logger.info(
				f"{current_time} , "
				"TaskUpdateStatusForGM , "
				f"{job.job_id} , "
				f"{(job.completion_time - job.start_time) - job.ideal_completion_time}")
			assert ((job.completion_time - job.start_time)
					) >= 0, ("jct-st "
							 f"{(job.completion_time - job.start_time)}"
							 " is negative")
			assert ((job.completion_time - job.start_time) -
					job.ideal_completion_time) >= 0, (f"{(job.completion_time - job.start_time) - job.ideal_completion_time} delay is negative")
			

			simulator_utils.globals.jobs_completed.append(job)
		
		#match free resource to pending task to reduce complexity
		if(self.task_queue):
			new_task=self.task_queue.pop(0)
			new_job=new_task.job
			key = PartitionKey(gm_id=task.partition_id, lm_id=task.lm.LM_id)
			new_job.tasks[new_task.task_id].communication_delay+=NETWORK_DELAY
			# if internal partition node
			logger.info(f"{MATCHING_LOGIC_MSG} , "
						f"{task.partition_id}_{task.lm.LM_id}_{task.node_id} , "
					f"{new_job.job_id}_{new_task.task_id}")
			new_task.scheduling_attempts+=1
			simulator_utils.globals.scheduling_attempts+=1
			self.simulation.event_queue.put(
				(current_time+NETWORK_DELAY, VerifyRequestEvent(
					new_task,
					self,
					self.simulation.lms[task.lm.LM_id],
					task.node_id)))	
			return

		#free resources
		#else - no tasks waiting in the queue

		if(task.partition_id==self.GM_id):
			self.internal_available_nodes[task.lm.LM_id].append(task.node_id)
		else:
			self.external_available_nodes[task.lm.LM_id][task.partition_id].append(task.node_id)

	#update sent from LM
	def update_status(self,lm:LM,latest_LM_config,current_time):
		LM_id=lm.LM_id
		for partition_id in latest_LM_config:
			partition=latest_LM_config[partition_id]
			self.random_obj.shuffle(partition)
			if partition_id==self.GM_id:
				self.internal_available_nodes[partition_id]=partition
			else:
				self.external_available_nodes[LM_id][partition_id]=partition

	# in case of an inconsistency event
	def unschedule_task(self, unverified_task: Task):
		"""
		Job is inserted back into the task_queue of the GM.
		"""
		# simulator_utils.globals.scheduling_attempts-=1
		self.task_queue.insert(0,unverified_task)
		
				

	
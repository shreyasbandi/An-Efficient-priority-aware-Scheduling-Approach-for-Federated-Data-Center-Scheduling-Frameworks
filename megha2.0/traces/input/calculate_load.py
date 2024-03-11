import sys
import os
import math
import statistics
import numpy as np
files=[]

subtrace_name=sys.argv[1]
load={"max_util":0,"min_util":0,"median_util":0,"mean_util":0,"90th%":0,"99th%":0,"95th%":0,"util":[]}
subtrace=open(subtrace_name).readlines()
start_time=float(subtrace[0].strip().split()[0])
#find max of last job's task durations
last_job=subtrace[-1].strip().split()
last_job_arrival=float(last_job[0])
last_job_duration=-1
for i in last_job[2:]:
	if float(i)>last_job_duration:
		last_job_duration=float(i)
end_time=last_job_arrival+last_job_duration
duration=math.ceil(end_time-start_time)
util=[0]*duration

for job in subtrace:
	job_req=job.strip().split()
	arrival_time=float(job_req[0])
	relative_arrival_time=arrival_time-start_time
	num_tasks=int(job_req[1])
	tasks=job_req[2:]
	for task in tasks:
		task_duration=float(task)
		ceil_arrival=math.ceil(relative_arrival_time)
		floor_ending=math.floor(relative_arrival_time+task_duration)
		for i in range(ceil_arrival,floor_ending):
			try:
				util[i]+=1
			except IndexError:
				util=util+[0]
				util[i]+=1
		#take care of the ends:
		
		util[math.floor(relative_arrival_time)]+=ceil_arrival-relative_arrival_time
		try:
			util[math.floor(relative_arrival_time+task_duration)]+=relative_arrival_time+task_duration-floor_ending
		except IndexError:
			util=util+[0]
			util[math.floor(relative_arrival_time+task_duration)]+=relative_arrival_time+task_duration-floor_ending

load={"max_util":max(util),"min_util":min(util),"median_util":statistics.median(util),"mean_util":statistics.mean(util),"90th%":np.percentile(util,90),"99th%":np.percentile(util,99),"95th%":np.percentile(util,95),"util":util}
print(load)
subtraces_stats=open(subtrace_name+"_stats.txt","w")
subtraces_stats.write(str(load))






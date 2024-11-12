# """
# Program to run the simulator based on the parameters provided by the user.

# This program uses the `megha_sim` module to run the simulator as per the
# Megha architecture and display/log the actions and results of the simulation.
# """

# import os
# import sys
# import time
# from typing_extensions import Final
# import cProfile
# import pstats
# import io
# from pstats import SortKey


# from megha_sim import Simulation, simulator_globals
# from megha_sim import (SimulationLogger,
#                        debug_print,
#                        DEBUG_MODE)

# if __name__ == "__main__":
#     WORKLOAD_FILE: Final[str] = sys.argv[1]
#     CONFIG_FILE: Final[str] = sys.argv[2]
#     NUM_CONSTRAINTS: Final [int] = int(sys.argv[3])
    


#     WORKLOAD_FILE_NAME: Final[str] = "subtrace_"+(os.path.basename(WORKLOAD_FILE)
#                                       .split("_")[-1])

#     logger = SimulationLogger(__name__,WORKLOAD_FILE_NAME).get_logger()

#     logger.metadata(f"Analysing logs for trace file: {WORKLOAD_FILE_NAME}")
    
#     logger.metadata("Simulator Info , Received CMD line arguments.")

#     NETWORK_DELAY = 0.005  # same as sparrow

#     # This is not the simulation's virtual time. This is just to
#     # understand how long the program takes
#     t1 = time.time()
#     s = Simulation(WORKLOAD_FILE, CONFIG_FILE, NUM_CONSTRAINTS)
    
#     # print("Simulator Info , Simulation running")
#     logger.metadata("Simulator Info , Simulation running")
    
#     # if DEBUG_MODE:
#     #     pr = cProfile.Profile()
#     #     pr.enable()

#     s.run()

#     # if DEBUG_MODE:
#     #     pr.disable()
#     #     text = io.StringIO()
#     #     sortby = SortKey.CUMULATIVE
#     #     ps = pstats.Stats(pr, stream=text).sort_stats(sortby)
#     #     ps.print_stats()
#     #     print(text.getvalue())

#     time_elapsed = time.time() - t1
#     print("Simulation ended in ", time_elapsed, " s ")
#     # logger.metadata(f"Simulation ended in {time_elapsed} s ")

#     # debug_print(simulator_globals.jobs_completed)

#     # print(f"Number of Jobs completed: {len(simulator_globals.jobs_completed)}")
#     logger.metadata(
#         "Simulator Info , Number of Jobs completed: "
#         f"{len(simulator_globals.jobs_completed)}")

#     logger.integrity()
#     logger.flush()
#     print("Jobs completed:",len(simulator_globals.jobs_completed))
#     # print(simulator_globals.jobs_arrived)
#     # print(simulator_globals.scheduling_attempts)
#     print("Inconsistencies:",simulator_globals.inconsistencies)
#     # print("Simulator Info , Simulation ended")


import os
import sys
import time
from typing_extensions import Final
import matplotlib.pyplot as plt  # Importing Matplotlib for plotting
import simulator_utils.globals
from megha_sim import Simulation, simulator_globals
from megha_sim import SimulationLogger, debug_print, DEBUG_MODE
import numpy as np

if __name__ == "__main__":
    WORKLOAD_FILE: Final[str] = sys.argv[1]
    CONFIG_FILE: Final[str] = sys.argv[2]
    NUM_CONSTRAINTS: Final[int] = int(sys.argv[3])

    WORKLOAD_FILE_NAME: Final[str] = "subtrace_" + (os.path.basename(WORKLOAD_FILE).split("_")[-1])

    logger = SimulationLogger(__name__, WORKLOAD_FILE_NAME).get_logger()

    logger.metadata(f"Analysing logs for trace file: {WORKLOAD_FILE_NAME}")

    logger.metadata("Simulator Info , Received CMD line arguments.")

    NETWORK_DELAY = 0.005  # same as sparrow

    # This is not the simulation's virtual time. This is just to
    # understand how long the program takes
    t1 = time.time()
    s = Simulation(WORKLOAD_FILE, CONFIG_FILE, NUM_CONSTRAINTS)

    logger.metadata("Simulator Info , Simulation running")

    s.run()

    time_elapsed = time.time() - t1

    # Write completion time to a different file
    

    print(f"Number of Jobs completed: {len(simulator_globals.jobs_completed)}")
    logger.metadata(
        "Simulator Info , Number of Jobs completed: "
        f"{len(simulator_globals.jobs_completed)}")

    logger.integrity()
    logger.flush()

    # Record baseline metrics
    baseline_metrics = {
        "Completion Time": time_elapsed,
        "Jobs Completed": len(simulator_globals.jobs_completed),
        "Inconsistencies": simulator_globals.inconsistencies
    }

    # Calculate average execution time of each job
    total_execution_time = sum(job.end_time - job.start_time for job in simulator_globals.jobs_completed)
    average_execution_time = total_execution_time / len(simulator_globals.jobs_completed)
    baseline_metrics["Average Execution Time"] = average_execution_time

    # Calculate the percentage of long and short jobs
    num_long_jobs = sum(1 for job in simulator_globals.jobs_completed if not job.is_short)
    num_short_jobs = len(simulator_globals.jobs_completed) - num_long_jobs
    total_jobs = len(simulator_globals.jobs_completed)
    long_job_percentage = (num_long_jobs / total_jobs) * 100
    short_job_percentage = (num_short_jobs / total_jobs) * 100

    # Write baseline metrics to a file


    #print("Baseline metrics recorded and saved in", baseline_file)
    short_job_delays = [job.completion_time-job.start_time - job.ideal_completion_time for job in simulator_globals.jobs_completed if job.is_short]
    long_job_delays = [job.completion_time-job.start_time- job.ideal_completion_time for job in simulator_globals.jobs_completed if not job.is_short]
    # Write job details including average execution time to completion_time.txt
    


# Calculate average response time
    total_response_time = sum(job.end_time - job.start_time for job in simulator_globals.jobs_completed)
    average_response_time = total_response_time / len(simulator_globals.jobs_completed)

# Calculate resource utilization (assuming CPU utilization)
    total_execution_time = sum(job.end_time - job.start_time for job in simulator_globals.jobs_completed)
    total_simulation_time = sum(job.end_time - job.start_time for job in simulator_globals.jobs_completed)
    resource_utilization = total_execution_time / total_simulation_time

# Calculate queue length (average number of jobs waiting in the queue)
    total_waiting_time = sum(job.start_time - job.start_time for job in simulator_globals.jobs_completed)
    average_waiting_time = total_waiting_time / len(simulator_globals.jobs_completed)

# Calculate fairness (Jain's fairness index)
    job_completion_times = [job.end_time for job in simulator_globals.jobs_completed]
    average_completion_time = sum(job_completion_times) / len(job_completion_times)
    fairness_numerator = sum((job.end_time - job.start_time - average_completion_time)**2 for job in simulator_globals.jobs_completed)
    fairness_denominator = len(simulator_globals.jobs_completed) * average_completion_time**2
    fairness_index = fairness_numerator / fairness_denominator

# Calculate inconsistencies
    inconsistencies = simulator_globals.inconsistencies

#Print or log the calculated metrics
    # print("Performance Metrics:")
    # print(f"Completion Time: {completion_time} seconds")
    # print(f"Throughput: {throughput} jobs per second")
    # print(f"Average Response Time: {average_response_time} seconds")
    # print(f"Resource Utilization: {resource_utilization}")
    # print(f"Average Queue Length: {average_waiting_time}")
    # print(f"Fairness Index: {fairness_index}")
    # print(f"Inconsistencies: {inconsistencies}")  
    

    import numpy as np

# Separate short and long jobs
    short_job_response_times = []
    long_job_response_times = []
    all_job_response_times = []

    for job in simulator_globals.jobs_completed:
        response_time = job.end_time - job.start_time
        all_job_response_times.append(response_time)
        if job.is_short:
            short_job_response_times.append(response_time)
        else:
            long_job_response_times.append(response_time)

# Calculate median and 99th percentile response times for short jobs if the array is not empty

# Separate short and long jobs
    short_job_response_times = []
    long_job_response_times = []
    all_job_response_times = []

    for job in simulator_globals.jobs_completed:
        response_time = round(job.end_time - job.start_time, 2)
        all_job_response_times.append(response_time)
        if job.is_short:
            short_job_response_times.append(response_time)
        else:
            long_job_response_times.append(response_time)

# Calculate percentiles for short jobs if the array is not empty
    if short_job_response_times:
        short_job_percentiles = np.percentile(short_job_response_times, [90, 95, 99])
    else:
        short_job_percentiles = [float('nan')] * 3

# Calculate percentiles for long jobs if the array is not empty
    if long_job_response_times:
        long_job_percentiles = np.percentile(long_job_response_times, [90, 95, 99])
    else:
        long_job_percentiles = [float('nan')] * 3

# Calculate percentiles for overall if the array is not empty
    if all_job_response_times:
        overall_percentiles = np.percentile(all_job_response_times, [90, 95, 99])
    else:
        overall_percentiles = [float('nan')] * 3

# Print or log the calculated metrics
    print("Short Job Metrics:")
    print(f"90th Percentile Response Time: {short_job_percentiles[0]} seconds")
    print(f"95th Percentile Response Time: {short_job_percentiles[1]} seconds")
    print(f"99th Percentile Response Time: {short_job_percentiles[2]} seconds")

    print("\nLong Job Metrics:")
    print(f"90th Percentile Response Time: {long_job_percentiles[0]} seconds")
    print(f"95th Percentile Response Time: {long_job_percentiles[1]} seconds")
    print(f"99th Percentile Response Time: {long_job_percentiles[2]} seconds")

    print("\nOverall Job Metrics:")
    print(f"90th Percentile Response Time: {overall_percentiles[0]} seconds")
    print(f"95th Percentile Response Time: {overall_percentiles[1]} seconds")
    print(f"99th Percentile Response Time: {overall_percentiles[2]} seconds")


    import matplotlib.pyplot as plt

    # Separate short and long job response times  (changes made here)
    # short_job_delays = [job.completion_time-job.start_time - job.ideal_completion_time for job in simulator_globals.jobs_completed if job.is_short]
    # long_job_delays = [job.completion_time-job.start_time- job.ideal_completion_time for job in simulator_globals.jobs_completed if not job.is_short]


    # Create subplots
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 6))

# Create box plot for short jobs
    axes[0].boxplot(short_job_delays, labels=['Short Jobs'])
    axes[0].set_title('Response Times for Short Jobs')
    axes[0].set_ylabel('Response Time (seconds)')
    axes[0].grid(True)


    # # Create subplots
    # fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 8))

    # # Create box plot for short jobs
    # axes[0].boxplot(short_job_delays, labels=['Short Jobs'])
    # axes[0].set_title('Delay Times for Short Jobs')
    # # axes[0].set_xlabel('Job Type')
    # axes[0].set_ylabel('Response Time (seconds)')
    # axes[0].set_ylim(0, 0.2)
    # axes[0].grid(which='both', axis='both')

# Create box plot for long jobs
    axes[1].boxplot(long_job_delays, labels=['Long Jobs'])
    axes[1].set_title('Response Times for Long Jobs')
    axes[1].set_ylabel('Response Time (seconds)')
    axes[1].grid(True)

    # axes[1].boxplot(long_job_delays, labels=['long Jobs'])
    # axes[1].set_title('Delay Times for long Jobs')
    # # axes[1].set_xlabel('Job Type')
    # axes[1].set_ylabel('Response Time (seconds)')
    # axes[1].set_ylim(0, 0.2)
    # axes[1].grid(which='both', axis='both')


# Adjust layout
    # plt.tight_layout()

# Show plots
    plt.show()


    # Calculate total execution time of all completed jobs
    total_execution_time = sum(job.end_time - job.start_time for job in simulator_globals.jobs_completed)

# Calculate total simulation time
    total_simulation_time = time_elapsed

# Calculate CPU utilization
    cpu_utilization = (total_execution_time / total_simulation_time) * 100
    cpu_utilization = min(cpu_utilization, 100)
# Print or log the calculated CPU utilization
    #print(f"CPU Utilization: {cpu_utilization:.2f}%")

    # Plotting the baseline metrics as a bar graph
    plt.figure(figsize=(12, 6))

    # Extracting metric names and values
    metrics = list(baseline_metrics.keys())
    values = list(baseline_metrics.values())

    # Creating the bar graph with custom colors
    colors = ['lightblue', 'lightgreen', 'lightcoral', 'lightsalmon']
    plt.bar(metrics, values, color=colors)

    # Adding labels and title
    plt.xlabel('Metrics')
    plt.ylabel('Values')
    plt.title('Baseline Metrics')

    # Rotating x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Displaying the plot
    plt.tight_layout()
    plt.show()

    # Plotting the pie chart for percentage of long and short jobs
    plt.figure(figsize=(8, 8))

    # Labels for the pie chart
    labels = ['Long Jobs', 'Short Jobs']

    # Values for the pie chart
    sizes = [long_job_percentage, short_job_percentage]

    # Colors for the pie chart
    colors = ['lightblue', 'lightgreen']

    # Explode the first slice (Long Jobs)
    explode = (0.1, 0)

    # Plotting the pie chart
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140)

    # Equal aspect ratio ensures that pie is drawn as a circle
    plt.axis('equal')

    # Adding title
    plt.title('Percentage of Long and Short Jobs')

    # Displaying the plot
    plt.show()

    labels = ['90th Percentile', '95th Percentile', '99th Percentile']
    short_job_values = [short_job_percentiles[0], short_job_percentiles[1], short_job_percentiles[2]]
    long_job_values = [long_job_percentiles[0], long_job_percentiles[1], long_job_percentiles[2]]
    overall_values = [overall_percentiles[0], overall_percentiles[1], overall_percentiles[2]]

    x = np.arange(len(labels))  # the label locations
    width = 0.3  # the width of the bars

    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.bar(x - width, short_job_values, width, label='Short Jobs')
    rects2 = ax.bar(x, long_job_values, width, label='Long Jobs')
    rects3 = ax.bar(x + width, overall_values, width, label='Overall')

# Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Response Time (seconds)')
    ax.set_title('Percentile Response Time by Job Type')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

# Attach a text label above each bar in rects, displaying its height
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)

    fig.tight_layout()

    plt.show()

# Calculate delay for each job
    delay_times = [max(0, job.end_time - job.start_time) for job in simulator_globals.jobs_completed]

# Calculate delay metrics
    average_delay = np.mean(delay_times)
    max_delay = np.max(delay_times)
    min_delay = np.min(delay_times)
    percentile_90_delay = np.percentile(delay_times, 90)
    percentile_95_delay = np.percentile(delay_times, 95)
    percentile_99_delay = np.percentile(delay_times, 99)

# Plot the delay metrics
    plt.figure(figsize=(10, 6))

# Histogram of delay times
    plt.hist(delay_times, bins=20, color='skyblue', edgecolor='black', alpha=0.7)
    plt.title('Histogram of Delay in Job Response Time')
    plt.xlabel('Delay Time (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()

# Plot delay metrics
    plt.figure(figsize=(10, 6))

# Bar plot for average, maximum, and minimum delay
    plt.bar(['Average', 'Maximum', 'Minimum'], [average_delay, max_delay, min_delay], color=['skyblue', 'lightgreen', 'lightcoral'])
    plt.ylabel('Delay Time (seconds)')
    plt.grid(axis='y')

# Line plot for percentiles
    percentiles = ['90th', '95th', '99th']
    percentile_values = [percentile_90_delay, percentile_95_delay, percentile_99_delay]
    plt.plot(percentiles, percentile_values, marker='o', linestyle='-')
    plt.legend(['Percentile Delay'])

    plt.title('Delay Metrics')
    plt.show()




    print("control ",simulator_utils.globals.control)
    print("preempts ",simulator_utils.globals.num_preempts)
    print("wait time ",simulator_utils.globals.waittime)
    print("max wait time ",simulator_utils.globals.mwaittime)
    print("wait task ",simulator_utils.globals.waittask)
    print("max wait task ",simulator_utils.globals.maxwaittask)
    # print("Simulator Info , Simulation ended")

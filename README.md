🧠 Efficient Priority-Aware Scheduling for Federated Datacenter Frameworks
This repository accompanies our research paper published in the 2025 10th International Conference on Cloud Computing and Big Data Analytics (ICCCBDA), titled:

"An Efficient Priority-Aware Scheduling Approach for Federated Datacenter Scheduling Frameworks"

📄 IEEE Xplore: https://ieeexplore.ieee.org/abstract/document/11030531

📌 Abstract
Modern cloud systems rely heavily on scheduling frameworks to efficiently allocate resources. This project enhances the Megha federated scheduler by introducing priority-aware scheduling that significantly reduces latency for short-term, user-facing jobs, while ensuring long-term job throughput remains acceptable.

We propose four novel scheduling algorithms aimed at optimizing job execution in federated architectures:

1) Static GM Allocation (3:2 short-to-long job ratio)
2) Repartitioning-based Adaptive Allocation
3) Dynamic Resource Allocation
4) Preemptive Scheduling (Priority-based interruption)
   
🚀 Key Features
✅ Priority-aware job classification
✅ Support for heterogeneous workloads
✅ Scalable across 100–1000 node clusters
✅ Benchmarked with Yahoo Cluster Trace
✅ Outperforms Megha, YARN, and Sparrow in short job latency
✅ Up to 99.9% reduction in short job response time
✅ Simulation-driven results with 99th percentile metrics

📊 Results Snapshot

<img width="431" height="312" alt="image" src="https://github.com/user-attachments/assets/d19e8a47-3879-482a-ab04-5fb0cee6c5de" />

<img width="908" height="312" alt="image" src="https://github.com/user-attachments/assets/602ea7fd-49e1-44e5-b88d-d71f8dfeb378" />



📉 Preemptive approach consistently yields best balance between short and long job scheduling.

🏗️ System Architecture

<img width="462" height="442" alt="image" src="https://github.com/user-attachments/assets/36ca3e14-7b53-4b93-85fe-bc337fa0aaa0" />

Federated Scheduler: GM (Global Masters) & LM (Local Masters)

Job Classification: Short vs Long (Threshold: 90.58s)

Dynamic Resource Pools: No static binding of resources

Preemption: Time-sensitive short jobs interrupt long jobs past wait threshold


▶️ Running the Simulation

git clone https://github.com/shreyasbandi/priority-aware-federated-scheduler.git

SET PYTHONPATH=priority-aware-federated-scheduler

cd priority-aware-federated-scheduler

python src/runner.py traces/input/own.tr simulator_config/config_G5L5s1000.json 0


🔬 Dataset Used
Yahoo Cluster Trace

24,262 real-world jobs

Job duration threshold: 90.5811 seconds

~90.6% short jobs, ~9.4% long jobs

📚 Citation
If you use this project or the algorithms in your work, please cite

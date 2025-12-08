# Workload Generator for AleaNG 

## Summary
The generator creates an extended, **SWF-like workload format** and additional files needed by the **AleaNG simulator** to perform detailed job scheduling simulations. 
Notably, the generator creates: 
1) *job file* (in extended SWF format, see SWF description: https://www.cs.huji.ac.il/labs/parallel/workload/swf.html) 
2) *machine file* (describing clusters)
3) *queue file* (describing queues and their limits and priorities)
4) *group file* (describing user-groups and their limits)
5) *user file* (describing users, their groups, their relative shares and limits)

### Detailed features
In the generator configuration various parameters allows you to:
- Specify the number of workload instances to be created with a given setup (using a different random seed)
- Specify the duration of the workload (how many days of user activity shall be created)
- Specify the target load of the system
  - e.g., 0.8 means that the generator will create a workload that occupies ~80% of all CPU resources (ideally packed). 
  - It is useful to influence the "backlog" of waiting jobs (1.5 -> huge, 0.99 -> large, 0.6 -> small)
- Specify the percentage of urgent users’ load. 
  - E.g., 30% means that all urgent users will be responsible for ~30% of all utilized CPU hours (i.e., 70% of job time will be assigned to jobs of normal users).
- Specify both urgent and normal users, their shares & CPU quotas, and parent groups
  - Specify the type of user workload
  - Three types are supported: tiny, small, large
- Specify groups and their priorities and CPU quota limits
- Specify queues, their priorities, and CPU quota limits
- Specify clusters, their names, nodes, GPUs, and properties 
  - (a property is used to steer a job toward an eligible cluster) 
- Select whether jobs occupy whole nodes or can run side-by-side with other jobs (space-sharing)
  - Specify the maximum number of nodes that can be allocated 
- Specify job batch sizes (number of jobs submitted at once), 
  - 3 batch sizes are supported (for large, small, and tiny jobs). 
  - Specify sizes (intervals) of tiny, small, and large jobs
- Specify typical job runtime limits (aka walltimes)
  - Actual runtimes are randomly generated based on the selected walltime limit
- Specify typical inter-batch idle periods, used to model the “think time” of a user
  - Different inter-batch intervals are available for urgent and normal users


### Principle of operation
The generator uses `configuration.properties` file as an input to generate all necessary workload files, especially the SWF-like job workload trace with correct user, 
queue, group and cluster mappings. 

In an iterative process, the generator first generates job arrival patterns (using batch sizes and inter-batch periods), then it creates jobs 
(all randomly based on the input parameters) and checks that each user does not exceed its utilization target (based on the number of users, target system load, 
and percentage of urgent load). 

This is repeated for every simulation day (based on the specified duration of the workload), and all these sub-traces are then merged and properly ordered into a single job trace. 


### Configuration

The generator uses `configuration.properties` file where all options are explained, as shown below in an example.
```ini
# DATA_SET name (output workload name)
workload_filename=Example1.swf

# DATA_SET directory (output directory name)
workload_dir=generated_data-set

# NUMBER of ACTIVE DAYS: how many days the users submit their jobs
days=5

# SYSTEM LOAD: target system load, e.g., 0.8 means that the generator will create workload that occupies ~80% of all CPU resources (ideally packed)
# useful to influence the "backlog" of waiting jobs (1.5 -> huge, 0.99 -> large, 0.6 -> small)
system_load=0.8

# Number of random unique workloads that will be created using this setup
workload_instances=1

# URGENT JOBS: how many jobs (in %) will belong to urgent QoS (urgent QoS jobs have higher priority). 
load_percentage_urgent=10


# USERS (comma separated list): two classes are supported (urgent and normal)
# format: user_name <TAB> shares(int) <TAB> CPU_limit_quota(int)
# keywords for job size: [tiny, large, small] - used by generator to identify job size   
# keywords for user priority (QoS) group: [urgent, normal] - used by generator to identify job class 
user_names_urgent=urgent_large_user \t 2 \t 96,urgent_tiny_user \t 2 \t 96
user_names_normal=normal_large_user \t 1 \t 64,normal_small_user \t 1 \t 64

...
```

### Extended SWF format
The generator adds 3 new fields to the standard SWF: `used_GPUs`, `soft_walltime`, and `required_properties`. The format is explained bellow:
```ini
; --------------------------------------------------------------------------- 
; JOB FIELDS MEANING: 
; --------------------------------------------------------------------------- 
;  0: job_id              (unique ID)
;  1: submit_time         (in seconds from 0)
;  2: wait_time           (in seconds)
;  3: runtime             (in seconds)
;  4: allocated_nodes     (number of all allocated nodes/or number of all allocated CPU cores)
;  5: CPU_time_used       (not used, same as runtime)
;  6: RAM                 (used, in kB)
;  7: required_nodes      (same as allocated nodes/cores)
;  8: walltime_limit      (user-provided upper bound runtime limit in seconds)
;  9: RAM                 (requested, in kB)
; 10: job_status          (always 1, not used) 
; 11: user_id             (must match with user IDs specified in user desc. file)
; 12: group_id            (must match with group IDs specified in group desc. file)
; 13: executable_number   (-1, not used) 
; 14: queue_id            (must match with queue IDs specified in queue desc. file)
; 15: partition           (-1, not used) 
; 16: preceding_job       (-1, not used) 
; 17: think_time          (-1, not used) 
; 18: used_GPUs           (sum over all used nodes, must be divisible by the number of used nodes) 
; 19: soft_walltime       ("expected" runtime in seconds - smaller than walltime - that can be exceeded without killing the job) 
; 20: required_properties (separated by ":", example: "all:excl:2x4" The first property "all" is used to match the String property of a cluster. If the property is "all", then this job can execute on any cluster. Otherwise, only a cluster that matches this String is acceptable. "excl" means that this job requires whole node(s) exclusively (no space-sharing with other jobs). "2x4" means that this job requires two nodes, each having at least 4 CPU cores.)
; --------------------------------------------------------------------------- 
``` 

### Running the generator
This generator is written in Java and is distributed as maven project. To start the generator, run:
```
java -cp target/SWF_Workload_Generator-1.0-SNAPSHOT.jar com.mycompany.swf_workload_generator.SWF_Workload_Generator
```
Similarly, to compile it, run:
```
 mvn compile
```
**IMPORTANT NOTE:** If you run into java version issues, use `java -version` to verify your java version and adjust `pom.xml` accordingly (set `<maven.compiler.source>` and `<maven.compiler.target>` tags to your java version, e.g., `21` or `17`, etc.).

Next, clean and build the whole generator:
```
mvn clean compile    (Deletes old compiled classes first → forces recompile with correct version)
mvn clean package    (Same as above + builds the final .jar)
```

##### Software licence:
This software is provided as is, free of charge under the terms of the LGPL licence. 

## Important
**When using AleaNG or this generator in your paper or presentation**, please use the following citations as an acknowledgement. Thank you!
* Dalibor Klusáček and Václav Chlumský. "*Fair-sharing simulator: Toward fair scheduling in batch computing systems*". In The International Journal of High Performance Computing Applications, Sage, 2025. https://doi.org/10.1177/10943420251385673
* Dalibor Klusáček. "*Fair-Sharing Simulator for Batch Computing Systems*". In proceedings of the 15th International Conference on Parallel Processing & Applied Mathematics (PPAM 2024), Springer, 2024.

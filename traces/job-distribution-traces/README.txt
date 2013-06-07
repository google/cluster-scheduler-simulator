== Schema of trace files of job interarrival time, num_tasks, job_duration ==

These trace files should contain distributions of job interarrival time,
num_tasks, job_duration from your cluster. The simulator will build
empirical distributions based on these files.

=== Columns ===
Column 0: cluster_name
Column 1: assignment policy ("cmb-new" = "CMB_PBB")
Column 2: scheduler id, values can be 0 or 1. 0 = batch, service = 1
Column 3: depending on which trace file:
    interarrival time (seconds since last job arrival)
    OR tasks in job
    OR job runtime (seconds)

Traces may mix batch and service jobs, although the provided examples segregate them.

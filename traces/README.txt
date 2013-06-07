== Schema of cell input file ==
Fields are space delimited. Each row represents a job scheduling event.

Each row in our traces belongs to one of two schemas. One schema has six columns, and the other has 8 columns. We describe both schema's below. The first five columns are the same in both schemas. 

=== Common Columns ===
Column 0: possible values are 11 or 12
  "11" - something that was there at the beginning of timewindow
  "12" - something that was there at beginning of timewindow and ended at [timestamp] (see Column 1)
Column 1: timestamp
Column 2: unique job ID
Column 3: 0 or 1 - prod_job - boolean flag indicating if this job is "production" priority as described in [1]
Column 4: 0, 1, 2, or 3 - sched_class - see description of "Scheduling Class" in [1]

=== 6 column format ===
Column 5: UNSPECIFIED/UNUSED

=== 8 column format ===
Column 5: number of tasks
Column 6: aggregate CPU usage of job (in num cores)
Column 7: aggregate Ram usage of job (in bytes)

== CMB_PBB split logic ==
For our primary research evaluation in [2] we used a job -> scheduler assignment policy as follows, which we call CMB_PBB.

The following Python snippet captures the definition of CMB_PBB. It decides a job's scheduler assignment based on its values of the prod_job and sched_class fields (see columns 3 and 4 above).

elif apol == 'cmb_pbb':
   if prod_job and sched_class != 0 and sched_class != 1:
     return 1
   else:
     return 0

== Example Lines ==
11 0.000000  623486366592 0 2 1 1 1074000000
12 12.602755 623486366592 0 2 82587
11 0.000000  158249529602 1 1 1 1 7286400

== Bibliography ==
[1] https://docs.google.com/file/d/0B5g07T_gRDg9NjZnSjZTZzRfbmM/edit
[2] http://eurosys2013.tudos.org/wp-content/uploads/2013/paper/Schwarzkopf.pdf

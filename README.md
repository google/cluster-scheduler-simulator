# Cluster scheduler simulator overview

This simulator can be used to prototype and compare different cluster scheduling strategies and policies. It generates synthetic cluster workloads from empirical parameter distributions (thus generating unique workloads even from a small amount of input data), simulates their scheduling and execution using a discrete event simulator, and finally permits analysis of scheduling performance metrics.

The simulator was originally written as part of research on the "Omega" shared-state cluster scheduling architecture at Google. A paper on Omega, published at EuroSys 2013, uses of this simulator for the comparative evaluation of Omega and other alternative architectures (referred to as a "lightweight" simulator there) [1]. As such, the simulators design is somewhat geared towards the comparative evaluation needs of this paper, but it does also permit more general experimentation with:

 * scheduling policies and logics (i.e. "what machine should a task be bound to?"),
 * resource models (i.e. "how are machines represented for scheduling, and how are they shared between tasks?"),
 * shared-cluster scheduling architectures (i.e. "how can multiple independent schedulers be supported for a large shared, multi-framework cluster?").

While the simulator will simulate job arrival, scheduler decision making and task placement, it does **not** simulate the actual execution of the tasks or variation in their runtime due to shared resources.

## Downloading, building, and running

The source code for the simulator is available in a Git repository hosted on Google Code. Instructions for downloading  can be found at at https://code.google.com/p/cluster-scheduler-simulator/source/checkout.

The simulator is written in Scala, and requires the Simple Build Tool (`sbt`) to run. A copy of `sbt` is package with the source code, but you will need the following prerequisites in order to run the simulator:

 * a working JVM (`openjdk-6-jre` and `openjdk-6-jdk` packages in mid-2013 Ubuntu packages),
 * a working installation of Scala (`scala` Ubuntu package),
 * Python 2.6 or above and matplotlib 1.0 or above for generation of graphs (`python-2.7` and `python-matplotlib` Ubuntu packages).

Once you have ensured that all of these exist, simply type `bin/sbt run` from the project home directory in order to run the simulator:

    $ bin/sbt run
    [...]
    [info] Compiling 9 Scala sources and 1 Java source to ${WORKING_DIR}/target/scala-2.9.1/classes...
    [...]
    [info] Running Simulation 
    
    RUNNING CLUSTER SIMULATOR EXPERIMENTS
    ------------------------
    [...]

### Using command line flags

The simulator can be passed some command-line arugments via configuration flags, such as `--thread-pool-size NUM_THREADS_INT` and `--random-seed SEED_VAL_INT`. To view all options run:

    $ bin/sbt "run --help"

Note that when passing command line options to the `sbt run` command you need to include the word `run` and all of the options that follow it within a single set of quotes. `sbt` can also be used via the `sbt` console by simply running `bin/sbt` which will drop you at a prompt. If you are using this `sbt` console option, you do not need to put quotes around the run command and any flags you pass.

### Configuration file
If a file `conf/cluster-sim-env.sh` exists, it will be sourced in the shell before the simulator is run. This was added as a way of setting up the JVM (e.g. heap size) for simulator runs. Check out `conf/cluster-sim-env.sh.template` as a starting point; you will need to uncomment and possibly modify the example configuration value set in that template file (and, of course, you will need to create a copy of the file removing the ".template" suffix).


## Configuring experiments

The simulation is controlled by the experiments configured in the `src/main/scala/Simulation.scala` setup file. Comments in the file explain how to set up different workloads, workload-to-scheduler mappings and simulated cluster and machine sizes.

Most of the workload setup happens in `src/main/scala/Workloads.scala`, so read through that file and make modifications there to have the simulator read from a trace file of your own (see more below about the type of trace files the simulator uses, and the example files included).

Workloads in the simulator are generated from *empirical parameter distributions*. These are typically based on cluster *snapshots* (at a point in time) or *traces* (sequences of events over time). We unfortunately cannot provide the full input data used for our experiments with the simulator, but we do provide example input files in the `traces` subdirectory, illustrating the expected data format (further explained in the local README file in `traces`). The following inputs are required:

 * **initial cluster state**: when the simulation starts, the simulated cluster obviously cannot start off empty. Instead, we pre-load it with a set of running jobs (and tasks) at this point in time. These jobs start before the beginning of simulation, and may end during the simulation or after. The example file `traces/example-init-cluster-state.log` shows the input format for the jobs in the initial cluster state, as well as the departure events of those of them which end during the simulation. The resource footprints of tasks generated at simulation runtime will also be sampled from the distribution of resource footprints of tasks in the initial cluster state.
 * **job parameters**: the simulator samples three key parameters for each job from empirical distributions (i.e. randomly picks values from a large set):
    1. Job sizes (`traces/job-distribution-traces/example_csizes_cmb.log`): the number of tasks in the generated job. We assume for simplicity that all tasks in a job have the same resource footprint.
    2. Job inter-arrival times (`traces/job-distribution-traces/example_interarrival_cmb.log`): the time in between job arrivals for each workload (in seconds). The value drawn from this distribution indicates how many seconds elapse until another job arrives, i.e. the "gaps" in between jobs.
    3. Job runtimes (`traces/job-distribution-traces/example_runtimes_cmb.log`): total job runtime. For simplicity, we assume that all tasks in a job run for exactly this long (although if a task gets scheduled later, it will also finish later).

For further details, see `traces/README.txt` and `traces/job-distribution-traces/README.txt`.

**Please note that the resource amounts specified in the example data files, and the example cluster machines configured in `Simulation.scala` do *not* reflect Google configurations. They are made-up numbers, so please do not quote them or try to interpret them!**

A possible starting point for generating realistic input data is the public Google cluster trace [2, 3]. It should be straightforward to write scripts that extract the relevant data from the public trace's event logs. Although we do not provide such scripts, it is worth noting that the "cluster C" workload in the EuroSys paper [1] represents the same workload as the public trace. (If you do write scripts for converting the public trace into simulator format, please let us know, and we will happily include them in the simulator code release!)

## Experimental results: post-processing

Experimental results are stored in serialized Protocol Buffers in the `experiment_results` directory at the root of the source tree by default: one subdirectory for each experiment, and with a unique name identifying the experimental setup as well as the start time. The schemas for the `.protobuf` files are stored in `src/main/protocolbuffers`.

A script for post-processing and graphing experimental results is located in `src/main/python/graphing-scripts`, and `src/main/python` also contains scripts for converting the protobuf-encoded results into ASCII CSV files. See the README file in the `graphing-scripts` directory for detailed explanation.

## NOTES

### Changing and compiling the protocol buffers

If you make changes to the protocol buffer file (in `src/main/protocolbuffers`), you will need to recompile them, which will generate updated Java files in `src/main/java`. To do so, you must install the protcol buffer compiler and run `src/main/protocolbuffers/compile_protobufs.sh`, which itself calls `protoc` (which it assumes is on your `PATH`).

### Known issues

- The `schedulePartialJobs` option is used in the current implementation of the `MesosScheduler` class. Partial jobs are always scheduled (even if this flag is set to false). Hence the `mesosSimulatorSingleSchedulerZeroResourceJobsTest` currently fails to pass.

## Contributing, Development Status, and Contact Info

Please use  the Google Code [project issue tracker](https://code.google.com/p/cluster-scheduler-simulator/issues/list) for all bug reports, pull requests and patches, although we are unlikely to be able to respond to feature requests. You can also send any kind of feedback to the developers, [Andy Konwinski](http://andykonwinski.com/) and [Malte Schwarzkopf](http://www.cl.cam.ac.uk/~ms705/).

## References

[1] Malte Schwarzkopf, Andy Konwinski, Michael Abd-El-Malek and John Wilkes. **[Omega: flexible, scalable schedulers for large compute clusters](http://eurosys2013.tudos.org/wp-content/uploads/2013/paper/Schwarzkopf.pdf)**. In *Proceedings of the 8th European Conference on Computer Systems (EuroSys 2013)*.

[2] Charles Reiss, Alexey Tumanov, Gregory Ganger, Randy Katz and Michael Kotzuch. **[Heterogeneity and Dynamicity of Clouds at Scale: Google Trace Analysis](http://www.pdl.cmu.edu/PDL-FTP/CloudComputing/googletrace-socc2012.pdf)**. In *Proceedings of the 3rd ACM Symposium on Cloud Computing (SoCC 2012)*.

[3] Google public cluster workload traces. [https://code.google.com/p/googleclusterdata/](https://code.google.com/p/googleclusterdata/).

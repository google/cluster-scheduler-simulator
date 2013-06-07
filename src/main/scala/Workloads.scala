package ClusterSchedulingSimulation

import java.io.File

/**
 * Set up workloads based on measurements from a real cluster.
 * In the Eurosys paper, we used measurements from Google clusters here.
 */
object Workloads {
  /**
   * Set up CellStateDescs that will go into WorkloadDescs. Fabricated
   * numbers are provided as an example. Enter numbers based on your
   * own clusters instead.
   */
  val exampleCellStateDesc = new CellStateDesc(numMachines = 10000,
                                          cpusPerMachine = 4,
                                          memPerMachine = 16)


  /**
   * Set up WorkloadDescs, containing generators of workloads and
   * pre-fill workloads based on measurements of cells/workloads.
   */
  val exampleWorkloadGeneratorBatch =
      new ExpExpExpWorkloadGenerator(workloadName = "Batch".intern(),
                                     initAvgJobInterarrivalTime = 10.0,
                                     avgTasksPerJob = 100.0,
                                     avgJobDuration = (100.0),
                                     avgCpusPerTask = 1.0,
                                     avgMemPerTask = 2.0)
  val exampleWorkloadGeneratorService =
      new ExpExpExpWorkloadGenerator(workloadName = "Service".intern(),
                                     initAvgJobInterarrivalTime = 20.0,
                                     avgTasksPerJob = 10.0,
                                     avgJobDuration = (500.0),
                                     avgCpusPerTask = 1.0,
                                     avgMemPerTask = 2.0)
  val exampleWorkloadDesc = WorkloadDesc(cell = "example",
                                      assignmentPolicy = "CMB_PBB",
                                      workloadGenerators =
                                          exampleWorkloadGeneratorBatch ::
                                          exampleWorkloadGeneratorService :: Nil,
                                      cellStateDesc = exampleCellStateDesc)


  // example pre-fill workload generators.
  val examplePrefillTraceFileName = "traces/example-init-cluster-state.log"
  assert((new File(examplePrefillTraceFileName)).exists())
  val exampleBatchPrefillTraceWLGenerator =
      new PrefillPbbTraceWorkloadGenerator("PrefillBatch",
                                           examplePrefillTraceFileName)
  val exampleServicePrefillTraceWLGenerator =
      new PrefillPbbTraceWorkloadGenerator("PrefillService",
                                          examplePrefillTraceFileName)
  val exampleBatchServicePrefillTraceWLGenerator =
      new PrefillPbbTraceWorkloadGenerator("PrefillBatchService",
                                          examplePrefillTraceFileName)

  val exampleWorkloadPrefillDesc =
      WorkloadDesc(cell = "example",
                   assignmentPolicy = "CMB_PBB",
                   workloadGenerators =
                       exampleWorkloadGeneratorBatch ::
                       exampleWorkloadGeneratorService ::
                       Nil,
                   cellStateDesc = exampleCellStateDesc,
                   prefillWorkloadGenerators =
                       List(exampleBatchServicePrefillTraceWLGenerator))


  // Set up example workload with jobs that have interarrival times
  // from trace-based interarrival times.
  val exampleInterarrivalTraceFileName = "traces/job-distribution-traces/" +
      "example_interarrival_cmb.log"
  val exampleNumTasksTraceFileName = "traces/job-distribution-traces/" +
      "example_csizes_cmb.log"
  val exampleJobDurationTraceFileName = "traces/job-distribution-traces/" +
      "example_runtimes_cmb.log"
  assert((new File(exampleInterarrivalTraceFileName)).exists())
  assert((new File(exampleNumTasksTraceFileName)).exists())
  assert((new File(exampleJobDurationTraceFileName)).exists())

  // A workload based on traces of interarrival times, tasks-per-job,
  // and job duration. Task shapes now based on pre-fill traces.
  val exampleWorkloadGeneratorTraceAllBatch =
      new TraceAllWLGenerator(
          "Batch".intern(),
          exampleInterarrivalTraceFileName,
          exampleNumTasksTraceFileName,
          exampleJobDurationTraceFileName,
          examplePrefillTraceFileName,
          maxCpusPerTask = 3.9, // Machines in example cluster have 4 CPUs.
          maxMemPerTask = 15.9) // Machines in example cluster have 16GB mem.

  val exampleWorkloadGeneratorTraceAllService =
      new TraceAllWLGenerator(
          "Service".intern(),
          exampleInterarrivalTraceFileName,
          exampleNumTasksTraceFileName,
          exampleJobDurationTraceFileName,
          examplePrefillTraceFileName,
          maxCpusPerTask = 3.9,
          maxMemPerTask = 15.9)

  val exampleTraceAllWorkloadPrefillDesc =
      WorkloadDesc(cell = "example",
                   assignmentPolicy = "CMB_PBB",
                   workloadGenerators =
                       exampleWorkloadGeneratorTraceAllBatch ::
                       exampleWorkloadGeneratorTraceAllService ::
                       Nil,
                   cellStateDesc = exampleCellStateDesc,
                   prefillWorkloadGenerators =
                      List(exampleBatchServicePrefillTraceWLGenerator))
}

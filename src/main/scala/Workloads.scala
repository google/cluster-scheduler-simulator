/**
 * Copyright (c) 2013, Regents of the University of California
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.  Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with
 * the distribution.  Neither the name of the University of California, Berkeley
 * nor the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.  THIS
 * SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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

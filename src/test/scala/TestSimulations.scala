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

import org.scalatest.FunSuite

import ClusterSchedulingSimulation.Workload
import ClusterSchedulingSimulation.WorkloadDesc
import ClusterSchedulingSimulation.Job
import ClusterSchedulingSimulation.UniformWorkloadGenerator
import ClusterSchedulingSimulation.CellState

import ClusterSchedulingSimulation.ClusterSimulator
import ClusterSchedulingSimulation.MonolithicScheduler

import ClusterSchedulingSimulation.MesosSimulator
import ClusterSchedulingSimulation.MesosScheduler
import ClusterSchedulingSimulation.MesosAllocator

import ClusterSchedulingSimulation.ClaimDelta
import ClusterSchedulingSimulation.OmegaSimulator
import ClusterSchedulingSimulation.OmegaScheduler

import ClusterSchedulingSimulation.PrefillPbbTraceWorkloadGenerator
import ClusterSchedulingSimulation.InterarrivalTimeTraceExpExpWLGenerator

import collection.mutable.HashMap
import collection.mutable.ListBuffer
import sys.process._

class SimulatorsTestSuite extends FunSuite {
  /**
   * Monolithic simulator tests.
   */
  test("MonolithicSimulatorTest") {
    println("\n\n\n=====================")
    println("Testing Monolithic simulator functionality.")
    println("=====================\n\n")
    // Build a workload manually.
    var workload = new Workload("unif")
    val numJobs = 4 // Don't change this unless you update the
                    // hand calculations used in assert()-s below.

    (1 to numJobs).foreach(i => {
      workload.addJob(new Job(id = i,
                                   submitted = 0,
                                   numTasks = i,
                                   taskDuration = i,
                                   workloadName = workload.name,
                                   cpusPerTask = 1.0,
                                   memPerTask = 1.0))
    })
    assert(workload.numJobs == numJobs)
    assert(workload.getJobs.last.id == numJobs)

    // Create a simple scheduler.
    val scheduler = new MonolithicScheduler("simple_sched",
                                            Map("unif" -> 1),
                                            Map("unif" -> 1))

    // Set up a CellState, conflictMode and transactionMode shouldn't 
    // matter for a Monolithic Simulator.
    val monolithicCellState = new CellState(1, 10.0, 20.0,
                                            conflictMode = "sequence-numbers",
                                            transactionMode = "all-or-nothing")

    val monolithicSimulator = new ClusterSimulator(
        monolithicCellState,  
        Map(scheduler.name -> scheduler),
        Map("unif" -> Seq("simple_sched")),
        List(workload),
        List(),
        logging = true,
        monitorUtilization = false)

    assert(monolithicSimulator.schedulers.size == 1)
    assert(monolithicSimulator.workloadToSchedulerMap.size == 1)
    assert(monolithicSimulator.agendaSize == numJobs)

    // Test that run() empties the Simulator's priority queue of WorkItem-s
    monolithicSimulator.run()
    println(monolithicSimulator.currentTime)
    println(numJobs + (1 to numJobs).sum)
    assert(monolithicSimulator.agendaSize == 0)
    // Last task finishes scheduling at (numJobs + (1 to numJobs).sum),
    // and runs for 3 seconds (since jobs are numbered 0 to 3 and each job has
    // task duration set to its id number).
    assert(monolithicSimulator.currentTime ==
           numJobs + (1 to numJobs).sum + numJobs - 1)
  }

  test("testStats") {
    var workload = new Workload("unif")
    val numJobs = 4 // Don't change this unless you update the
                    // hand calculations used in assert()-s below.

    workload = new Workload("unif")
    (1 to numJobs).foreach(i => {
      workload.addJob(Job(id = i,
                               submitted = i,
                               numTasks = i,
                               taskDuration = i,
                               workloadName = workload.name,
                               cpusPerTask = 1.0,
                               memPerTask = 1.0))
    })

    // Create a simple scheduler.
    val scheduler = new MonolithicScheduler("simple_sched",
                                            Map("unif" -> 1),
                                            Map("unif" -> 1))

    // Set up a CellState
    val monolithicCellState = new CellState(1, 10.0, 20.0,
                                            conflictMode = "sequence-numbers",
                                            transactionMode = "all-or-nothing")

    val monolithicSimulator = new ClusterSimulator(
          monolithicCellState,
          Map(scheduler.name -> scheduler),
          Map("unif" -> Seq("simple_sched")),
          List(workload),
          List(),
          logging = true,
          monitorUtilization = false)

    monolithicSimulator.run()

    // Test the workload stats.
    workload.getJobs.foreach(job => {
      println(job.usefulTimeScheduling)
      assert(job.usefulTimeScheduling == 1 + job.id * 1)
    })
    println(workload.jobUsefulThinkTimesPercentile(0.9))
    assert(workload.jobUsefulThinkTimesPercentile(0.9) == ((numJobs + 1) * 0.9).toInt)
    // Job queue times:
    // job 1 arrives 1, thinktime 2, finishes at 3, queued 0
    // job 2 arrives 2, thinktime 3, finishes at 6, queued 1
    // job 3 arrives 3, thinktime 4, finishes at 10, queued 3
    // job 4 arrives 4, thinktime 5, finishes at 15 , queued 6
    assert(workload.avgJobQueueTimeTillFirstScheduled ==
           (0.0 + 1.0 + 3.0 + 6.0)/4.0)
    println(workload.jobQueueTimeTillFirstScheduledPercentile(0.9))
    val array = Array[Double](0.0, 1.0, 3.0, 6.0)
    assert(workload.jobQueueTimeTillFirstScheduledPercentile(0.9) ==
           array((3 *0.9).toInt))
  }

  /**
   * Mesos simulator tests.
   */

  // The following test exercises functionality that is not yet implemented,
  // so we currently expect it to fail.
  test("mesosSimulatorSingleSchedulerZeroResourceJobsTest") {
    println("\n\n\n=====================")
    println("Testing Mesos simulator functionality.")
    println("=====================\n\n")

    var workload = new Workload("unif")
    val numJobs = 40 
    (1 to numJobs).foreach(i => {
      workload.addJob(Job(id = i,
                               submitted = i,
                               numTasks = i,
                               taskDuration = i,
                               workloadName = workload.name,
                               cpusPerTask = 1.0,
                               memPerTask = 1.0))
    })

    // Create a simple scheduler, turn off partial job scheduling
    // So that we can check our think time calculations by just
    // assuming that each job only has to be scheduled once.
    // An alternative to this would be to just increase the size
    // of the test cell (e.g., to 1000cpus and 1000mem),
    // which would allow all of the jobs to fit simultaneously.
    val scheduler = new MesosScheduler(name = "mesos_test_sched",
                                       constantThinkTimes = Map("unif" -> 1),
                                       perTaskThinkTimes = Map("unif" -> 1),
                                       schedulePartialJobs = false)

    // Set up a CellState with plenty of space so that no jobs 
    val mesosCellState = new CellState(1, 100.0, 200.0,
                                       conflictMode = "resource-fit",
                                       transactionMode = "all-or-nothing")

    // Create a round-robin allocator
    val allocatorConstantThinkTime = 1.0
    val mesosDRFAllocator = new MesosAllocator(allocatorConstantThinkTime)

    val mesosSimulator = new MesosSimulator(
          mesosCellState,
          Map(scheduler.name -> scheduler),
          Map("unif" -> Seq("mesos_test_sched")),
          List(workload),
          List(),
          mesosDRFAllocator,
          logging = true,
          monitorUtilization = false)

    mesosSimulator.run()
    assert(mesosSimulator.agendaSize == 0, ("Mesos Agenda should have been " +
                                            "zero when simulator finished " +
                                            "running, but was %d.")
                                           .format(mesosSimulator.agendaSize))
    // Each job has constant think time of 1, and per task think of 1.
    // Thus, job i (which has i tasks) will think for 1 + i seconds.
    // So we have numJobs seconds from the constant term of each job, and
    // and 1 + 2 + ... + i from the i term.
    assert(workload.totalJobUsefulThinkTimes == numJobs + (1 to numJobs).sum,
           ("totalJobThinkTimes should have been %d, but was %f. THIS TEST " +
            "EXERCISES FUNCTIONALITY THAT IS NOT YET IMPLEMENTED, SO WE " +
            "CURRENTLY EXPECT IT TO FAIL").format(
           numJobs + (1 to numJobs).sum, workload.totalJobUsefulThinkTimes))
    // For .75 percentile we should see 1 + i (see comment above)
    // where i = 40 * .75.
    assert(workload.jobUsefulThinkTimesPercentile(.75) == 1 + (40 * .75).toInt,
           ("Expected jobUsefulThinkTimesPercentil(0.75) to be %d, " +
            "but it was %f.")
           .format(1 + (40 * .75).toInt,
                   workload.jobUsefulThinkTimesPercentile(.75)))
  }

  test("MesosAllocatorTest") {
    val testAllocator = new MesosAllocator(12)
    assert(testAllocator.getThinkTime == 12)
  }

  /**
   * Omega simulator tests.
   */
  test("omegaSimulatorCellStateSyncApplyDeltaAndCommitTest") {
    println("\n\n\n=====================")
    println("omegaSimulatorCellStateSyncApplyDeltaAndCommitTest")
    println("=====================\n\n")
    println("\nRunning cellstate functionality test.")
    // Set up a workload with one job with one task.
    var workload = new Workload("unif")
    workload.addJob(Job(id = 1,
                        submitted = 1.0,
                        numTasks = 1,
                        taskDuration = 10.0,
                        workloadName = workload.name,
                        cpusPerTask = 1.0,
                        memPerTask = 1.0))

    // Create an Omega scheduler.
    val scheduler = new OmegaScheduler(name = "omega_test_sched",
                                       constantThinkTimes = Map("unif" -> 1),
                                       perTaskThinkTimes = Map("unif" -> 1))

    // Set up a CellState.
    val commonCellState = new CellState(numMachines = 10,
                                        cpusPerMachine = 1.0,
                                        memPerMachine = 2.0,
                                        conflictMode = "sequence-numbers",
                                        transactionMode = "all-or-nothing")

    // Set up a Simulator.
    val omegaSimulator = new OmegaSimulator(
          commonCellState,
          Map(scheduler.name -> scheduler),
          Map("unif" -> Seq("omega_test_sched")),
          List(workload),
          List(),
          logging = true,
          monitorUtilization = false)

    // Create a private copy of cellstate.
    val privateCellState = commonCellState.copy
    assert(privateCellState.numMachines == commonCellState.numMachines)
    assert(privateCellState.cpusPerMachine == commonCellState.cpusPerMachine)
    assert(privateCellState.memPerMachine == commonCellState.memPerMachine)
    // Test that the per machine state was successfully copied.
    (0 to commonCellState.allocatedCpusPerMachine.length - 1).foreach{ i => {
      assert(privateCellState.allocatedCpusPerMachine(i) ==
             commonCellState.allocatedCpusPerMachine(i))
    }}
    assert(privateCellState.machineSeqNums(0) == 0)
    // Make changes to the private cellstate by creating and applying a delta.
    val claimDelta = new ClaimDelta(scheduler,
                                    machineID = 0,
                                    privateCellState.machineSeqNums(0),
                                    duration = 10,
                                    cpus = 0.25,
                                    mem = 0.75)
    claimDelta.apply(privateCellState, false)
    // Check that machines sequence number was incremented in private cellstate.
    assert(privateCellState.machineSeqNums(0) == 1)
    // Check that changes to private cellstate stuck.
    assert(privateCellState.availableCpusPerMachine(0) == 1.0 - 0.25)
    assert(privateCellState.availableMemPerMachine(0) == 2.0 - 0.75)
    assert(privateCellState.allocatedCpusPerMachine(0) == 0.25)
    assert(privateCellState.allocatedMemPerMachine(0) == 0.75)
    // Check that common cellstate didn't change yet.
    assert(commonCellState.availableCpusPerMachine(0) == 1.0,
           ("commonCellState should have 1.0 cpus available on machine 0, " +
            "but only has %f.").format(commonCellState.availableCpusPerMachine(0)))
    assert(commonCellState.availableMemPerMachine(0) == 2.0)
    assert(commonCellState.allocatedCpusPerMachine(0) == 0.0)
    assert(commonCellState.allocatedMemPerMachine(0) == 0.0)

    // Commit the changes back to common cellstate.
    commonCellState.commit(Seq(claimDelta))
    // Check that changes to common cellstate stuck.
    assert(commonCellState.availableCpusPerMachine(0) == 1.0 - 0.25)
    assert(commonCellState.availableMemPerMachine(0) == 2.0 - 0.75)
    assert(commonCellState.allocatedCpusPerMachine(0) == 0.25)
    assert(commonCellState.allocatedMemPerMachine(0) == 0.75)
    assert(commonCellState.machineSeqNums(0) == 1)

    // Set up two new private cellstates.
    val privateCellState1 = commonCellState.copy
    val privateCellState2 = commonCellState.copy
    assert(privateCellState1.machineSeqNums(0) == 1)
    assert(privateCellState2.machineSeqNums(0) == 1)

    // Make parallel changes in both private cellstates that
    // should cause a conflict in all-or-nothing conflict-mode.
    val claimDelta1 = new ClaimDelta(scheduler,
                                     machineID = 0,
                                     privateCellState1.machineSeqNums(0),
                                     duration = 10,
                                     cpus = 0.25,
                                     mem = 0.75)
    claimDelta1.apply(privateCellState1, false)
    assert(privateCellState1.machineSeqNums(0) == 2)

    // Check that the other private cellstate didn't change yet.
    assert(privateCellState2.availableCpusPerMachine(0) == 1.0 - 0.25)
    assert(privateCellState2.availableMemPerMachine(0) == 2.0 - 0.75)
    assert(privateCellState2.allocatedCpusPerMachine(0) == 0.25)
    assert(privateCellState2.allocatedMemPerMachine(0) == 0.75)
    assert(privateCellState2.machineSeqNums(0) == 1)

    val claimDelta2 = new ClaimDelta(scheduler,
                                     machineID = 0,
                                     privateCellState2.machineSeqNums(0),
                                     duration = 10,
                                     cpus = 0.25,
                                     mem = 0.75)
    claimDelta2.apply(privateCellState2, false)

    // Commit the changes from the first private cellstate to common cellstate.
    assert(commonCellState.commit(Seq(claimDelta1)).conflictedDeltas.length == 0)
    // Commit the changes from the second private cellstate and check
    // that it conflicts and doesn't change common cellstate.
    assert(commonCellState.commit(Seq(claimDelta2)).conflictedDeltas.length > 0)
    assert(commonCellState.availableCpusPerMachine(0) == 1.0 - 2 * 0.25)
    assert(commonCellState.availableMemPerMachine(0) == 2.0 - 2 * 0.75)
    assert(commonCellState.allocatedCpusPerMachine(0) == 2 * 0.25)
    assert(commonCellState.allocatedMemPerMachine(0) == 2 * 0.75)
    assert(commonCellState.machineSeqNums(0) == 2)
  }

  test("omegaSchedulerTest") {
    println("===========\nomegaSchedulerTest\n==========")
    println("\nRunning cellstate flow test.")
    var workload = new Workload("unif")
    workload.addJob(Job(id = 1,
                             submitted = 1.0,
                             numTasks = 1,
                             taskDuration = 10.0,
                             workloadName = "unif",
                             cpusPerTask = 1.0,
                             memPerTask = 1.0))

    val scheduler = new OmegaScheduler(name = "omega_test_sched",
                                       constantThinkTimes = Map("unif" -> 1),
                                       perTaskThinkTimes = Map("unif" -> 1))

    val commonCellState = new CellState(numMachines = 20,
                                        cpusPerMachine = 1.0,
                                        memPerMachine = 1.0,
                                        conflictMode = "sequence-numbers",
                                        transactionMode = "all-or-nothing")

    val omegaSimulator = new OmegaSimulator(commonCellState,
                                            Map(scheduler.name -> scheduler),
                                            Map("unif" -> Seq("omega_test_sched")),
                                            List(workload),
                                            List(),
                                            logging = true,
                                            monitorUtilization = false)

    // The job should be scheduled as soon as it is added to the scheduler.
    println("adding a job to scheduler.")
    scheduler.addJob(workload.getJobs.head)
    assert(scheduler.scheduling)
    assert(scheduler.jobQueueSize == 0)
    println("added job to scheduler.")
  }

  test("omegaSimulatorRunWithSingleSchedulerTest") {
    println("===========\nomegaSimulatorRunWithSingleSchedulerTest\n===========")
    println("\nRunning cellstate run w/ single scheduler test.")
    // Set up a workload with 40 jobs, each with 1 task.
    var workload = new Workload("unif")
    val numJobs = 40 
    (1 to numJobs).foreach(i => {
      workload.addJob(Job(id = i,
                               submitted = i,
                               numTasks = 1,
                               taskDuration = i,
                               workloadName = workload.name,
                               cpusPerTask = 1.0,
                               memPerTask = 1.0))
    })

    // Create an Omega scheduler.
    val scheduler = new OmegaScheduler(name = "omega_test_sched",
                                       constantThinkTimes = Map("unif" -> 1),
                                       perTaskThinkTimes = Map("unif" -> 1))

    // Set up a CellState.
    val commonCellState = new CellState(numMachines = 1000,
                                        cpusPerMachine = 1.0,
                                        memPerMachine = 1.0,
                                        conflictMode = "sequence-numbers",
                                        transactionMode = "all-or-nothing")

    // Set up a Simulator.
    val omegaSimulator = new OmegaSimulator(
          commonCellState,
          Map(scheduler.name -> scheduler),
          Map("unif" -> Seq("omega_test_sched")),
          List(workload),
          List(),
          logging = true,
          monitorUtilization = false)

    omegaSimulator.run()
    // Each job is scheduled two seconds after it arrives since all jobs
    // have one task so think time = C + L * 1 = 1 + 1 = 2. So job 40 
    // should be scheduled at time 40 * 2 + 1, and it should run for 40 seconds.
    // Thus the simulator should finish at time 121, when the final task
    // finishes running.
    assert(omegaSimulator.currentTime == 121,
           "Simulation ran for %f seconds, but should have run for %d"
           .format(omegaSimulator.currentTime, 121))
  }
  
  test("UniformWorkloadGeneratorTest") {
    println("\nRunning Uniform workload generator test.")
    // create a new WorkloadGenerator
    var workloadGen =
        new UniformWorkloadGenerator(workloadName = "test_wl",
                                     initJobInterarrivalTime = 1.0,
                                     tasksPerJob = 2,
                                     jobDuration = 3.0,
                                     cpusPerTask = 4.0,
                                     memPerTask = 5.0)

    // Test newWorkload.
    val workload = workloadGen.newWorkload(100.0)
    assert(workload.numJobs == 100, "numJobs was %d, should have been %d"
                                    .format(workload.numJobs, 100))
    for(j <- workload.getJobs) {
      assert(j.numTasks == 2.0)
      assert(j.taskDuration == 3.0)
      assert(j.cpusPerTask == 4.0)
      assert(j.memPerTask == 5.0)
    }

    // Test newJob.
    val job = workloadGen.newJob(2003.0)
    assert(job.submitted == 2003.0)
    assert(job.numTasks == 2.0)
    assert(job.taskDuration == 3.0)
    assert(job.cpusPerTask == 4.0)
    assert(job.memPerTask == 5.0)
  }

  test("PrefillWorkloadGeneratorTest") {
    println("\nRunning exmaple prefill workload generator test.")
    val filename = "traces/example-init-cluster-state.log"

    // Load Service jobs.
    val servicePrefillTraceWLGenerator = new PrefillPbbTraceWorkloadGenerator(
        "PrefillService", filename)
    val prefillServiceWL = servicePrefillTraceWLGenerator.newWorkload(1000.0)
    // Cross validated by running at command line:
    val numServiceJobsInFile
      = Seq("awk",
            "$1 == 11 && $4 == 1 && $5 != 0 && $5 != 1",
            filename)
           .!!.split("\n").length
    assert(prefillServiceWL.numJobs == numServiceJobsInFile,
           ("Expected to find %d prefill service jobs from tracefile " +
            "%s, but found %d.")
           .format(numServiceJobsInFile, filename, prefillServiceWL.numJobs))
    for (j <- prefillServiceWL.getJobs) {
      assert(j.submitted == 0)
    }

    // Load batch jobs.
    val batchPrefillTraceWLGenerator = new PrefillPbbTraceWorkloadGenerator(
        "PrefillBatch", filename)
    val prefillBatchWL = batchPrefillTraceWLGenerator.newWorkload(1000.0)  
    val numBatchJobsInFile
      = Seq("awk",
            "$1 == 11 && ($4 != 1 || $5 == 0 || $5 == 1)",
            filename)
           .!!.split("\n").length
    assert(prefillBatchWL.numJobs == numBatchJobsInFile,
           ("Expected to find %d prefill batch jobs from tracefile %s, " +
             "but found %d.")
           .format(numBatchJobsInFile, filename, prefillBatchWL.numJobs))
  }
}

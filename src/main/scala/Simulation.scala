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

import ClusterSchedulingSimulation.Seed
import ClusterSchedulingSimulation.ClusterSimulator
import ClusterSchedulingSimulation.ExpExpExpWorkloadGenerator
import ClusterSchedulingSimulation.Experiment
import ClusterSchedulingSimulation.Job
import ClusterSchedulingSimulation.SchedulerDesc
import ClusterSchedulingSimulation.Workload
import ClusterSchedulingSimulation.WorkloadDesc

import ClusterSchedulingSimulation.Workloads._

import ClusterSchedulingSimulation.MonolithicScheduler
import ClusterSchedulingSimulation.MonolithicSimulatorDesc

import ClusterSchedulingSimulation.MesosSchedulerDesc
import ClusterSchedulingSimulation.MesosSimulatorDesc

import ClusterSchedulingSimulation.OmegaSchedulerDesc
import ClusterSchedulingSimulation.OmegaSimulatorDesc

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.channels.FileChannel

import ca.zmatrix.utils._

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.NumericRange

object Simulation {
  def main(args: Array[String]) {
    val helpString = "Usage: bin/sbt run [--thread-pool-size INT_NUM_THREADS] [--random-seed INT_SEED_VALUE]"
    if (args.length > 0) {
      if (args.head.equals("--help") || args.head.equals("-h")) {
        println(helpString)
        System.exit(0)
      }
    }
    val pp = new ParseParms(helpString)
    pp.parm("--thread-pool-size", "4").rex("^\\d*") // optional_arg
    pp.parm("--random-seed").rex("^\\d*") // optional_arg

    var inputArgs = Map[String, String]()
    val result = pp.validate(args.toList)
    if(result._1 == false) {
      println(result._2)
      sys.error("Exiting due to invalid input.")
    } else {
      inputArgs = result._3
    }

    println("\nRUNNING CLUSTER SIMULATOR EXPERIMENTS")
    println("------------------------\n")

    /**
     * Set up SchedulerDesc-s.
     */
     // Monolithic
    var monolithicSchedulerDesc = new SchedulerDesc(
        name = "Monolithic".intern(),
        constantThinkTimes = Map("Batch" -> 0.01, "Service" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005, "Service" -> 0.01))

    // Mesos
    var mesosBatchSchedulerDesc = new MesosSchedulerDesc(
        name = "MesosBatch".intern(),
        constantThinkTimes = Map("Batch" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005),
        schedulePartialJobs = true)

    var mesosServiceSchedulerDesc = new MesosSchedulerDesc(
        name = "MesosService".intern(),
        constantThinkTimes = Map("Service" -> 0.01),
        perTaskThinkTimes = Map("Service" -> 0.01),
        schedulePartialJobs = true)

    val mesosSchedulerDescs = Array(mesosBatchSchedulerDesc,
                                    mesosServiceSchedulerDesc)

    var mesosBatchScheduler2Desc = new MesosSchedulerDesc(
        name = "MesosBatch-2".intern(),
        constantThinkTimes = Map("Batch" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005),
        schedulePartialJobs = true)

    var mesosBatchScheduler3Desc = new MesosSchedulerDesc(
        name = "MesosBatch-3".intern(),
        constantThinkTimes = Map("Batch" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005),
        schedulePartialJobs = true)

    var mesosBatchScheduler4Desc = new MesosSchedulerDesc(
        name = "MesosBatch-4".intern(),
        constantThinkTimes = Map("Batch" -> 0.01),
        perTaskThinkTimes = Map("Batch" -> 0.005),
        schedulePartialJobs = true)

    val mesos4BatchSchedulerDescs = Array(mesosBatchSchedulerDesc,
                                          mesosBatchScheduler2Desc,
                                          mesosBatchScheduler3Desc,
                                          mesosBatchScheduler4Desc,
                                          mesosServiceSchedulerDesc)


    // Omega
    def generateOmegaSchedulerDescs(numServiceScheds: Int,
                                    numBatchScheds: Int)
                                   : Array[OmegaSchedulerDesc] = {
      val schedDescs = ArrayBuffer[OmegaSchedulerDesc]()
      (1 to numBatchScheds).foreach(i => {
        schedDescs +=
            new OmegaSchedulerDesc(name = "OmegaBatch-%d".format(i).intern(),
                                   constantThinkTimes = Map("Batch" -> 0.01),
                                   perTaskThinkTimes = Map("Batch" -> 0.01))
      })
      (1 to numServiceScheds).foreach(i => {
        schedDescs +=
            new OmegaSchedulerDesc(name = "OmegaService-%d".format(i).intern(),
                                   constantThinkTimes = Map("Service" -> 0.01),
                                   perTaskThinkTimes = Map("Service" -> 0.01))
      })
      println("Generated schedulerDescs: " + schedDescs)
      schedDescs.toArray
    }

    /**
     * Set up workload-to-scheduler mappings.
     */
     var monolithicSchedulerWorkloadMap =
         Map[String, Seq[String]]("Batch" -> Seq("Monolithic"),
                             "Service" -> Seq("Monolithic"))

     var mesos1BatchSchedulerWorkloadMap =
         Map[String, Seq[String]]("Batch" -> Seq("MesosBatch"),
                             "Service" -> Seq("MesosService"))

     var mesos4BatchSchedulerWorkloadMap =
         Map[String, Seq[String]]("Batch" -> Seq("MesosBatch",
                                                 "MesosBatch-2",
                                                 "MesosBatch-3",
                                                 "MesosBatch-4"),
                                  "Service" -> Seq("MesosService"))

     /**
      * Returns a Map with mappings from workload to an arbitrary
      * number of schedulers. These mappings are used by the simulator
      * to decide which scheduler to send a job to when it arrives.
      * If more than one scheduler is specified for a single workload
      * name, then the jobs will be scheduled round-robin across all
      * of those schedulers.
      */
     type SchedulerWorkloadMap = Map[String, Seq[String]]
     def generateSchedulerWorkloadMap(schedulerNamePrefix: String,
                                      numServiceScheds: Int,
                                      numBatchScheds: Int)
                                     : SchedulerWorkloadMap = {
       println("Generating workload map with %d serv scheds & %d batch scheds"
               .format(numServiceScheds, numBatchScheds))
       val schedWorkloadMap = collection.mutable.Map[String, Seq[String]]()
       schedWorkloadMap("Service") =
           (1 to numServiceScheds).map(schedulerNamePrefix + "Service-" + _)
       schedWorkloadMap("Batch") =
           (1 to numBatchScheds).map(schedulerNamePrefix + "Batch-" + _)
       println("Generated schedulerWorkloadMap: " + schedWorkloadMap)
       schedWorkloadMap.toMap
     }

    /**
     * Returns a Map whose entries represent which scheduler/workload pairs
     * to apply the L/C parameter sweep to.
     */
    type SchedulerWorkloadsToSweep = Map[String, Seq[String]]
    def generateSchedulerWorkloadsToSweep(schedulerNamePrefix: String,
                                          numServiceScheds: Int,
                                          numBatchScheds: Int)
                                         : SchedulerWorkloadsToSweep = {
      println("Generating workload map with %d serv scheds & %d batch scheds"
              .format(numServiceScheds, numBatchScheds))
      val schedWorkloadsToSweep = collection.mutable.Map[String, Seq[String]]()
      (1 to numServiceScheds).foreach{ i: Int => {
        schedWorkloadsToSweep(schedulerNamePrefix + "Service-" + i) = Seq("Service")
      }}
      (1 to numBatchScheds).foreach{ i: Int => {
        schedWorkloadsToSweep(schedulerNamePrefix + "Batch-" + i) = Seq("Batch")
      }}
      println("Generated schedulerWorkloadsToSweepMap: " + schedWorkloadsToSweep)
      schedWorkloadsToSweep.toMap
    }

    /**
     * Set up a simulatorDesc-s.
     */
    val globalRunTime = 86400.0 // 1 Day
    val monolithicSimulatorDesc =
        new MonolithicSimulatorDesc(Array(monolithicSchedulerDesc),
                                          globalRunTime)

    val mesosSimulator1BatchDesc =
        new MesosSimulatorDesc(mesosSchedulerDescs,
                               runTime = globalRunTime,
                               allocatorConstantThinkTime = 0.001)
    // Mesos simulator with 4 batch schedulers
    val mesosSimulator4BatchDesc =
        new MesosSimulatorDesc(mesos4BatchSchedulerDescs,
                               runTime = globalRunTime,
                               allocatorConstantThinkTime = 0.001)


    /**
     * Synthetic workloads for testing.
     * These can probably be deleted eventually.
     */
    val synthWorkloadGeneratorService = 
        new ExpExpExpWorkloadGenerator(workloadName = "Service".intern(),
                                       initAvgJobInterarrivalTime = 5.0,
                                       avgTasksPerJob = 100,
                                       avgJobDuration = 10.0,
                                       avgCpusPerTask = 1.0,
                                       avgMemPerTask = 1.5)
    val synthWorkloadGeneratorBatch = 
        new ExpExpExpWorkloadGenerator(workloadName = "Batch".intern(),
                                       initAvgJobInterarrivalTime = 3.0,
                                       avgTasksPerJob = 100,
                                       avgJobDuration = 10.0,
                                       avgCpusPerTask = 1.0,
                                       avgMemPerTask = 1.5)
    val synthWorkloadDesc =
        WorkloadDesc(cell = "synth",
                     assignmentPolicy = "none",
                     workloadGenerators =
                       synthWorkloadGeneratorService ::
                       synthWorkloadGeneratorBatch :: Nil,
                     cellStateDesc = exampleCellStateDesc)

    /**
     * Set up parameter sweeps.
     */

    // 91 values.
    val fullConstantRange: List[Double] = (0.001 to 0.01 by 0.0005).toList :::
                                          (0.015 to 0.1 by 0.005).toList :::
                                          (0.15 to 1.0 by 0.05).toList :::
                                          (1.5 to 10.0 by 0.5).toList :::
                                          (15.0 to 100.0 by 5.0).toList // :::
                                          // (150.0 to 1000.0 by 50.0).toList

    // Full PerTaskRange is 55 values.
    val fullPerTaskRange: List[Double] = (0.001 to 0.01 by 0.0005).toList :::
                                         (0.015 to 0.1 by 0.005).toList :::
                                         (0.15 to 1.0 by 0.05).toList // :::
                                         // (1.5 to 10 by 0.5).toList

    // Full lambda is 20 values.
    val fullLambdaRange: List[Double] = (0.01 to 0.11 by 0.01).toList :::
                                        (0.15 to 1.0 by 0.1).toList // :::
                                        // (1.5 to 10.0 by 1.0).toList

    val fullPickinessRange: List[Double] = (0.00 to 0.75 by 0.05).toList
 

    val medConstantRange: List[Double] = 0.01 :: 0.05 :: 0.1 :: 0.5 ::
                                         1.0 :: 5.0 :: 10.0:: 50.0 ::
                                         100.0 :: Nil
    val medPerTaskRange: List[Double] = 0.001 :: 0.005 :: 0.01 :: 0.05 ::
                                        0.1 :: 0.5 :: 1.0 :: Nil

    val medLambdaRange: List[Double] = 0.01 :: 0.05 :: 0.1 :: 0.5 :: Nil

    val smallConstantRange: List[Double] = (0.1 to 100.0 by 99.9).toList
    val smallPerTaskRange: List[Double] = (0.1 to 1.0 by 0.9).toList
    val smallLambdaRange: List[Double] = (0.001 to 10.0 by 9.999).toList

    /**
     * Choose which "experiment environments" (i.e. WorkloadDescs)
     * we want to use.
     */
    var allWorkloadDescs = List[WorkloadDesc]()
    // allWorkloadDescs ::= exampleWorkloadDesc

    // allWorkloadDescs ::= exampleWorkloadPrefillDesc

    // Prefills jobs based on prefill trace, draws job and task stats from
    // exponential distributions.
    // allWorkloadDescs ::= exampleInterarrivalTimeTraceWorkloadPrefillDesc

    // Prefills jobs based on prefill trace. Loads Job stats (interarrival
    // time, num tasks, duration) from traces, and task stats from
    // exponential distributions.
    // allWorkloadDescs ::= exampleTraceWorkloadPrefillDesc

    // Prefills jobs based on prefill trace. Loads Job stats (interarrival
    // time, num tasks, duration) and task stats (cpusPerTask, memPerTask)
    // from traces.
    allWorkloadDescs ::= exampleTraceAllWorkloadPrefillDesc

    /**
     * Set up a run of experiments.
     */
    var allExperiments: List[Experiment] = List()
    val wlDescs = allWorkloadDescs

    // ------------------Omega------------------
    val numOmegaServiceSchedsRange = Seq(1)
    val numOmegaBatchSchedsRange = Seq(1)

    val omegaSimulatorSetups =
    for (numOmegaServiceScheds <- numOmegaServiceSchedsRange;
         numOmegaBatchScheds <- numOmegaBatchSchedsRange) yield {
      // List of the different {{SimulatorDesc}}s to be run with the
      // SchedulerWorkloadMap and SchedulerWorkloadToSweep.
      val omegaSimulatorDescs = for(
          conflictMode <- Seq("sequence-numbers", "resource-fit");
          transactionMode <- Seq("all-or-nothing", "incremental")) yield {
        new OmegaSimulatorDesc(
            generateOmegaSchedulerDescs(numOmegaServiceScheds,
                                        numOmegaBatchScheds),
            runTime = globalRunTime,
            conflictMode,
            transactionMode)
      }

      val omegaSchedulerWorkloadMap =
          generateSchedulerWorkloadMap("Omega",
                                       numOmegaServiceScheds,
                                       numOmegaBatchScheds)

      val omegaSchedulerWorkloadsToSweep =
      generateSchedulerWorkloadsToSweep("Omega",
                                        numServiceScheds = 0,
                                        numOmegaBatchScheds)
      (omegaSimulatorDescs,omegaSchedulerWorkloadMap, omegaSchedulerWorkloadsToSweep)
    }

    // ------------------Mesos------------------
    // val mesosSimulatorDesc = mesosSimulator4BatchDesc
    val mesosSimulatorDesc = mesosSimulator1BatchDesc

    // val mesosSchedulerWorkloadMap = mesos4BatchSchedulerWorkloadMap
    val mesosSchedulerWorkloadMap = mesos1BatchSchedulerWorkloadMap

    // val mesosSchedWorkloadsToSweep = Map("MesosBatch" -> List("Batch"),
    //                                      "MesosBatch-2" -> List("Batch"),
    //                                      "MesosBatch-3" -> List("Batch"),
    //                                      "MesosBatch-4" -> List("Batch"))
    val mesosSchedWorkloadsToSweep = Map("MesosService" -> List("Service"))

    // val mesosWorkloadToSweep = "Batch"
    val mesosWorkloadToSweep = "Service"

    val runMonolithic = true
    val runMesos = true
    val runOmega = true

    val constantRange = (0.1 :: 1.0 :: 10.0 :: Nil)
    // val constantRange = medConstantRange
    // val constantRange = fullConstantRange
    val perTaskRange = (0.005 :: Nil)
    // val perTaskRange = medPerTaskRange
    // val perTaskRange = fullPerTaskRange
    val pickinessRange = fullPickinessRange
    // val lambdaRange = fullLambdaRange
    val interArrivalScaleRange = 0.009 :: 0.01 :: 0.02 :: 0.1 :: 0.2 :: 1.0 :: Nil
    // val interArrivalScaleRange = lambdaRange.map(1/_)
    val prefillCpuLim = Map("PrefillBatchService" -> 0.6)
    val doLogging = false
    val timeout = 60.0 * 60.0 // In seconds.

    val sweepC = true
    val sweepL = false
    val sweepCL = false
    val sweepPickiness = false
    val sweepLambda = false

    var sweepDimensions = collection.mutable.ListBuffer[String]()
    if (sweepC)
      sweepDimensions += "C"
    if (sweepL)
      sweepDimensions += "L"
    if (sweepCL)
      sweepDimensions += "CL"
    if (sweepPickiness)
      sweepDimensions += "Pickiness"
    if (sweepLambda)
      sweepDimensions += "Lambda"

    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val dateTimeStamp = formatter.format(new java.util.Date)
    // Make the experiment_results dir if it doesn't exist
    val experDir = new java.io.File("experiment_results")
    if (!experDir.exists) {
      println("Creating the 'experiment_results' dir.")
      experDir.mkdir()
    }
    val outputDirName = "%s/%s-%s-%s-%.0f"
                        .format(
                        experDir.toString,
                        dateTimeStamp,
                        "vary_" + sweepDimensions.mkString("_"),
                        wlDescs.map(i => {
                          i.cell + i.assignmentPolicy +
                          (if (i.prefillWorkloadGenerators.length > 0) {
                              "_prefilled"
                           } else {""})
                        }).mkString("_"),
                        globalRunTime)
    println("outputDirName is %s".format(outputDirName))

    if (runOmega) {
      // Set up Omega Experiments
      omegaSimulatorSetups.foreach { case(simDescs, schedWLMap, schedWLToSweep) => {
        for (simDesc <- simDescs) {
          val numServiceScheds =
              simDesc.schedulerDescs.filter(_.name.contains("Service")).size
          val numBatchScheds =
              simDesc.schedulerDescs.filter(_.name.contains("Batch")).size
          if (sweepC) {
            allExperiments ::= new Experiment(
                name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_c"
                       .format(simDesc.conflictMode,
                               simDesc.transactionMode,
                               numServiceScheds,
                               numBatchScheds),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedWLToSweep,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = (0.005 :: Nil),
                blackListPercentRange = (0.0 :: Nil),
                schedulerWorkloadMap = schedWLMap,
                simulatorDesc = simDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
          }

          if (sweepCL) {
            allExperiments ::= new Experiment(
                name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_cl"
                       .format(simDesc.conflictMode,
                               simDesc.transactionMode,
                               numServiceScheds,
                               numBatchScheds),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedWLToSweep,
                constantThinkTimeRange = constantRange,
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = (0.0 :: Nil),
                schedulerWorkloadMap = schedWLMap,
                simulatorDesc = simDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
          }

          if (sweepL) {
            allExperiments ::= new Experiment(
                name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_l"
                       .format(simDesc.conflictMode,
                               simDesc.transactionMode,
                               numServiceScheds,
                               numBatchScheds),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedWLToSweep,
                constantThinkTimeRange = (0.1 :: Nil),
                perTaskThinkTimeRange = perTaskRange,
                blackListPercentRange = (0.0 :: Nil),
                schedulerWorkloadMap = schedWLMap,
                simulatorDesc = simDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
          }

          if (sweepPickiness) {
            allExperiments ::= new Experiment(
                name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_pickiness"
                       .format(simDesc.conflictMode,
                               simDesc.transactionMode,
                               numServiceScheds,
                               numBatchScheds),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedWLToSweep,
                constantThinkTimeRange = (0.1 :: Nil),
                perTaskThinkTimeRange = (0.005 :: Nil),
                blackListPercentRange = pickinessRange,
                schedulerWorkloadMap = schedWLMap,
                simulatorDesc = simDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
          }

          if (sweepLambda) {
            allExperiments ::= new Experiment(
                name = "google-omega-%s-%s-%d_service-%d_batch-single_path-vary_lambda"
                       .format(simDesc.conflictMode,
                               simDesc.transactionMode,
                               numServiceScheds,
                               numBatchScheds),
                workloadToSweepOver = "Service",
                workloadDescs = wlDescs,
                schedulerWorkloadsToSweepOver = schedWLToSweep,
                avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
                constantThinkTimeRange = (0.1 :: Nil),
                perTaskThinkTimeRange = (0.005 :: Nil),
                blackListPercentRange = (0.0 :: Nil),
                schedulerWorkloadMap = schedWLMap,
                simulatorDesc = simDesc,
                logging = doLogging,
                outputDirectory = outputDirName,
                prefillCpuLimits = prefillCpuLim,
                simulationTimeout = timeout)
          }
        }
      }}
    }

    // Set up Mesos experiments, one each for a sweep over l, c, lambda.
    if (runMesos) {
      if (sweepC) {
        allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_c",
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            // constantThinkTimeRange = (0.1 :: Nil), 
            constantThinkTimeRange = constantRange, 
            perTaskThinkTimeRange = (0.005 :: Nil),
            blackListPercentRange = (0.0 :: Nil),
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
      }

      if (sweepCL) {
        allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_cl",
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            constantThinkTimeRange = constantRange,
            perTaskThinkTimeRange = perTaskRange,
            blackListPercentRange = (0.0 :: Nil),
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
      }

      if (sweepL) {
        allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_l",
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            constantThinkTimeRange = (0.1 :: Nil),
            perTaskThinkTimeRange = perTaskRange,
            blackListPercentRange = (0.0 :: Nil),
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
      }

      if (sweepPickiness) {
        allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_pickiness",
            workloadToSweepOver = mesosWorkloadToSweep,
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = mesosSchedWorkloadsToSweep,
            constantThinkTimeRange = (0.1 :: Nil),
            perTaskThinkTimeRange = (0.005 :: Nil),
            blackListPercentRange = pickinessRange,
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
      }

      if (sweepLambda) {
        allExperiments ::= new Experiment(
            name = "google-mesos-single_path-vary_lambda",
            workloadToSweepOver = "Service",
            workloadDescs = wlDescs,
            schedulerWorkloadsToSweepOver = Map("MesosService" -> List("Service")),
            avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
            constantThinkTimeRange = (0.1 :: Nil),
            perTaskThinkTimeRange = (0.005 :: Nil),
            blackListPercentRange = (0.0 :: Nil),
            schedulerWorkloadMap = mesosSchedulerWorkloadMap,
            simulatorDesc = mesosSimulatorDesc,
            logging = doLogging,
            outputDirectory = outputDirName,
            prefillCpuLimits = prefillCpuLim,
            simulationTimeout = timeout)
      }
    }

    if (runMonolithic) {
      // Loop over both a single and multi path Monolithic scheduler.
      // Emulate a single path scheduler by making the parameter sweep
      // apply to both the "Service" and "Batch" workload types for it.
      val multiPathSetup = ("multi", Map("Monolithic" -> List("Service")))
      val singlePathSetup =
          ("single", Map("Monolithic" -> List("Service", "Batch")))
      List(singlePathSetup, multiPathSetup).foreach {
           case (multiOrSingle, schedulerWorkloadsMap) => {
        if (sweepC) {
          allExperiments ::= new Experiment(
              name = "google-monolithic-%s_path-vary_c"
                     .format(multiOrSingle),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = (0.005 :: Nil),
              blackListPercentRange = (0.0 :: Nil),
              schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
              simulatorDesc = monolithicSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
        }

        if (sweepCL) {
          allExperiments ::= new Experiment(
              name = "google-monolithic-%s_path-vary_cl"
                     .format(multiOrSingle),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = constantRange,
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = (0.0 :: Nil),
              schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
              simulatorDesc = monolithicSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
        }

        if (sweepL) {
          allExperiments ::= new Experiment(
              name = "google-monolithic-%s_path-vary_l"
                     .format(multiOrSingle),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = (0.1 :: Nil),
              perTaskThinkTimeRange = perTaskRange,
              blackListPercentRange = (0.0 :: Nil),
              schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
              simulatorDesc = monolithicSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
        }

        if (sweepPickiness) {
          allExperiments ::= new Experiment(
              name = "google-monolithic-%s_path-vary_pickiness"
                     .format(multiOrSingle),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              constantThinkTimeRange = (0.1 :: Nil),
              perTaskThinkTimeRange = (0.005 :: Nil),
              blackListPercentRange = pickinessRange,
              schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
              simulatorDesc = monolithicSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
        }

        if (sweepLambda) {
          allExperiments ::= new Experiment(
              name = "google-monolithic-%s_path-vary_lambda"
                     .format(multiOrSingle),
              workloadToSweepOver = "Service",
              workloadDescs = wlDescs,
              schedulerWorkloadsToSweepOver = schedulerWorkloadsMap,
              avgJobInterarrivalTimeRange = Some(interArrivalScaleRange),
              constantThinkTimeRange = (0.1 :: Nil),
              perTaskThinkTimeRange = (0.005 :: Nil),
              blackListPercentRange = (0.0 :: Nil),
              schedulerWorkloadMap = monolithicSchedulerWorkloadMap,
              simulatorDesc = monolithicSimulatorDesc,
              logging = doLogging,
              outputDirectory = outputDirName,
              prefillCpuLimits = prefillCpuLim,
              simulationTimeout = timeout)
        }
      }}
    }

          
    /* Make a snapshot of the source file that has our settings in it */
    println("Making a copy of Simulation.scala in %s"
            .format(outputDirName))
    val settingsFileName = "Simulation.scala"
    val sourceFile = new File("src/main/scala/" + settingsFileName)
    val destFile = new File(outputDirName + "/" + settingsFileName +
                            "-snapshot")
    // Create the output directory if it doesn't exist.
    (new File(outputDirName)).mkdirs()
    if(!destFile.exists()) {
        destFile.createNewFile();
    }
    var source: FileChannel = null
    var destination: FileChannel = null

    try {
        source = new FileInputStream(sourceFile).getChannel();
        destination = new FileOutputStream(destFile).getChannel();
        destination.transferFrom(source, 0, source.size());
    }
    finally {
        if(source != null) {
            source.close();
        }
        if(destination != null) {
            destination.close();
        }
    }

    /**
     * Run the experiments we've set up.
     */
    if (!inputArgs("--random-seed").equals("")) {
      println("Using random seed %d.".format(inputArgs("--random-seed").toLong))
      Seed.set(inputArgs("--random-seed").toLong)
    } else {
      val randomSeed = util.Random.nextLong
      println("--random-seed not set. So using the default seed: %d."
              .format(0))
      Seed.set(0)
    }
    println("Using %d threads.".format(inputArgs("--thread-pool-size").toInt))
    val pool = java.util
                   .concurrent
                   .Executors
                   .newFixedThreadPool(inputArgs("--thread-pool-size").toInt)
    val numTotalExps = allExperiments.length
    var numFinishedExps = 0
    var futures = allExperiments.map(pool.submit)
    // Let go of pointers to Experiments because each Experiment will use
    // quite a lot of memory.
    allExperiments = Nil
    pool.shutdown()
    while(!futures.isEmpty) {
      Thread.sleep(5 * 1000)
      val (completed, running) = futures.partition(_.isDone)
      if (completed.length > 0) {
        numFinishedExps += completed.length
        println("%d more experiments just finished running. In total, %d of %d have finished."
                .format(completed.length, numFinishedExps, numTotalExps))
      }
      completed.foreach(x => try x.get() catch {
        case e => e.printStackTrace()
      })
      futures = running
    }
    println("Done running all experiments. See output in %s."
            .format(outputDirName))
  }
}

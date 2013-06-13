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

import collection.mutable.HashMap
import collection.mutable.ListBuffer

class MesosSimulatorDesc(
    schedulerDescs: Seq[MesosSchedulerDesc],
    runTime: Double,
    val allocatorConstantThinkTime: Double)
   extends ClusterSimulatorDesc(runTime){
  override
  def newSimulator(constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   blackListPercent: Double,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                   workloadToSchedulerMap: Map[String, Seq[String]],
                   cellStateDesc: CellStateDesc,
                   workloads: Seq[Workload],
                   prefillWorkloads: Seq[Workload],
                   logging: Boolean = false): ClusterSimulator = {
    var schedulers = HashMap[String, MesosScheduler]()
    // Create schedulers according to experiment parameters.
    schedulerDescs.foreach(schedDesc => {
      // If any of the scheduler-workload pairs we're sweeping over
      // are for this scheduler, then apply them before
      // registering it.
      var constantThinkTimes = HashMap[String, Double](
          schedDesc.constantThinkTimes.toSeq: _*)
      var perTaskThinkTimes = HashMap[String, Double](
          schedDesc.perTaskThinkTimes.toSeq: _*)
      var newBlackListPercent = 0.0
      if (schedulerWorkloadsToSweepOver
          .contains(schedDesc.name)) {
        newBlackListPercent = blackListPercent
        schedulerWorkloadsToSweepOver(schedDesc.name)
            .foreach(workloadName => {
          constantThinkTimes(workloadName) = constantThinkTime
          perTaskThinkTimes(workloadName) = perTaskThinkTime
        })
      }
      schedulers(schedDesc.name) =
          new MesosScheduler(schedDesc.name,
                             constantThinkTimes.toMap,
                             perTaskThinkTimes.toMap,
                             schedDesc.schedulePartialJobs,
                             math.floor(newBlackListPercent *
                               cellStateDesc.numMachines.toDouble).toInt)
    })
    // It shouldn't matter which transactionMode we choose, but it does
    // matter that we use "resource-fit" conflictMode or else
    // responses to resource offers will likely fail.
    val cellState = new CellState(cellStateDesc.numMachines,
                                  cellStateDesc.cpusPerMachine,
                                  cellStateDesc.memPerMachine,
                                  conflictMode = "resource-fit",
                                  transactionMode = "all-or-nothing")

    val allocator =
        new MesosAllocator(allocatorConstantThinkTime)

    new MesosSimulator(cellState,
                       schedulers.toMap,
                       workloadToSchedulerMap,
                       workloads,
                       prefillWorkloads,
                       allocator,
                       logging)
  }
}

class MesosSimulator(cellState: CellState,
                     override val schedulers: Map[String, MesosScheduler],
                     workloadToSchedulerMap: Map[String, Seq[String]],
                     workloads: Seq[Workload],
                     prefillWorkloads: Seq[Workload],
                     var allocator: MesosAllocator,
                     logging: Boolean = false,
                     monitorUtilization: Boolean = true)
                    extends ClusterSimulator(cellState,
                                             schedulers,
                                             workloadToSchedulerMap,
                                             workloads,
                                             prefillWorkloads,
                                             logging,
                                             monitorUtilization) {
  assert(cellState.conflictMode.equals("resource-fit"),
         "Mesos requires cellstate to be set up with resource-fit conflictMode")
  // Set up a pointer to this simulator in the allocator.
  allocator.simulator = this

  log("========================================================")
  log("Mesos SIM CONSTRUCTOR - CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s)."
                .format(cellState.totalOccupiedCpus,
                        cellState.totalOccupiedCpus /
                        cellState.totalCpus * 100.0,
                        "%",
                        cellState.totalOccupiedMem,
                        cellState.totalOccupiedMem /
                          cellState.totalMem * 100.0,
                        "%"))

  // Set up a pointer to this simulator in each scheduler.
  schedulers.values.foreach(_.mesosSimulator = this)
}

class MesosSchedulerDesc(name: String,
                         constantThinkTimes: Map[String, Double],
                         perTaskThinkTimes: Map[String, Double],
                         val schedulePartialJobs: Boolean)
                        extends SchedulerDesc(name,
                                              constantThinkTimes,
                                              perTaskThinkTimes)

class MesosScheduler(name: String,
                     constantThinkTimes: Map[String, Double],
                     perTaskThinkTimes: Map[String, Double],
                     val schedulePartialJobs: Boolean,
                     numMachinesToBlackList: Double = 0)
                    extends Scheduler(name,
                                      constantThinkTimes,
                                      perTaskThinkTimes,
                                      numMachinesToBlackList) {
  println("scheduler-id-info: %d, %s, %d, %s, %s"
          .format(Thread.currentThread().getId(),
                  name,
                  hashCode(),
                  constantThinkTimes.mkString(";"),
                  perTaskThinkTimes.mkString(";")))
  // TODO(andyk): Clean up these <subclass>Simulator classes
  //              by templatizing the Scheduler class and having only
  //              one simulator of the correct type, instead of one
  //              simulator for each of the parent and child classes.
  var mesosSimulator: MesosSimulator = null
  val offerQueue = new collection.mutable.Queue[Offer]

  override
  def checkRegistered = {
    super.checkRegistered
    assert(mesosSimulator != null, "This scheduler has not been added to a " +
                                   "simulator yet.")
  }

  /**
   * How an allocator sends offers to a framework.
   */
  def resourceOffer(offer: Offer): Unit = {
    offerQueue.enqueue(offer)
    handleNextResourceOffer()
  }

  def handleNextResourceOffer(): Unit = {
    // We essentially synchronize access to this scheduling logic
    // via the scheduling variable. We aren't protecting this from real
    // parallelism, but rather from discrete-event-simlation style parallelism.
    if(!scheduling && !offerQueue.isEmpty) {
      scheduling = true
      val offer = offerQueue.dequeue()
      // Use this offer to attempt to schedule jobs.
      simulator.log("------ In %s.resourceOffer(offer %d).".format(name, offer.id))
      val offerResponse = collection.mutable.ListBuffer[ClaimDelta]()
      var aggThinkTime: Double = 0.0
      // TODO(andyk): add an efficient method to CellState that allows us to
      //              check the largest slice of available resources to decode
      //              if we should keep trying to schedule or not.
      while (offer.cellState.availableCpus > 0.000001 &&
             offer.cellState.availableMem > 0.000001 &&
             !pendingQueue.isEmpty) {
        val job = pendingQueue.dequeue
        job.updateTimeInQueueStats(simulator.currentTime)
        val jobThinkTime = getThinkTime(job)
        aggThinkTime += jobThinkTime
        job.numSchedulingAttempts += 1
        job.numTaskSchedulingAttempts += job.unscheduledTasks

        // Before calling the expensive scheduleJob() function, check
        // to see if one of this job's tasks could fit into the sum of
        // *all* the currently free resources in the offers' cell state.
        // If one can't, then there is no need to call scheduleJob(). If 
        // one can, we call scheduleJob(), though we still might not fit
        // any tasks due to fragmentation.
        if (offer.cellState.availableCpus > job.cpusPerTask &&
            offer.cellState.availableMem > job.cpusPerTask) {
          // Schedule the job using the cellstate in the ResourceOffer.
          val claimDeltas = scheduleJob(job, offer.cellState)
          if(claimDeltas.length > 0) {
            numSuccessfulTransactions += 1
            recordUsefulTimeScheduling(job,
                                       jobThinkTime,
                                       job.numSchedulingAttempts == 1)
            mesosSimulator.log(("Setting up job %d to accept at least " +
                                "part of offer %d. About to spend %f seconds " +
                                "scheduling it. Assigning %d tasks to it.")
                                .format(job.id, offer.id, jobThinkTime,
                                        claimDeltas.length))
            offerResponse ++= claimDeltas
            job.unscheduledTasks -= claimDeltas.length
          } else {
            mesosSimulator.log(("Rejecting all of offer %d for job %d, " +
                                "which requires tasks with %f cpu, %f mem. " +
                                "Not counting busy time for this sched attempt.")
                                .format(offer.id,
                                        job.id,
                                        job.cpusPerTask,
                                        job.memPerTask))
            numNoResourcesFoundSchedulingAttempts += 1
          }
        } else {
          mesosSimulator.log(("Short-path rejecting all of offer %d for " +
                              "job %d because a single one of its tasks " +
                              "(%f cpu, %f mem) wouldn't fit into the sum " +
                              "of the offer's private cell state's " +
                              "remaining resources (%f cpu, %f mem).")
                              .format(offer.id,
                                      job.id,
                                      job.cpusPerTask,
                                      job.memPerTask,
                                      offer.cellState.availableCpus,
                                      offer.cellState.availableMem))
        }

        var jobEventType = "" // Set this conditionally below; used in logging.
        // If job is only partially scheduled, put it back in the pendingQueue.
        if (job.unscheduledTasks > 0) {
          mesosSimulator.log(("Job %d is [still] only partially scheduled, " +
                             "(%d out of %d its tasks remain unscheduled) so " +
                             "putting it back in the queue.")
                             .format(job.id,
                                     job.unscheduledTasks,
                                     job.numTasks))
          // Give up on a job if (a) it hasn't scheduled a single task in
          // 100 tries or (b) it hasn't finished scheduling after 1000 tries.
          if ((job.numSchedulingAttempts > 100 &&
               job.unscheduledTasks == job.numTasks) ||
              job.numSchedulingAttempts > 1000) {
            println(("Abandoning job %d (%f cpu %f mem) with %d/%d " +
                   "remaining tasks, after %d scheduling " +
                   "attempts.").format(job.id,
                                       job.cpusPerTask,
                                       job.memPerTask,
                                       job.unscheduledTasks,
                                       job.numTasks,
                                       job.numSchedulingAttempts))
            numJobsTimedOutScheduling += 1
            jobEventType = "abandoned"
          } else {
            simulator.afterDelay(1) {
              addJob(job)
            }
          }
          job.lastEnqueued = simulator.currentTime
        } else {
          // All tasks in job scheduled so not putting it back in pendingQueue.
          jobEventType = "fully-scheduled"
        }
        if (!jobEventType.equals("")) {
          // Print some stats that we can use to generate CDFs of the job
          // # scheduling attempts and job-time-till-scheduled.
          // println("%s %s %d %s %d %d %f"
          //         .format(Thread.currentThread().getId(),
          //                 name,
          //                 hashCode(),
          //                 jobEventType,
          //                 job.id,
          //                 job.numSchedulingAttempts,
          //                 simulator.currentTime - job.submitted))
        }
      }

      if (pendingQueue.isEmpty) {
        // If we have scheduled everything, notify the allocator that we
        // don't need resources offers until we request them again (which
        // we will do when another job is added to our pendingQueue.
        // Do this before we reply to the offer since the allocator may make
        // its next round of offers shortly after we respond to this offer.
        mesosSimulator.log(("After scheduling, %s's pending queue is " +
                            "empty, canceling outstanding " +
                            "resource request.").format(name))
        mesosSimulator.allocator.cancelOfferRequest(this)
      } else {
        mesosSimulator.log(("%s's pending queue still has %d jobs in it, but " +
                            "for some reason, they didn't fit into this " +
                            "offer, so it will patiently wait for more " +
                            "resource offers.").format(name, pendingQueue.size))
      }

      // Send our response to this offer.
      mesosSimulator.afterDelay(aggThinkTime) {
        mesosSimulator.log(("Waited %f seconds of aggThinkTime, now " +
                            "responding to offer %d with %d responses after.")
                           .format(aggThinkTime, offer.id, offerResponse.length))
        mesosSimulator.allocator.respondToOffer(offer, offerResponse)
      }
      // Done with this offer, see if we have another one to handle.
      scheduling = false
      handleNextResourceOffer()
    }
  }

  // When a job arrives, notify the allocator, so that it can make us offers
  // until we notify it that we don't have any more jobs, at which time it
  // can stop sending us offers.
  override
  def addJob(job: Job) = {
    assert(simulator != null, "This scheduler has not been added to a " +
                              "simulator yet.")
      simulator.log("========================================================")
      simulator.log("addJOB: CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s)."
                    .format(simulator.cellState.totalOccupiedCpus,
                            simulator.cellState.totalOccupiedCpus /
                              simulator.cellState.totalCpus * 100.0,
                            "%",
                            simulator.cellState.totalOccupiedMem,
                            simulator.cellState.totalOccupiedMem /
                              simulator.cellState.totalMem * 100.0,
                            "%"))
    super.addJob(job)
    pendingQueue.enqueue(job)
    simulator.log("Enqueued job %d of workload type %s."
                  .format(job.id, job.workloadName))
    mesosSimulator.allocator.requestOffer(this)
  }
}

/**
 * Decides which scheduler to make resource offer to next, and manages
 * the resource offer process.
 *
 * @param constantThinkTime the time this scheduler takes to sort the
 *       list of schedulers to decide which to offer to next. This happens
 *       before each series of resource offers is made.
 * @param resources How many resources is managed by this MesosAllocator
 */
class MesosAllocator(constantThinkTime: Double,
                     minCpuOffer: Double = 100.0,
                     minMemOffer: Double = 100.0,
                     // Min time, in seconds, to batch up resources
                     // before making an offer.
                     val offerBatchInterval: Double = 1.0) {
  var simulator: MesosSimulator = null
  var allocating: Boolean = false
  var schedulersRequestingResources = collection.mutable.Set[MesosScheduler]()
  var timeSpentAllocating: Double = 0.0
  var nextOfferId: Long = 0
  val offeredDeltas = HashMap[Long, Seq[ClaimDelta]]()
  // Are we currently waiting while a resource batch offer builds up
  // that has already been scheduled?
  var buildAndSendOfferScheduled = false

  def checkRegistered = {
    assert(simulator != null, "You must assign a simulator to a " +
                              "MesosAllocator before you can use it.")
  }

  def getThinkTime: Double = {
    constantThinkTime
  }

  def requestOffer(needySched: MesosScheduler) {
    checkRegistered
    simulator.log("Received an offerRequest from %s.".format(needySched.name))
    // Adding a scheduler to this list will ensure that it gets included
    // in the next round of resource offers.
    schedulersRequestingResources += needySched
    schedBuildAndSendOffer()
  }

  def cancelOfferRequest(needySched: MesosScheduler) = {
    simulator.log("Canceling the outstanding resourceRequest for scheduler %s.".format(
        needySched.name))
    schedulersRequestingResources -= needySched
  }

  /**
   * We batch up available resources into periodic offers so
   * that we don't send an offer in response to *every* small event,
   * which adds latency to the average offer and slows the simulator down.
   * This feature was in Mesos for the NSDI paper experiments, but didn't
   * get committed to the open source codebase at that time.
   */
  def schedBuildAndSendOffer() = {
    if (!buildAndSendOfferScheduled) {
      buildAndSendOfferScheduled = true
      simulator.afterDelay(offerBatchInterval) {
        simulator.log("Building and sending a batched offer")
        buildAndSendOffer()
        // Let another call to buildAndSendOffer() get scheduled,
        // giving some time for resources to build up that are
        // becoming available due to tasks finishing.
        buildAndSendOfferScheduled = false
      }
    }
  }
  /**
   * Sort schedulers in simulator using DRF, then make an offer to
   * the first scheduler in the list.
   *
   * After any task finishes or scheduler says it wants offers, we
   * call this, i.e. buildAndSendOffer(), again. Note that the only
   * resources that will be available will be the ones that
   * the task that just finished was using).
   */
  def buildAndSendOffer(): Unit = {
    checkRegistered
    simulator.log("========================================================")
    simulator.log(("TOP OF BUILD AND SEND. CellState total occupied: " +
                   "%fcpus (%.1f%%), %fmem (%.1f%%).")
                  .format(simulator.cellState.totalOccupiedCpus,
                          simulator.cellState.totalOccupiedCpus /
                            simulator.cellState.totalCpus * 100.0,
                          simulator.cellState.totalOccupiedMem,
                          simulator.cellState.totalOccupiedMem /
                            simulator.cellState.totalMem * 100.0))
    // Build and send an offer only if:
    // (a) there are enough resources in cellstate and
    // (b) at least one scheduler wants offers currently
    // Else, don't do anything, since this function will be called
    // again when a task finishes or a scheduler says it wants offers.
    if (!schedulersRequestingResources.isEmpty &&
      simulator.cellState.availableCpus >= minCpuOffer &&
      simulator.cellState.availableCpus >= minMemOffer) {
      // Use DRF to pick a candidate scheduler to offer resources.
      val sortedSchedulers =
          drfSortSchedulers(schedulersRequestingResources.toSeq)
      sortedSchedulers.headOption.foreach(candidateSched => {
        // Create an offer by taking a snapshot of cell state. We might
        // discard this without sending it if we find that there are
        // no available resources in cell state right now.
        val privCellState = simulator.cellState.copy
        val offer = Offer(nextOfferId, candidateSched, privCellState)
        nextOfferId += 1

        // Call scheduleAllAvailable() which creates deltas, applies them,
        // and returns them; all based on common cell state. This doesn't
        // affect the privateCellState we created above. Store the deltas
        // using the offerID as key until we get a response from the scheduler.
        // This has the effect of pessimistally locking the resources in
        // common cell state until we hear back from the scheduler (or time
        // out and rescind the offer).
        val claimDeltas =
            candidateSched.scheduleAllAvailable(cellState = simulator.cellState,
                                                locked = true)
        // Make sure scheduleAllAvailable() did its job.
        assert(simulator.cellState.availableCpus < 0.01 &&
               simulator.cellState.availableMem < 0.01,
               ("After scheduleAllAvailable() is called on a cell state " +
                "that cells state should not have any available resources " +
                "of any type, but this cell state still has %f cpus and %f " +
                "memory available").format(simulator.cellState.availableCpus,
                                           simulator.cellState.availableMem))
        if (!claimDeltas.isEmpty) {
          assert(privCellState.totalLockedCpus !=
                 simulator.cellState.totalLockedCpus,
                 "Since some resources were locked and put into a resource " +
                 "offer, we expect the number of total lockedCpus to now be " +
                 "different in the private cell state we created than in the" +
                 "common cell state.")
          offeredDeltas(offer.id) = claimDeltas

          val thinkTime = getThinkTime
          simulator.afterDelay(thinkTime) {
            timeSpentAllocating += thinkTime
            simulator.log(("Allocator done thinking, sending offer to %s. " +
                           "Offer contains private cell state with " +
                           "%f cpu, %f mem available.")
                          .format(candidateSched.name,
                                  offer.cellState.availableCpus,
                                  offer.cellState.availableMem))
            // Send the offer.
            candidateSched.resourceOffer(offer)
          }
        }
      })
    } else {
      var reason = ""
      if (schedulersRequestingResources.isEmpty)
        reason = "No schedulers currently want offers."
      if (simulator.cellState.availableCpus < minCpuOffer ||
          simulator.cellState.availableCpus < minMemOffer)
        reason = ("Only %f cpus and %f mem available in common cell state " +
                  "but min offer size is %f cpus and %f mem.")
                  .format(simulator.cellState.availableCpus,
                          simulator.cellState.availableCpus,
                          minCpuOffer,
                          minMemOffer)
      simulator.log("Not sending an offer after all. %s".format(reason))
    }
  }

  /**
   * Schedulers call this to respond to resource offers.
   */
  def respondToOffer(offer: Offer, claimDeltas: Seq[ClaimDelta]) = {
    checkRegistered
    simulator.log(("------Scheduler %s responded to offer %d with " +
                   "%d claimDeltas.")
                  .format(offer.scheduler.name, offer.id, claimDeltas.length))

    // Look up, unapply, & discard the saved deltas associated with the offerid.
    // This will cause the framework to stop being charged for the resources that
    // were locked while he made his scheduling decision.
    assert(offeredDeltas.contains(offer.id),
           "Allocator received response to offer that is not on record.")
    offeredDeltas.remove(offer.id).foreach(savedDeltas => {
      savedDeltas.foreach(_.unApply(cellState = simulator.cellState,
                                    locked = true))
    })
      simulator.log("========================================================")
      simulator.log("AFTER UNAPPLYING SAVED DELTAS")
      simulator.log("CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s)."
                    .format(simulator.cellState.totalOccupiedCpus,
                            simulator.cellState.totalOccupiedCpus /
                              simulator.cellState.totalCpus * 100.0,
                            "%",
                            simulator.cellState.totalOccupiedMem,
                            simulator.cellState.totalOccupiedMem /
                              simulator.cellState.totalMem * 100.0,
                            "%"))
    simulator.log("Committing all %d deltas that were part of response %d "
                  .format(claimDeltas.length, offer.id))
    // commit() all deltas that were part of the offer response, don't use
    // the option of having cell state create the end events for us since we
    // want to add code to the end event that triggers another resource offer.
    if (claimDeltas.length > 0) {
      val commitResult = simulator.cellState.commit(claimDeltas, false)
      assert(commitResult.conflictedDeltas.length == 0,
             "Expecting no conflicts, but there were %d."
             .format(commitResult.conflictedDeltas.length))

      // Create end events for all tasks committed.
      commitResult.committedDeltas.foreach(delta => {
        simulator.afterDelay(delta.duration) {
          delta.unApply(simulator.cellState)
          simulator.log(("A task started by scheduler %s finished. " +
                         "Freeing %f cpus, %f mem. Available: %f cpus, %f " +
                         "mem. Also, triggering a new batched offer round.")
                       .format(delta.scheduler.name,
                               delta.cpus,
                               delta.mem,
                               simulator.cellState.availableCpus,
                               simulator.cellState.availableMem))
          schedBuildAndSendOffer()
        }
      })
    }
    schedBuildAndSendOffer()
  }

  /**
   * 1/N multi-resource fair sharing.
   */
  def drfSortSchedulers(schedulers: Seq[MesosScheduler]): Seq[MesosScheduler] = {
    val schedulerDominantShares = schedulers.map(scheduler => {
      val shareOfCpus =
          simulator.cellState.occupiedCpus.getOrElse(scheduler.name, 0.0)
      val shareOfMem =
          simulator.cellState.occupiedMem.getOrElse(scheduler.name, 0.0)
      val domShare = math.max(shareOfCpus / simulator.cellState.totalCpus,
                              shareOfMem / simulator.cellState.totalMem)
      var nameOfDomShare = ""
      if (shareOfCpus > shareOfMem) nameOfDomShare = "cpus"
      else nameOfDomShare = "mem"
      simulator.log("%s's dominant share is %s (%f%s)."
                    .format(scheduler.name, nameOfDomShare, domShare, "%"))
      (scheduler, domShare)
    })
    schedulerDominantShares.sortBy(_._2).map(_._1)
  }
}

case class Offer(id: Long, scheduler: MesosScheduler, cellState: CellState)

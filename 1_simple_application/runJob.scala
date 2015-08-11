/* Spark-1.4.0 */

/* This function trace answers to the question, how does Spark cluster
 * run a job?
 */


// Driver Node
SparkContext.runJob()
    DAGScheduler.runJob()
        DAGScheduler.submitJob()
            EventLoop[DAGSchedulerEvent].post(JobSubmitted)

// Driver Node
EventLoop[DAGSchedulerEvent].onReceive(JobSubmitted)
    DAGScheduler.handleJobSubmitted()
        DAGScheduler.submitStage()
            DAGScheduler.submitMissingTasks() {
                val partitionsToCompute: Seq[Int] = { /* ... */ }
                var taskBinary: Broadcast[Array[Byte]] = null
                val taskBinaryBytes: Array[Byte] = stage match {
                    case stage: ShuffleMapStage =>
                        closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
                    case stage: ResultStage =>
                        closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
                }
                taskBinary = sc.broadcast(taskBinaryBytes)
                val tasks: Seq[Task[_]] = stage match {
                    case stage: ShuffleMapStage =>
                        partitionsToCompute.map { id =>
                            val locs = getPreferredLocs(stage.rdd, id)
                            val part = stage.rdd.partitions(id)
                            new ShuffleMapTask(stage.id, taskBinary, part, locs)
                        }
                    case stage: ResultStage =>
                        val job = stage.resultOfJob.get
                        partitionsToCompute.map { id =>
                            val p: Int = job.partitions(id)
                            val part = stage.rdd.partitions(p)
                            val locs = getPreferredLocs(stage.rdd, p)
                            new ResultTask(stage.id, taskBinary, part, locs, id)
                        }
                }
                taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
            }
            TaskSchedulerImpl.submitTasks()
                CoarseGrainedSchedulerBackend.reviveOffers()
                    CoarseGrainedSchedulerBackend.DriverEndpoint.send(ReviveOffers)

// Driver Node
CoarseGrainedSchedulerBackend.DriverEndpoint.receive(ReviveOffers)
    CoarseGrainedSchedulerBackend.DriverEndpoint.makeOffers()
        TaskSchedulerImpl.resourceOffers()
            TaskSchedulerImpl.resourceOfferSingleTaskSet()
                TaskSetManager.resourceOffer()
                    DAGScheduler.taskStarted()
                        DAGScheduler.DAGSchedulerEventProcessLoop.post(BeginEvent)
                            DAGScheduler.handleBeginEvent()
                                LiveListenerBus.post(SparkListenerTaskStart) // SparkListener
                                    EventLoggingListener.onTaskStart()
        CoarseGrainedSchedulerBackend.DriverEndpoint.launchTasks()
            ExecutorData.executorEndpoint.send(LaunchTask(serializedTask))


// Worker Node
/* Suppose the task given to the worker node is: */
sc.textFile("simple.txt")       // HadoopRDD => MapParititonsRDD-1
    .map(name => name.reverse)  // MapPartitionsRDD-1 => MapPartitionsRDD-2
    .collect()
/* Then, the function trace will look like below: */
CoarseGrainedExecutorBackend.receive(LaunchTask(data))
    Executor.launchTask()
        Executor.TaskRunner.run()
            Task.run()
            /* [[org.apache.spark.scheduler.ShuffleMapTask]]
             * [[org.apache.spark.scheduler.ResultTask]] */
                ResultTask.runTask() {
                    (rdd, func) = deserialize[( RDD[T], (TaskContext, Iterator[T])=>U )]
                    func(context, rdd.iterator(partition, context))
                }
                    RDD.iterator
                        RDD.computeOrReadCheckpoint()
                            MapPartitionsRDD.compute()  // MapPartitionsRDD-2

                                MapPartitionsRDD.iterator()
                                    MapPartitionsRDD.computeOrReadCheckpoint()
                                        MapPartitionsRDD.compute()  // MapPartitionsRDD-1

                                            HadoopRDD.iterator()
                                                HadoopRDD.computeOrReadCheckpoint()
                                                    HadoopRDD.compute()  // HadoopRDD


/* =============================================================================================== */
/* SparkContext.scala */
/**
 * Run a function on a given set of partitions in an RDD and pass the results to the given
 * handler function. This is the main entry point for all actions in Spark. The allowLocal
 * flag specifies whether the scheduler can run the computation on the driver rather than
 * shipping it out to the cluster, for short actions like first().
 */
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit) {
    if (stopped.get()) {
        throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
        logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
        resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
}


/* =============================================================================================== */
/* scheduler/DAGScheduler.scala */
def runJob[T, U](
        rdd: RDD[T],
        func: (TaskContext, Iterator[T]) => U,
        partitions: Seq[Int],
        callSite: CallSite,
        allowLocal: Boolean,
        resultHandler: (Int, U) => Unit,
        properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    waiter.awaitResult() match {
        case JobSucceeded =>
            logInfo("Job %d finished: %s, took %f s".format
                (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        case JobFailed(exception: Exception) =>
            logInfo("Job %d failed: %s, took %f s".format
                (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
            throw exception
    }
}

/**
 * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
 * can be used to block until the the job finishes executing or can be used to cancel the job.
 */
def submitJob[T, U](
        rdd: RDD[T],
        func: (TaskContext, Iterator[T]) => U,
        partitions: Seq[Int],
        callSite: CallSite,
        allowLocal: Boolean,
        resultHandler: (Int, U) => Unit,
        properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
        throw new IllegalArgumentException(
            "Attempting to access a non-existent partition: " + p + ". " +
            "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
        return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(  // <--------------------------------------------------------
        jobId, rdd, func2, partitions.toArray, allowLocal, callSite, waiter,
        SerializationUtils.clone(properties)))
    waiter
}

private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
        extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {
    // ...
}

/* =============================================================================================== */
/* util/EventLoop.scala */
/**
 * Put the event into the event queue. The event thread will process it later.
 */
def post(event: E): Unit = {
    eventQueue.put(event)
}

private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
        try {
            while (!stopped.get) {
                val event = eventQueue.take()
                try {
                    onReceive(event)
                } catch {
                    case NonFatal(e) => {
                        try {
                            onError(e)
                        } catch {
                            case NonFatal(e) => logError("Unexpected error in " + name, e)
                        }
                    }
                }
            }
        } catch {
            case ie: InterruptedException => // exit even if eventQueue is not empty
            case NonFatal(e) => logError("Unexpected error in " + name, e)
        }
    }
}

/* =============================================================================================== */
/* scheduler/DAGScheduler.scala */
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
        extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {
    /**
     * The main event loop of the DAG scheduler.
     */
    override def onReceive(event: DAGSchedulerEvent): Unit = event match {
        case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
            dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite,
                listener, properties)
        // ...
    }
}

private[scheduler] def handleJobSubmitted(jobId: Int,
        finalRDD: RDD[_],
        func: (TaskContext, Iterator[_]) => _,
        partitions: Array[Int],
        allowLocal: Boolean,
        callSite: CallSite,
        listener: JobListener,
        properties: Properties) {
    var finalStage: ResultStage = null
    try {
        // New stage creation may throw an exception if, for example, jobs are run on a
        // HadoopRDD whose underlying HDFS files have been deleted.
        finalStage = newResultStage(finalRDD, partitions.size, jobId, callSite)
    } catch {
        case e: Exception =>
            logWarning("Creating new stage failed due to exception - job: " + jobId, e)
            listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
        val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
        clearCacheLocs()
        logInfo("Got job %s (%s) with %d output partitions (allowLocal=%s)".format(
            job.jobId, callSite.shortForm, partitions.length, allowLocal))
        logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
        logInfo("Parents of final stage: " + finalStage.parents)
        logInfo("Missing parents: " + getMissingParentStages(finalStage))
        val shouldRunLocally =
            localExecutionEnabled && allowLocal && finalStage.parents.isEmpty && partitions.length == 1
        val jobSubmissionTime = clock.getTimeMillis()
        if (shouldRunLocally) {
            // Compute very short actions like first() or take() with no parent stages locally.
            listenerBus.post(
                SparkListenerJobStart(job.jobId, jobSubmissionTime, Seq.empty, properties))
            runLocally(job)
        } else {
            jobIdToActiveJob(jobId) = job
            activeJobs += job
            finalStage.resultOfJob = Some(job)
            val stageIds = jobIdToStageIds(jobId).toArray
            val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
            listenerBus.post(
                SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
            submitStage(finalStage)
        }
    }
    submitWaitingStages()
}

/** Submits stage, but first recursively submits any missing parents. */
private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
        logDebug("submitStage(" + stage + ")")
        if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
            val missing = getMissingParentStages(stage).sortBy(_.id)

            logDebug("missing: " + missing)
            if (missing.isEmpty) {
                logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
                submitMissingTasks(stage, jobId.get)  // <-------------------------------------------
            } else {
                for (parent <- missing) {
                    submitStage(parent)
                }
                waitingStages += stage
            }
        }
    } else {
        abortStage(stage, "No active job for stage " + stage.id)
    }
}

/** Called when stage's parents are available and we can now do its task. */
private def submitMissingTasks(stage: Stage, jobId: Int) {
  logDebug("submitMissingTasks(" + stage + ")")
  // Get our pending tasks and remember them in our pendingTasks entry
  stage.pendingTasks.clear()


  // First figure out the indexes of partition ids to compute.
  val partitionsToCompute: Seq[Int] = {
    stage match {
      case stage: ShuffleMapStage =>
        (0 until stage.numPartitions).filter(id => stage.outputLocs(id).isEmpty)
      case stage: ResultStage =>
        val job = stage.resultOfJob.get
        (0 until job.numPartitions).filter(id => !job.finished(id))
    }
  }

  val properties = jobIdToActiveJob.get(stage.jobId).map(_.properties).orNull

  runningStages += stage
  // SparkListenerStageSubmitted should be posted before testing whether tasks are
  // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
  // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
  // event.
  stage.latestInfo = StageInfo.fromStage(stage, Some(partitionsToCompute.size))
  outputCommitCoordinator.stageStart(stage.id)
  listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

  // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
  // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
  // the serialized copy of the RDD and for each task we will deserialize it, which means each
  // task gets a different copy of the RDD. This provides stronger isolation between tasks that
  // might modify state of objects referenced in their closures. This is necessary in Hadoop
  // where the JobConf/Configuration object is not thread-safe.
  var taskBinary: Broadcast[Array[Byte]] = null
  try {
    // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
    // For ResultTask, serialize and broadcast (rdd, func).
    val taskBinaryBytes: Array[Byte] = stage match {
      case stage: ShuffleMapStage =>
        closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
      case stage: ResultStage =>
        closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
    }

    taskBinary = sc.broadcast(taskBinaryBytes)
  } catch {
    // In the case of a failure during serialization, abort the stage.
    case e: NotSerializableException =>
      abortStage(stage, "Task not serializable: " + e.toString)
      runningStages -= stage

      // Abort execution
      return
    case NonFatal(e) =>
      abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
      runningStages -= stage
      return
  }

  val tasks: Seq[Task[_]] = stage match {
    case stage: ShuffleMapStage =>
      partitionsToCompute.map { id =>
        val locs = getPreferredLocs(stage.rdd, id)
        val part = stage.rdd.partitions(id)
        new ShuffleMapTask(stage.id, taskBinary, part, locs)
      }

    case stage: ResultStage =>
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = getPreferredLocs(stage.rdd, p)
        new ResultTask(stage.id, taskBinary, part, locs, id)
      }
  }

  if (tasks.size > 0) {
    logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
    stage.pendingTasks ++= tasks
    logDebug("New pending tasks: " + stage.pendingTasks)
    taskScheduler.submitTasks(
      new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
    stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
  } else {
    // Because we posted SparkListenerStageSubmitted earlier, we should mark
    // the stage as completed here in case there are no tasks to run
    markStageAsFinished(stage, None)

    val debugString = stage match {
      case stage: ShuffleMapStage =>
        s"Stage ${stage} is actually done; " +
          s"(available: ${stage.isAvailable}," +
          s"available outputs: ${stage.numAvailableOutputs}," +
          s"partitions: ${stage.numPartitions})"
      case stage : ResultStage =>
        s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
    }
    logDebug(debugString)
  }
}


/* =============================================================================================== */
/* scheduler/TaskSchedulerImpl.scala */
override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
        val manager = createTaskSetManager(taskSet, maxTaskFailures)
        activeTaskSets(taskSet.id) = manager
        schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

        if (!isLocal && !hasReceivedTask) {
            starvationTimer.scheduleAtFixedRate(
                new TimerTask() {
                    override def run() {
                        if (!hasLaunchedTask) {
                            logWarning("Initial job has not accepted any resources; " +
                                "check your cluster UI to ensure that workers are registered " +
                                "and have sufficient resources")
                        } else {
                            this.cancel()
                        }
                    }
                },
                STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS
            )
        }
        hasReceivedTask = true
    }
    backend.reviveOffers()
}


/* =============================================================================================== */
/* scheduler/cluster/SparkDeploySchedulerBackend.scala */

/* =============================================================================================== */
/* scheduler/cluster/CoarseGrainedSchedulerBackend.scala */
  override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }

  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {
    override def receive: PartialFunction[Any, Unit] = {
      case ReviveOffers =>
        makeOffers()
      // ...
    }

    // Make fake resource offers on all executors
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq))
    }
    // ...
  }


/* =============================================================================================== */
/* scheduler/TaskSchedulerImpl.scala */
  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      activeExecutorIds += o.executorId
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
        launchedTask = resourceOfferSingleTaskSet(
            taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetId(tid) = taskSet.taskSet.id
            taskIdToExecutorId(tid) = execId
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }


/* =============================================================================================== */
/* scheduler/TaskSetManager.scala */
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (!isZombie) {
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      dequeueTask(execId, host, allowedLocality) match {
        case Some((index, taskLocality, speculative)) => {
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index)
          val taskId = sched.newTaskId()
          // Do various bookkeeping
          copiesRunning(index) += 1
          val attemptNum = taskAttempts(index).size
          val info = new TaskInfo(taskId, index, attemptNum, curTime,
            execId, host, taskLocality, speculative)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          // NO_PREF will not affect the variables related to delay scheduling
          if (maxLocality != TaskLocality.NO_PREF) {
            currentLocalityIndex = getLocalityIndex(taskLocality)
            lastLaunchTime = curTime
          }
          // Serialize and return the task
          val startTime = clock.getTimeMillis()
          val serializedTask: ByteBuffer = try {
            Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          } catch {
            // If the task cannot be serialized, then there's no point to re-attempt the task,
            // as it will always fail. So just abort the whole task-set.
            case NonFatal(e) =>
              val msg = s"Failed to serialize task $taskId, not attempting to retry it."
              logError(msg, e)
              abort(s"$msg Exception during serialization: $e")
              throw new TaskNotSerializableException(e)
          }
          if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
              !emittedTaskSizeWarning) {
            emittedTaskSizeWarning = true
            logWarning(s"Stage ${task.stageId} contains a task of very large size " +
              s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
              s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
          }
          addRunningTask(taskId)

          // We used to log the time it takes to serialize the task, but task size is already
          // a good proxy to task serialization time.
          // val timeTaken = clock.getTime() - startTime
          val taskName = s"task ${info.id} in stage ${taskSet.id}"
          logInfo("Starting %s (TID %d, %s, %s, %d bytes)".format(
              taskName, taskId, host, taskLocality, serializedTask.limit))

          sched.dagScheduler.taskStarted(task, info)
          // sched : TaskSchedulerImpl
          return Some(new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
            taskName, index, serializedTask))
        }
        case _ =>
      }
    }
    None
  }


/* =============================================================================================== */
/* scheduler/DAGScheduler.scala */
  // Called by TaskScheduler to report task's starting.
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }


/* =============================================================================================== */
/* scheduler/cluster/CoarseGrainedSchedulerBackend.scala */
  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)]) {
    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
          val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
          scheduler.activeTaskSets.get(taskSetId).foreach { taskSet =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                "spark.akka.frameSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                AkkaUtils.reservedSizeBytes)
              taskSet.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // ...
  }


/* =============================================================================================== */
/* scheduler/cluster/ExecutorData.scala */
/**
 * Grouping of data for an executor used by CoarseGrainedSchedulerBackend.
 *
 * @param executorEndpoint The ActorRef representing this executor
 * @param executorAddress The network address of this executor
 * @param executorHost The hostname that this executor is running on
 * @param freeCores  The current number of cores available for work on the executor
 * @param totalCores The total number of cores available to the executor
 */
private[cluster] class ExecutorData(
   val executorEndpoint: RpcEndpointRef,
   val executorAddress: RpcAddress,
   override val executorHost: String,
   var freeCores: Int,
   override val totalCores: Int,
   override val logUrlMap: Map[String, String]
) extends ExecutorInfo(executorHost, totalCores, logUrlMap)


/* =============================================================================================== */
/* Worker Node Side */
/* =============================================================================================== */

/* =============================================================================================== */
/* executor/CoarseGrainedExecutorBackend.scala */
  override def receive: PartialFunction[Any, Unit] = {
    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }
    // ...
  }


/* =============================================================================================== */
/* executor/Executor.scala */
  def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskId: Long,
      val attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer)
    extends Runnable {
    // ...
    override def run(): Unit = {
      val taskMemoryManager = new TaskMemoryManager(env.executorMemoryManager)
      val deserializeStartTime = System.currentTimeMillis()
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles, taskJars)
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        val value = try {
          task.run(taskAttemptId = taskId, attemptNumber = attemptNumber)
        } finally {
          // Note: this memory freeing logic is duplicated in DAGScheduler.runLocallyWithinThread;
          // when changing this, make sure to update both copies.
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()
          if (freedMemory > 0) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logError(errMsg)
            }
          }
        }
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        for (m <- task.metrics) {
          // Deserialization happens in two parts: first, we deserialize a Task object, which
          // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
          m.setExecutorDeserializeTime(
            (taskStart - deserializeStartTime) + task.executorDeserializeTime)
          // We need to subtract Task.run()'s deserialization time to avoid double-counting
          m.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
          m.setJvmGCTime(computeTotalGcTime() - startGCTime)
          m.setResultSerializationTime(afterSerialization - beforeSerialization)
        }

        val accumUpdates = Accumulators.values
        val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException | _: InterruptedException if task.killed =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case cDE: CommitDeniedException =>
          val reason = cDE.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          val metrics: Option[TaskMetrics] = Option(task).flatMap { task =>
            task.metrics.map { m =>
              m.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              m.setJvmGCTime(computeTotalGcTime() - startGCTime)
              m
            }
          }
          val taskEndReason = new ExceptionFailure(t, metrics)
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(taskEndReason))

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        // Release memory used by this thread for shuffles
        env.shuffleMemoryManager.releaseMemoryForThisThread()
        // Release memory used by this thread for unrolling blocks
        env.blockManager.memoryStore.releaseUnrollMemoryForThisThread()
        // Release memory used by this thread for accumulators
        Accumulators.clear()
        runningTasks.remove(taskId)
      }
    }
  }


/* =============================================================================================== */
/* scheduler/Task.scala */
/**
 * A unit of execution. We have two kinds of Task's in Spark:
 * - [[org.apache.spark.scheduler.ShuffleMapTask]]
 * - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 */
private[spark] abstract class Task[T](val stageId: Int, var partitionId: Int) extends Serializable {
  final def run(taskAttemptId: Long, attemptNumber: Int): T = {
    context = new TaskContextImpl(
      stageId = stageId,
      partitionId = partitionId,
      taskAttemptId = taskAttemptId,
      attemptNumber = attemptNumber,
      taskMemoryManager = taskMemoryManager,
      runningLocally = false)
    TaskContext.setTaskContext(context)
    context.taskMetrics.setHostname(Utils.localHostName())
    taskThread = Thread.currentThread()
    if (_killed) {
      kill(interruptThread = false)
    }
    try {
      runTask(context)  // <--------------------------------------------------------------------
    } finally {
      context.markTaskCompleted()
      TaskContext.unset()
    }
  }
  // ...
}


/* =============================================================================================== */
/* scheduler/ShuffleMapTask.scala */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }


/* =============================================================================================== */
/* scheduler/ResultTask.scala */
  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    func(context, rdd.iterator(partition, context))
  }


/* =============================================================================================== */
/* rdd/RDD.scala */
  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointed) firstParent[T].iterator(split, context) else compute(split, context)
  }


/* =============================================================================================== */
/* rdd/MapPartitionsRDD.scala */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))
}

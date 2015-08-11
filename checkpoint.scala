/* spark-1.3.1 */

/*================================================================== */
/* rdd/RDD.scala */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }
// SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)

/*================================================================== */
/* CacheManager.scala */
  def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {
      case Some(blockResult) =>
        // Partition is already materialized, so just return its values
        context.taskMetrics.inputMetrics = Some(blockResult.inputMetrics)
        new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])

      case None =>
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {
          return new InterruptibleIterator[T](context, storedValues.get)
        }

        // Otherwise, we have to load the partition ourselves
        try {
          logInfo(s"Partition $key not found, computing it")
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)

          // If the task is running locally, do not persist the result
          if (context.isRunningLocally) {
            return computedValues
          }

          // Otherwise, cache the values and keep track of any updates in block statuses
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          val metrics = context.taskMetrics
          val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
          metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }
// val computedValues = rdd.computeOrReadCheckpoint(partition, context)

/*================================================================== */
/* rdd/RDD.scala */
  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointed) firstParent[T].iterator(split, context) else compute(split, context)
  }


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

/*================================================================== */
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
    if (stopped) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
      resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }

  /**
   * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
   * has completed (therefore the RDD has been materialized and potentially stored in memory).
   * doCheckpoint() is called recursively on the parent RDDs.
   */
  private[spark] def doCheckpoint() {
    if (!doCheckpointCalled) {
      doCheckpointCalled = true
      if (checkpointData.isDefined) {
        checkpointData.get.doCheckpoint()
      } else {
        dependencies.foreach(_.rdd.doCheckpoint())
      }
    }
  }

/*================================================================== */
/* rdd/RDDCheckpointData.scala */
  // Do the checkpointing of the RDD. Called after the first job using that RDD is over.
  def doCheckpoint() {
    // If it is marked for checkpointing AND checkpointing is not already in progress,
    // then set it to be in progress, else return
    RDDCheckpointData.synchronized {
      if (cpState == MarkedForCheckpoint) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }

    // Create the output path for the checkpoint
    val path = new Path(rdd.context.checkpointDir.get, "rdd-" + rdd.id)
    val fs = path.getFileSystem(rdd.context.hadoopConfiguration)
    if (!fs.mkdirs(path)) {
      throw new SparkException("Failed to create checkpoint path " + path)
    }

    // Save to file, and reload it as an RDD
    val broadcastedConf = rdd.context.broadcast(
      new SerializableWritable(rdd.context.hadoopConfiguration))
    rdd.context.runJob(rdd, CheckpointRDD.writeToFile[T](path.toString, broadcastedConf) _)
    val newRDD = new CheckpointRDD[T](rdd.context, path.toString)
    if (newRDD.partitions.size != rdd.partitions.size) {
      throw new SparkException(
        "Checkpoint RDD " + newRDD + "(" + newRDD.partitions.size + ") has different " +
          "number of partitions than original RDD " + rdd + "(" + rdd.partitions.size + ")")
    }

    // Change the dependencies and partitions of the RDD
    RDDCheckpointData.synchronized {
      cpFile = Some(path.toString)
      cpRDD = Some(newRDD)
      rdd.markCheckpointed(newRDD)   // Update the RDD's dependencies and partitions
      cpState = Checkpointed
    }
    logInfo("Done checkpointing RDD " + rdd.id + " to " + path + ", new parent is RDD " + newRDD.id)
  }

/**
 * Spark checkpoints RDD after the first job using that RDD is over.
 * But why just skip repartition?
 * Shouldn't it be skipping all stages?
 * Let's try to run and see the log where it skips then try to pin-point at which function Spark skips stages.
 */

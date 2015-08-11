/* spark-1.4.0 */


ExternalAppendOnlyMap.insertAll()
    Spillable.maybeSpill()
        ShuffleMemoryManager.tryToAcquire()
        ExternalAppendOnlyMap.spill()
        _memoryBytesSpilled += currentMemory

ExternalSorter.insertAll()
    Spillable.maybeSpill()
        ShuffleMemoryManager.tryToAcquire()
        ExternalSorter.spill()
        _memoryBytesSpilled += currentMemory


Aggregator.combineValuesByKey()
    ExternalAppendOnlyMap.insertAll()
    TaskMetrics.incMemoryBytesSpilled(ExternalAppendOnlyMap.memoryBytesSpilled)

Aggregator.combineCombinersByKey()
    ExternalAppendOnlyMap.insertAll()
    TaskMetrics.incMemoryBytesSpilled(ExternalAppendOnlyMap.memoryBytesSpilled)

CoGroupedRDD.compute()
    ExternalAppendOnlyMap.insertAll()
    TaskMetrics.incMemoryBytesSpilled(ExternalAppendOnlyMap.memoryBytesSpilled)
    
HashShuffleReader.read()
    ExternalSorter.insertAll()
    TaskMetrics.incMemoryBytesSpilled(ExternalSorter.memoryBytesSpilled)


/* ====================================================================================== */
/* util/collection/Spillable.scala */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)
      myMemoryThreshold += granted
      if (myMemoryThreshold <= currentMemory) {
        // We were granted too little memory to grow further (either tryToAcquire returned 0,
        // or we already had more memory than myMemoryThreshold); spill the current collection
        _spillCount += 1
        logSpillage(currentMemory)

        spill(collection)

        _elementsRead = 0
        // Keep track of spills, and release memory
        _memoryBytesSpilled += currentMemory
        releaseMemoryForThisThread()
        return true
      }
    }
    false
  }


/* ========================================================================================== */
/* Aggregator.scala */
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      // <-----------------------------------------------------------------------------------------
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)  // <--------------------
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)  // <------------------------
      }
      combiners.iterator
    }
  }

  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
    : Iterator[(K, C)] =
  {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeCombiners(oldValue, kc._2) else kc._2
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)  // <---------------------
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }


/* ====================================================================================== */
/* rdd/CoGroupedRDD.scala */
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true)
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of shuffle
        val it = SparkEnv.get.shuffleManager
          .getReader(shuffleDependency.shuffleHandle, split.index, split.index + 1, context)
          .read()
        rddIterators += ((it, depNum))
    }

    if (!externalSorting) {
      val map = new AppendOnlyMap[K, CoGroupCombiner]
      val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
        if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup)
      }
      val getCombiner: K => CoGroupCombiner = key => {
        map.changeValue(key, update)
      }
      rddIterators.foreach { case (it, depNum) =>
        while (it.hasNext) {
          val kv = it.next()
          getCombiner(kv._1)(depNum) += kv._2
        }
      }
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    } else {
      val map = createExternalMap(numRdds)
      for ((it, depNum) <- rddIterators) {
        map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
      }
      context.taskMetrics.incMemoryBytesSpilled(map.memoryBytesSpilled)  // <------------------
      context.taskMetrics.incDiskBytesSpilled(map.diskBytesSpilled)
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    }
  }


/* ====================================================================================== */
/* shuffle/hash/HashShuffleReader.scala */
  override def read(): Iterator[Product2[K, C]] = {
    val ser = Serializer.getSerializer(dep.serializer)
    val iter = BlockStoreShuffleFetcher.fetch(handle.shuffleId, startPartition, context, ser)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        new InterruptibleIterator(context, dep.aggregator.get.combineCombinersByKey(iter, context))
      } else {
        new InterruptibleIterator(context, dep.aggregator.get.combineValuesByKey(iter, context))
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")

      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)  // <------------------------------------------------------
        context.taskMetrics.incMemoryBytesSpilled(sorter.memoryBytesSpilled)  // <------------------
        context.taskMetrics.incDiskBytesSpilled(sorter.diskBytesSpilled)
        sorter.iterator
      case None =>
        aggregatedIter
    }
  }

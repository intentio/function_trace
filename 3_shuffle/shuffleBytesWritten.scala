/* spark-1.4.0 */

ExternalAppendOnlyMap.insertAll()
    Spillable.maybeSpill()
        ShuffleMemoryManager.tryToAcquire()
        ExternalAppendOnlyMap.spill()
            ExternalAppendOnlyMap.flush()
                DiskBlockObjectWriter.commitAndClose()
                    ShuffleWriteMetrics.incShuffleBytesWritten()
            ExternalAppendOnlyMap.revertPartialWritesAndClose()
                ShuffleWriteMetrics.decShuffleBytesWritten()

ExternalSorter.insertAll()
    Spillable.maybeSpill()
        ShuffleMemoryManager.tryToAcquire()
        ExternalSorter.spill()
            ExternalSorter.spillToMergeableFile()
                ExternalSorter.flush()
                    DiskBlockObjectWriter.commitAndClose()
                        ShuffleWriteMetrics.incShuffleBytesWritten()
                ExternalSorter.revertPartialWritesAndClose()
                    ShuffleWriteMetrics.decShuffleBytesWritten()


Aggregator.combineValuesByKey()
    ExternalAppendOnlyMap.insertAll()
Aggregator.combineCombinersByKey()
    ExternalAppendOnlyMap.insertAll()
CoGroupedRDD.compute()
    ExternalAppendOnlyMap.insertAll()
HashShuffleReader.read()
    ExternalSorter.insertAll()
SortShuffleWriter.write()
    ExternalSorter.insertAll()


/* ====================================================================================== */
/* util/collection/ExternalAppendOnlyMap.scala */
  /**
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    while (entries.hasNext) {
      curEntry = entries.next()
      if (maybeSpill(currentMap, currentMap.estimateSize())) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()
    }
  }


/* ====================================================================================== */
/* util/collection/ExternalSorter.scala */
  def insertAll(records: Iterator[_ <: Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpillCollection(usingMap = true)
      }
    } else if (bypassMergeSort) {
      // SPARK-4479: Also bypass buffering if merge sort is bypassed to avoid defensive copies
      if (records.hasNext) {
        spillToPartitionFiles(
          WritablePartitionedIterator.fromIterator(records.map { kv =>
            ((getPartition(kv._1), kv._1), kv._2.asInstanceOf[C])
          })
        )
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }


/* ====================================================================================== */
/* util/collection/Spillable.scala */
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


/* ====================================================================================== */
/* shuffle/ShuffleMemoryManager.scala */
  /**
   * Try to acquire up to numBytes memory for the current thread, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each thread has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active threads) before it is forced to spill. This can
   * happen if the number of threads increases but an older thread had a lot of memory already.
   */
  def tryToAcquire(numBytes: Long): Long = synchronized {
    val threadId = Thread.currentThread().getId
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this thread to the threadMemory map just so we can keep an accurate count of the number
    // of active threads, to let other threads ramp down their memory in calls to tryToAcquire
    if (!threadMemory.contains(threadId)) {
      threadMemory(threadId) = 0L
      notifyAll()  // Will later cause waiting threads to wake up and check numThreads again
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // thread would have more than 1 / numActiveThreads of the memory) or we have enough free
    // memory to give it (we always let each thread get at least 1 / (2 * numActiveThreads)).
    while (true) {
      val numActiveThreads = threadMemory.keys.size
      val curMem = threadMemory(threadId)
      val freeMemory = maxMemory - threadMemory.values.sum

      // How much we can grant this thread; don't let it grow to more than 1 / numActiveThreads;
      // don't let it be negative
      val maxToGrant = math.min(numBytes, math.max(0, (maxMemory / numActiveThreads) - curMem))

      if (curMem < maxMemory / (2 * numActiveThreads)) {
        // We want to let each thread get at least 1 / (2 * numActiveThreads) before blocking;
        // if we can't give it this much now, wait for other threads to free up memory
        // (this happens if older threads allocated lots of memory before N grew)
        if (freeMemory >= math.min(maxToGrant, maxMemory / (2 * numActiveThreads) - curMem)) {
          val toGrant = math.min(maxToGrant, freeMemory)
          threadMemory(threadId) += toGrant
          return toGrant
        } else {
          logInfo(s"Thread $threadId waiting for at least 1/2N of shuffle memory pool to be free")
          wait()
        }
      } else {
        // Only give it as much memory as is free, which might be none if it reached 1 / numThreads
        val toGrant = math.min(maxToGrant, freeMemory)
        threadMemory(threadId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

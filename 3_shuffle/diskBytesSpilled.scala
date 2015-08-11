/* spark-1.4.0 */

/* If you check util/collection/ExternalAppendOnlyMap.scala, you can see that
 * diskBytesSpilled variable is directly related to shuffleBytesWritten
 * variable. */

ExternalAppendOnlyMap.insertAll()
    Spillable.maybeSpill()
        ShuffleMemoryManager.tryToAcquire()
        ExternalAppendOnlyMap.spill()
            ExternalAppendOnlyMap.flush()
                DiskBlockObjectWriter.commitAndClose()
                    ShuffleWriteMetrics.incShuffleBytesWritten()
                _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten

ExternalSorter.insertAll()
    Spillable.maybeSpill()
        ShuffleMemoryManager.tryToAcquire()
        ExternalSorter.spill()
            ExternalSorter.spillToMergeableFile()
                ExternalSorter.flush()
                    DiskBlockObjectWriter.commitAndClose()
                        ShuffleWriteMetrics.incShuffleBytesWritten()
                    _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten


Aggregator.combineValuesByKey()
    ExternalAppendOnlyMap.insertAll()
    TaskMetrics.incDiskBytesSpilled(ExternalAppendOnlyMap.diskBytesSpilled)

Aggregator.combineCombinersByKey()
    ExternalAppendOnlyMap.insertAll()
    TaskMetrics.incDiskBytesSpilled(ExternalAppendOnlyMap.diskBytesSpilled)

CoGroupedRDD.compute()
    ExternalAppendOnlyMap.insertAll()
    TaskMetrics.incDiskBytesSpilled(ExternalAppendOnlyMap.diskBytesSpilled)
    
HashShuffleReader.read()
    ExternalSorter.insertAll()
    TaskMetrics.incDiskBytesSpilled(ExternalSorter.diskBytesSpilled)



/* ====================================================================================== */
/* util/collection/ExternalAppendOnlyMap.scala extends Spillable */
  def diskBytesSpilled: Long = _diskBytesSpilled


  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L


  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    curWriteMetrics = new ShuffleWriteMetrics()  // <----------------------------------------------
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten  // <-------------------------------
      batchSizes.append(curWriteMetrics.shuffleBytesWritten)
      objectsWritten = 0
    }
    //....
  }


/* ====================================================================================== */
/* storage/BlockObjectWriter.scala */
  override def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      close()
    }
    finalPosition = file.length()
    // In certain compression codecs, more bytes are written after close() is called
    writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
  }

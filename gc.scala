/* spark-1.4.0 */

/* ============================================================================================ */
/* executor/Executor.scala */

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum
  }


/* ============================================================================================ */
/* storage/BlockManager.scala */

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }


/* ============================================================================================ */
/* ui/ConsoleProgressBar.scala */

  /**
   * Tear down the timer thread.  The timer thread is a GC root, and it retains the entire
   * SparkContext if it's not terminated.
   */
  def stop(): Unit = timer.cancel()


/* ============================================================================================ */
/* ui/ToolTips.scala */

  val GC_TIME =
    """Time that the executor spent paused for Java garbage collection while the task was
       running."""


/* ============================================================================================ */
/* shuffle/unsafe/UnsafeShuffleManager.scala */

/**
 * A shuffle implementation that uses directly-managed memory to implement several performance
 * optimizations for certain types of shuffles. In cases where the new performance optimizations
 * cannot be applied, this shuffle manager delegates to [[SortShuffleManager]] to handle those
 * shuffles.
 *
 * UnsafeShuffleManager's optimizations will apply when _all_ of the following conditions hold:
 *
 *  - The shuffle dependency specifies no aggregation or output ordering.
 *  - The shuffle serializer supports relocation of serialized values (this is currently supported
 *    by KryoSerializer and Spark SQL's custom serializers).
 *  - The shuffle produces fewer than 16777216 output partitions.
 *  - No individual record is larger than 128 MB when serialized.
 *
 * In addition, extra spill-merging optimizations are automatically applied when the shuffle
 * compression codec supports concatenation of serialized streams. This is currently supported by
 * Spark's LZF serializer.
 *
 * At a high-level, UnsafeShuffleManager's design is similar to Spark's existing SortShuffleManager.
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can are spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * UnsafeShuffleManager optimizes this process in several ways:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[UnsafeShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on UnsafeShuffleManager's design, see SPARK-7081.
 */

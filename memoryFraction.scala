/* SparkConf.scala */
    // Validate memory fractions
    val memoryKeys = Seq(
      "spark.storage.memoryFraction",
      "spark.shuffle.memoryFraction",
      "spark.shuffle.safetyFraction",
      "spark.storage.unrollFraction",
      "spark.storage.safetyFraction")
    for (key <- memoryKeys) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException("$key should be between 0 and 1 (was '$value').")
      }
    }

/* storage/BlockManager.scala */
  /** Return the total amount of storage memory available. */
  private def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

/* shuffle/ShuffleMemoryManager.scala */
private object ShuffleMemoryManager {
  /**
   * Figure out the shuffle memory limit from a SparkConf. We currently have both a fraction
   * of the memory pool and a safety factor since collections can sometimes grow bigger than
   * the size we target before we estimate their sizes again.
   */
  def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}

/* storage/MemoryStore.scala */
  /**
   * The amount of space ensured for unrolling values in memory, shared across all cores.
   * This space is not reserved in advance, but allocated dynamically by dropping existing blocks.
   */
  private val maxUnrollMemory: Long = {
    val unrollFraction = conf.getDouble("spark.storage.unrollFraction", 0.2)
    (maxMemory * unrollFraction).toLong
  }

/* util/collection/ExternalAppendOnlyMap.scala */
/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 *
 * Two parameters control the memory threshold:
 *
 *   `spark.shuffle.memoryFraction` specifies the collective amount of memory used for storing
 *   these maps as a fraction of the executor's total memory. Since each concurrently running
 *   task maintains one map, the actual threshold for each map is this quantity divided by the
 *   number of running tasks.
 *
 *   `spark.shuffle.safetyFraction` specifies an additional margin of safety as a fraction of
 *   this threshold, in case map size estimation is not sufficiently accurate.
 */

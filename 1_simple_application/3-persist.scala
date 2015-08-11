/* Spark-1.4.0 */

/* Spark Application Source Code */
val rdd = sc.textFile("sample.txt")  // HadoopRDD => MapPartitionsRDD (storageLevel=NONE)
    .map(name => name.reverse)  // MapPartitionsRDD => MapPartitionsRDD (storageLevel=NONE)
rdd.persist(StorageLevel.MEMORY_ONLY)  // MapPartitionsRDD (storageLevel=MEMORY_ONLY)
rdd.collect()


/* =============================================================================================== */
/* rdd/RDD.scala
 * Since MapPartitionsRDD.scala does not have persist() method, it uses persist() in RDD.scala.
 */
/**
 * Set this RDD's storage level to persist its values across operations after the first time
 * it is computed. This can only be used to assign a new storage level if the RDD does not
 * have a storage level set yet..
 */
def persist(newLevel: StorageLevel): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel) {
        throw new UnsupportedOperationException(
            "Cannot change storage level of an RDD after it was already assigned a level")
    }
    sc.persistRDD(this)
    // Register the RDD with the ContextCleaner for automatic GC-based cleanup
    sc.cleaner.foreach(_.registerRDDForCleanup(this))
    storageLevel = newLevel
    this
}

/* =============================================================================================== */
/* SparkContext.scala */
/**
 * Register an RDD to be persisted in memory and/or disk storage
 */
private[spark] def persistRDD(rdd: RDD[_]) {
    _executorAllocationManager.foreach { _ =>
        logWarning(
            s"Dynamic allocation currently does not support cached RDDs. Cached data for RDD " +
            s"${rdd.id} will be lost when executors are removed.")
    }
    persistentRdds(rdd.id) = rdd
}

// Keeps track of all persisted RDDs
private[spark] val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]

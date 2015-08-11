/* Spark-1.4.0 */

/* Spark Application Source Code */
val rdd = sc.textFile("sample.txt")  // HadoopRDD => MapPartitionsRDD
    .map(name => name.reverse)  // MapPartitionsRDD => MapPartitionsRDD
rdd.persist(StorageLevel.MEMORY_ONLY)
rdd.collect()


/* =============================================================================================== */
/* rdd/RDD.scala
 * Since HadoopRDD.scala does not have map() method, it uses map() in RDD.scala.
 */

/**
 * Return a new RDD by applying a function to all elements of this RDD.
 */
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
}

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

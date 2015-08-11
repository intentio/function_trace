/* Spark-1.4.0 */

/* [ Action Functions ]
 * All action functions end up calling SparkContext.runJob method.
 */
reduce(func) => sc.runJob
collect() => sc.runJob
count() => sc.runJob
first() => take(1) => sc.runJob
take(n) => sc.runJob
takeSample(withReplacement, num, [seed]) => take(num) => sc.runJob
takeOrdered(n, [ordering]) => reduce => sc.runJob
saveAsTextFile(path) => saveAsHadoopFile => self.context.runJob()
saveAsSequenceFile(path) => saveAsHadoopFile => self.context.runJob()
saveAsObjectFile(path) => saveAsSequenceFile => saveAsHadoopFile => self.context.runJob()
countByKey() => collect() => sc.runJob
foreach(func) => sc.runJob

/* Spark Application Source Code */
val rdd = sc.textFile("sample.txt")  // HadoopRDD => MapPartitionsRDD (storageLevel=NONE)
    .map(name => name.reverse)  // MapPartitionsRDD => MapPartitionsRDD (storageLevel=NONE)
rdd.persist(StorageLevel.MEMORY_ONLY)  // MapPartitionsRDD (storageLevel=MEMORY_ONLY)
rdd.collect()


/* =============================================================================================== */
/* rdd/RDD.scala */
def collect(): Array[T] = withScope {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
}

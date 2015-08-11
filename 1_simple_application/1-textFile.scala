/* Spark-1.4.0 */

/* Spark Application Source Code */
val rdd = sc.textFile("sample.txt")  // HadoopRDD => MapPartitionsRDD
    .map(name => name.reverse)
rdd.persist(StorageLevel.MEMORY_ONLY)
rdd.collect()


/* =============================================================================================== */
/* SparkContext.scala */
/**
 * Read a text file from HDFS, a local file system (available on all nodes), or any
 * Hadoop-supported file system URI, and return it as an RDD of Strings.
 */
def textFile(
    path: String,
    minPartitions: Int = defaultMinPartitions
    ): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(
        path,
        classOf[TextInputFormat], 
        classOf[LongWritable], 
        classOf[Text],
        minPartitions
    ).map(pair => pair._2.toString)
}

/** Get an RDD for a Hadoop file with an arbitrary InputFormat
 *
 * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
 * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
 * operation will create many references to the same object.
 * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
 * copy them using a `map` function.
 */
def hadoopFile[K, V](
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = defaultMinPartitions
    ): RDD[(K, V)] = withScope {
    assertNotStopped()
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableWritable(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
        this,
        confBroadcast,
        Some(setInputPathsFunc),
        inputFormatClass,
        keyClass,
        valueClass,
        minPartitions
    ).setName(path)
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * Note: Instantiating this class directly is not recommended, please use
 * [[org.apache.spark.SparkContext.hadoopRDD()]]
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *     variabe references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *     Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
 *     creates.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
 */
@DeveloperApi
class HadoopRDD[K, V](
    @transient sc: SparkContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int) extends RDD[(K, V)](sc, Nil) with Logging {
    // ...
}

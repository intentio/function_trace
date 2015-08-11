(4) MapPartitionsRDD[2] at map at Conf.scala:41 [Memory Deserialized 1x Replicated]
 |       CachedPartitions: 4; MemorySize: 22.9 MB; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
 |  MapPartitionsRDD[1] at textFile at Conf.scala:40 [Memory Deserialized 1x Replicated]
 |  /home/intentio/Code/scala/spark/local/storage_level/input/names_1.txt HadoopRDD[0] at textFile at Conf.scala:40 [Memory Deserialized 1x Replicated]

$rdd
- MapPartitionsRDD[2] at map at Conf.scala:42
- MapPartitionsRDD[1] at textFile at Conf.scala:41
- /home/intentio/Code/scala/spark/local/storage_level/input/names_1.txt HadoopRDD[0] at textFile at Conf.scala:41

[$persistence]
- [Memory Deserialized 1x Replicated]
- [Memory Deserialized 1x Replicated]
- [Memory Deserialized 1x Replicated]

storageInfo (Array[String])
- CachedPartitions: 4; MemorySize: 22.9 MB; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B


/* spark-1.4.0 */

/*==================================================================================================*/
/* rdd/RDD.scala */
  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    def debugSelf(rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
      val storageInfo = rdd.context.getRDDStorageInfo.filter(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          bytesToString(info.externalBlockStoreSize), bytesToString(info.diskSize)))

      s"$rdd [$persistence]" +: storageInfo
    }
    /* ... */
  }

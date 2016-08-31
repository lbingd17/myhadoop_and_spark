 Spark源码系列（六）Shuffle的过程解析

Spark大会上，所有的演讲嘉宾都认为shuffle是最影响性能的地方，但是又无可奈何。之前去百度面试hadoop的时候，也被问到了这个问题，直接回答了不知道。

这篇文章主要是沿着下面几个问题来开展：

1、shuffle过程的划分？

2、shuffle的中间结果如何存储？

3、shuffle的数据如何拉取过来？
Shuffle过程的划分

Spark的操作模型是基于RDD的，当调用RDD的reduceByKey、groupByKey等类似的操作的时候，就需要有shuffle了。再拿出reduceByKey这个来讲。

  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

reduceByKey的时候，我们可以手动设定reduce的个数，如果不指定的话，就可能不受控制了。
复制代码

  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    for (r <- bySize if r.partitioner.isDefined) {
      return r.partitioner.get
    }
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(bySize.head.partitions.size)
    }
  }

复制代码

如果不指定reduce个数的话，就按默认的走：

1、如果自定义了分区函数partitioner的话，就按你的分区函数来走。

2、如果没有定义，那么如果设置了spark.default.parallelism，就使用哈希的分区方式，reduce个数就是设置的这个值。

3、如果这个也没设置，那就按照输入数据的分片的数量来设定。如果是hadoop的输入数据的话，这个就多了。。。大家可要小心啊。

设定完之后，它会做三件事情，也就是之前讲的3次RDD转换。
View Code

1、在第一个MapPartitionsRDD这里先做一次map端的聚合操作。

2、ShuffledRDD主要是做从这个抓取数据的工作。

3、第二个MapPartitionsRDD把抓取过来的数据再次进行聚合操作。

4、步骤1和步骤3都会涉及到spill的过程。

怎么做的聚合操作，回去看RDD那章。
Shuffle的中间结果如何存储

作业提交的时候，DAGScheduler会把Shuffle的过程切分成map和reduce两个Stage（之前一直被我叫做shuffle前和shuffle后），具体的切分的位置在上图的虚线处。

map端的任务会作为一个ShuffleMapTask提交，最后在TaskRunner里面调用了它的runTask方法。
复制代码

  override def runTask(context: TaskContext): MapStatus = {
    val numOutputSplits = dep.partitioner.numPartitions
    metrics = Some(context.taskMetrics)

    val blockManager = SparkEnv.get.blockManager
    val shuffleBlockManager = blockManager.shuffleBlockManager
    var shuffle: ShuffleWriterGroup = null
    var success = false

    try {
      // serializer为空的情况调用默认的JavaSerializer，也可以通过spark.serializer来设置成别的
      val ser = Serializer.getSerializer(dep.serializer)
      // 实例化Writer，Writer的数量=numOutputSplits=前面我们说的那个reduce的数量
      shuffle = shuffleBlockManager.forMapTask(dep.shuffleId, partitionId, numOutputSplits, ser)

      // 遍历rdd的元素，按照key计算出来它所在的bucketId，然后通过bucketId找到相应的Writer写入
      for (elem <- rdd.iterator(split, context)) {
        val pair = elem.asInstanceOf[Product2[Any, Any]]
        val bucketId = dep.partitioner.getPartition(pair._1)
        shuffle.writers(bucketId).write(pair)
      }

      // 提交写入操作. 计算每个bucket block的大小
      var totalBytes = 0L
      var totalTime = 0L
      val compressedSizes: Array[Byte] = shuffle.writers.map { writer: BlockObjectWriter =>
        writer.commit()
        writer.close()
        val size = writer.fileSegment().length
        totalBytes += size
        totalTime += writer.timeWriting()
        MapOutputTracker.compressSize(size)
      }

      // 更新 shuffle 监控参数.
      val shuffleMetrics = new ShuffleWriteMetrics
      shuffleMetrics.shuffleBytesWritten = totalBytes
      shuffleMetrics.shuffleWriteTime = totalTime
      metrics.get.shuffleWriteMetrics = Some(shuffleMetrics)

      success = true
      new MapStatus(blockManager.blockManagerId, compressedSizes)
    } catch { case e: Exception =>
      // 出错了，取消之前的操作，关闭writer
      if (shuffle != null && shuffle.writers != null) {
        for (writer <- shuffle.writers) {
          writer.revertPartialWrites()
          writer.close()
        }
      }
      throw e
    } finally {
      // 关闭writer
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
      // 执行注册的回调函数，一般是做清理工作
      context.executeOnCompleteCallbacks()
    }
  }

复制代码

遍历每一个记录，通过它的key来确定它的bucketId，再通过这个bucket的writer写入数据。

下面我们看看ShuffleBlockManager的forMapTask方法吧。
复制代码

def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer) = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
      private val shuffleState = shuffleStates(shuffleId)
      private var fileGroup: ShuffleFileGroup = null

      val writers: Array[BlockObjectWriter] = if (consolidateShuffleFiles) {
        fileGroup = getUnusedFileGroup()
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
　　　　　　// 从已有的文件组里选文件，一个bucket一个文件，即要发送到同一个reduce的数据写入到同一个文件
          blockManager.getDiskWriter(blockId, fileGroup(bucketId), serializer, bufferSize)
        }
      } else {
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          // 按照blockId来生成文件，文件数为map数*reduce数
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          val blockFile = blockManager.diskBlockManager.getFile(blockId)
          if (blockFile.exists) {
            if (blockFile.delete()) {
              logInfo(s"Removed existing shuffle file $blockFile")
            } else {
              logWarning(s"Failed to remove existing shuffle file $blockFile")
            }
          }
          blockManager.getDiskWriter(blockId, blockFile, serializer, bufferSize)
        }
      }

复制代码

1、map的中间结果是写入到本地硬盘的，而不是内存。

2、默认是一个Executor的中间结果文件是M*R（M=map数量，R=reduce的数量），设置了spark.shuffle.consolidateFiles为true之后是R个文件，根据bucketId把要分到同一个reduce的结果写入到一个文件中。

3、consolidateFiles采用的是一个reduce一个文件，它还记录了每个map的写入起始位置，所以查找的时候先通过reduceId查找到哪个文件，再通过mapId查找索引当中的起始位置offset，长度length=（mapId + 1）.offset -（mapId）.offset，这样就可以确定一个FileSegment(file, offset, length)。

4、Finally，存储结束之后， 返回了一个new MapStatus(blockManager.blockManagerId, compressedSizes)，把blockManagerId和block的大小都一起返回。

个人想法，shuffle这块和hadoop的机制差别不大，tez这样的引擎会赶上spark的速度呢？还是让我们拭目以待吧！
Shuffle的数据如何拉取过来

ShuffleMapTask结束之后，最后走到DAGScheduler的handleTaskCompletion方法当中（关于中间的过程，请看《图解作业生命周期》）。
View Code

1、把结果添加到Stage的outputLocs数组里，它是按照数据的分区Id来存储映射关系的partitionId->MapStaus。

2、stage结束之后，通过mapOutputTracker的registerMapOutputs方法，把此次shuffle的结果outputLocs记录到mapOutputTracker里面。

这个stage结束之后，就到ShuffleRDD运行了，我们看一下它的compute函数。

SparkEnv.get.shuffleFetcher.fetch[P](shuffledId, split.index, context, ser)

它是通过ShuffleFetch的fetch方法来抓取的，具体实现在BlockStoreShuffleFetcher里面。
复制代码

  override def fetch[T](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer)
    : Iterator[T] =
{
    val blockManager = SparkEnv.get.blockManager
    val startTime = System.currentTimeMillis
　　 // mapOutputTracker也分Master和Worker，Worker向Master请求获取reduce相关的MapStatus，主要是（BlockManagerId和size）
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    // 一个BlockManagerId对应多个文件的大小
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }
    // 构造BlockManagerId 和 BlockId的映射关系，想不到ShffleBlockId的mapId，居然是1,2,3,4的序列...
    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2)))
    }
    // 名为updateBlock，实际是检验函数，每个Block都对应着一个Iterator接口，如果该接口为空，则应该报错
    def unpackBlock(blockPair: (BlockId, Option[Iterator[Any]])) : Iterator[T] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[T]]
        }
        case None => {
          blockId match {
            case ShuffleBlockId(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, null)
            case _ =>
              throw new SparkException("Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }
    // 从blockManager获取reduce所需要的全部block，并添加校验函数
    val blockFetcherItr = blockManager.getMultiple(blocksByAddress, serializer)
    val itr = blockFetcherItr.flatMap(unpackBlock)
    
　　val completionIter = CompletionIterator[T, Iterator[T]](itr, {
      // CompelteIterator迭代结束之后，会执行以下这部分代码，提交它记录的各种参数
      val shuffleMetrics = new ShuffleReadMetrics
      shuffleMetrics.shuffleFinishTime = System.currentTimeMillis
      shuffleMetrics.fetchWaitTime = blockFetcherItr.fetchWaitTime
      shuffleMetrics.remoteBytesRead = blockFetcherItr.remoteBytesRead
      shuffleMetrics.totalBlocksFetched = blockFetcherItr.totalBlocks
      shuffleMetrics.localBlocksFetched = blockFetcherItr.numLocalBlocks
      shuffleMetrics.remoteBlocksFetched = blockFetcherItr.numRemoteBlocks
      context.taskMetrics.shuffleReadMetrics = Some(shuffleMetrics)
    })

    new InterruptibleIterator[T](context, completionIter)
  }
}

复制代码

1、MapOutputTrackerWorker向MapOutputTrackerMaster获取shuffle相关的map结果信息。

2、把map结果信息构造成BlockManagerId --> Array(BlockId, size)的映射关系。

3、通过BlockManager的getMultiple批量拉取block。

4、返回一个可遍历的Iterator接口，并更新相关的监控参数。

我们继续看getMultiple方法。
复制代码

  def getMultiple(
      blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
      serializer: Serializer): BlockFetcherIterator = {
    val iter =
      if (conf.getBoolean("spark.shuffle.use.netty", false)) {
        new BlockFetcherIterator.NettyBlockFetcherIterator(this, blocksByAddress, serializer)
      } else {
        new BlockFetcherIterator.BasicBlockFetcherIterator(this, blocksByAddress, serializer)
      }

    iter.initialize()
    iter
  }

复制代码

分两种情况处理，分别是netty的和Basic的，Basic的就不讲了，就是通过ConnectionManager去指定的BlockManager那里获取数据，上一章刚好说了。

我们讲一下Netty的吧，这个是需要设置的才能启用的，不知道性能会不会好一些呢？

看NettyBlockFetcherIterator的initialize方法，再看BasicBlockFetcherIterator的initialize方法，发现Basic的不能同时抓取超过48Mb的数据。
复制代码

    override def initialize() {
      // 分开本地请求和远程请求，返回远程的FetchRequest
      val remoteRequests = splitLocalRemoteBlocks()
      // 抓取顺序随机
      for (request <- Utils.randomize(remoteRequests)) {
        fetchRequestsSync.put(request)
      }
      // 默认是开6个线程去进行抓取
      copiers = startCopiers(conf.getInt("spark.shuffle.copier.threads", 6))// 读取本地的block
      getLocalBlocks()
   }

复制代码

在NettyBlockFetcherIterator的sendRequest方法里面，发现它是通过ShuffleCopier来试下的。

　　val cpier = new ShuffleCopier(blockManager.conf)
   cpier.getBlocks(cmId, req.blocks, putResult)

这块接下来就是netty的客户端调用的方法了，我对这个不了解。在服务端的处理是在DiskBlockManager内部启动了一个ShuffleSender的服务，最终的业务处理逻辑是在FileServerHandler。

它是通过getBlockLocation返回一个FileSegment，下面这段代码是ShuffleBlockManager的getBlockLocation方法。
复制代码

  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    // Search all file groups associated with this shuffle.
    val shuffleState = shuffleStates(id.shuffleId)
    for (fileGroup <- shuffleState.allFileGroups) {
      val segment = fileGroup.getFileSegmentFor(id.mapId, id.reduceId)
      if (segment.isDefined) { return segment.get }
    }
    throw new IllegalStateException("Failed to find shuffle block: " + id)
  }

复制代码

先通过shuffleId找到ShuffleState，再通过reduceId找到文件，最后通过mapId确定它的文件分片的位置。但是这里有个疑问了，如果启用了consolidateFiles，一个reduce的所需数据都在一个文件里，是不是就可以把整个文件一起返回呢，而不是通过N个map来多次读取？还是害怕一次发送一个大文件容易失败？这就不得而知了。

到这里整个过程就讲完了。可以看得出来Shuffle这块还是做了一些优化的，但是这些参数并没有启用，有需要的朋友可以自己启用一下试试效果。

 

 
Referemce:
岑玉海 http://www.cnblogs.com/cenyuhai/p/3826227.html

转载请注明出处，谢谢！

 ／／／／
  
 首先每一个Mapper会根据Reducer的数量创建出相应的bucket，bucket的数量是M×R，其中M是Map的个数，R是Reduce的个数。
 
其次Mapper产生的结果会根据设置的partition算法填充到每个bucket中去。这里的partition算法是可以自定义的，当然默认的算法是根据key哈希到不同的bucket中去。

当Reducer启动时，它会根据自己task的id和所依赖的Mapper的id从远端或是本地的block manager中取得相应的bucket作为Reducer的输入进行处理。

文／JasonDing（简书作者）
原文链接：http://www.jianshu.com/p/60bab35bc01e
著作权归作者所有，转载请联系作者获得授权，并标注“简书作者”。
http://www.jianshu.com/p/60bab35bc01e

／／／／／
 Spark源码系列（六）Shuffle的过程解析

Spark大会上，所有的演讲嘉宾都认为shuffle是最影响性能的地方，但是又无可奈何。之前去百度面试hadoop的时候，也被问到了这个问题，直接回答了不知道。

这篇文章主要是沿着下面几个问题来开展：

1、shuffle过程的划分？

2、shuffle的中间结果如何存储？

3、shuffle的数据如何拉取过来？

一、Shuffle过程的划分

Spark的操作模型是基于RDD的，当调用RDD的reduceByKey、groupByKey等类似的操作的时候，就需要有shuffle了。再拿出reduceByKey这个来讲 
reduceByKey的时候，我们可以手动设定reduce的个数，如果不指定的话，就可能不受控制了。 

如果不指定reduce个数的话，就按默认的走：

1、如果自定义了分区函数partitioner的话，就按你的分区函数来走。

2、如果没有定义，那么如果设置了spark.default.parallelism，就使用哈希的分区方式，reduce个数就是设置的这个值。

3、如果这个也没设置，那就按照输入数据的分片的数量来设定。如果是hadoop的输入数据的话，这个就多了。。。大家可要小心啊。

设定完之后，它会做三件事情，也就是之前讲的3次RDD转换。

-------    --------------     --------------   -------------- 
pairRDD -> MapPartitionsRDD -> ShuffledRDD -> MapPartitionsRDD
-------     --------------     --------------   -------------- 

1、在第一个MapPartitionsRDD这里先做一次map端的聚合操作。

2、ShuffledRDD主要是做从这个抓取数据的工作。

3、第二个MapPartitionsRDD把抓取过来的数据再次进行聚合操作。

4、步骤1和步骤3都会涉及到spill的过程。

二、Shuffle的中间结果如何存储

作业提交的时候，DAGScheduler会把Shuffle的过程切分成map和reduce两个Stage（之前一直被我叫做shuffle前和shuffle后），具体的切分的位置在上图的虚线处。

map端的任务会作为一个ShuffleMapTask提交，最后在TaskRunner里面调用了它的runTask方法。
遍历每一个记录，通过它的key来确定它的bucketId，再通过这个bucket的writer写入数据。

下面我们看看ShuffleBlockManager的forMapTask方法吧。 

1、map的中间结果是写入到本地硬盘的，而不是内存。

2、默认是一个Executor的中间结果文件是M*R（M=map数量，R=reduce的数量），设置了spark.shuffle.consolidateFiles为true之后是R个文件，根据bucketId把要分到同一个reduce的结果写入到一个文件中。

3、consolidateFiles采用的是一个reduce一个文件，它还记录了每个map的写入起始位置，所以查找的时候先通过reduceId查找到哪个文件，再通过mapId查找索引当中的起始位置offset，长度length=（mapId + 1）.offset -（mapId）.offset，这样就可以确定一个FileSegment(file, offset, length)。

4、Finally，存储结束之后， 返回了一个new MapStatus(blockManager.blockManagerId, compressedSizes)，把blockManagerId和block的大小都一起返回。

个人想法，shuffle这块和hadoop的机制差别不大，tez这样的引擎会赶上spark的速度呢？还是让我们拭目以待吧！

三、Shuffle的数据如何拉取过来

ShuffleMapTask结束之后，最后走到DAGScheduler的handleTaskCompletion方法当中（关于中间的过程，请看《图解作业生命周期》）。
 
1、把结果添加到Stage的outputLocs数组里，它是按照数据的分区Id来存储映射关系的partitionId->MapStaus。

2、stage结束之后，通过mapOutputTracker的registerMapOutputs方法，把此次shuffle的结果outputLocs记录到mapOutputTracker里面。

这个stage结束之后，就到ShuffleRDD运行了，我们看一下它的compute函数。

SparkEnv.get.shuffleFetcher.fetch[P](shuffledId, split.index, context, ser)

它是通过ShuffleFetch的fetch方法来抓取的，具体实现在BlockStoreShuffleFetcher里面。
 

1、MapOutputTrackerWorker向MapOutputTrackerMaster获取shuffle相关的map结果信息。

2、把map结果信息构造成BlockManagerId --> Array(BlockId, size)的映射关系。

3、通过BlockManager的getMultiple批量拉取block。

4、返回一个可遍历的Iterator接口，并更新相关的监控参数。

我们继续看getMultiple方法。
 
分两种情况处理，分别是netty的和Basic的，Basic的就不讲了，就是通过ConnectionManager去指定的BlockManager那里获取数据，上一章刚好说了。

我们讲一下Netty的吧，这个是需要设置的才能启用的，不知道性能会不会好一些呢？

看NettyBlockFetcherIterator的initialize方法，再看BasicBlockFetcherIterator的initialize方法，发现Basic的不能同时抓取超过48Mb的数据。
 

在NettyBlockFetcherIterator的sendRequest方法里面，发现它是通过ShuffleCopier来试下的。

　　val cpier = new ShuffleCopier(blockManager.conf)
   cpier.getBlocks(cmId, req.blocks, putResult)

这块接下来就是netty的客户端调用的方法了，我对这个不了解。在服务端的处理是在DiskBlockManager内部启动了一个ShuffleSender的服务，最终的业务处理逻辑是在FileServerHandler。

它是通过getBlockLocation返回一个FileSegment，下面这段代码是ShuffleBlockManager的getBlockLocation方法。
 

先通过shuffleId找到ShuffleState，再通过reduceId找到文件，最后通过mapId确定它的文件分片的位置。但是这里有个疑问了，如果启用了consolidateFiles，一个reduce的所需数据都在一个文件里，是不是就可以把整个文件一起返回呢，而不是通过N个map来多次读取？还是害怕一次发送一个大文件容易失败？这就不得而知了。

到这里整个过程就讲完了。可以看得出来Shuffle这块还是做了一些优化的，但是这些参数并没有启用，有需要的朋友可以自己启用一下试试效果。

 

 
Referemce:
岑玉海 http://www.cnblogs.com/cenyuhai/p/3826227.html

 

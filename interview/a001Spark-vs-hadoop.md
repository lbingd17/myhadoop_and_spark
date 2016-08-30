###1,Spark较之Hadoop有什么优势(扩展:请简要描述一下Hadoop, Spark, MPI三种计
算框架的特点以及分别适用于什么样的场景)
总体而言: 1,Hadoop是离线计算,基于磁盘,每次运算之后的结果需要存储在HDFS里
面,下次再用的话,还需要读出来进行一次计算,磁盘IO开销比较大。底层基于HDFS
存储文件系统。适用于离线数据处理和不需要多次迭代计算的场景,并且Hadoop只有
Map和Reduce两种接口,相对于Spark来说太少了。 2,Spark是内存计算框架,适用于
多次迭代的计算模型,诸如各种机器学习算法 ,Spark里面的一个核心的概念就是
RDD,弹性分布式数据集。Spark支持内存计算模型,用户可以指定存储的策略,当内
存不够的时候,可以放置到磁盘上。并且Spark提供了一组RDD的接口,Tran敏感词
ormations和Action。Tran敏感词ormations是把一个RDD转换成为另一个RDD以便形成
Lineage血统链,这样当数据发生错误的时候可以快速的依靠这种继承关系恢复数据。
Action操作是启动一个Job并开始真正的进行一些计算并把返回的结果可以给Driver或者
是缓存在worker里面。 3,MPI是消息传递接口,可以理解为是更原生的一种分布式模型
就作业而言: 1,Hadoop中的作业,一段编好能运行的程序被称为MapReduce程序,也
就是一个Job,每个Job下还能有若干个task,可区分为MapTask和ReduceTask。

2,在Spark中,作业有很复杂的层级划分
‐ Application:用户在Spark上部署的程序,由集群上的驱动程序和执行器组成 
‐ Task:被发送到一个执行器的工作单元 
‐ Job:一个由多个由Spark action引起的反应task组成的并行计算的单元 
‐ Stage:每个Job可以被划分成多个task的集合,成为stage,它们之间互相关联 

一个Application和一个SparkContext相关联,每个Application中可以有一个或多个Job,
可以并行或者串行运行Job。Spark中的一个Action可以触发一个Job的运行。在Job里面
又包含了多个Stage,Stage是以Shuffle进行划分的。在Stage中又包含了多个Task,多个
Task构成了Task Set。
###2,为什么要在Spark Streaming之前加一个Apache Kafka
#####使用背景:流数据。 活动流数据是几乎所有站点在对其网站使用情况做报表时
都要用到的数据中最常规的部分。活动数据包括页面访问量(Page View)、被查看内
容方面的信息以及搜索情况等内容。这种数据通常的处理方式是先把各种活动以日志
的形式写入某种文件,然后周期性地对这些文件进行统计分析。运营数据指的是服务
器的性能数据(CPU、IO使用率、请求时间、服务日志等等数据)。运营数据的统计方
法种类繁多。
#####Kafka是一种分布式的,基于发布/订阅的消息系统。主要设计目标如下:
以时间复杂度为O(1)的方式提供消息持久化能力,即使对TB级以上数据也能保证常
数时间复杂度的访问性
高吞吐率。即使在非常廉价的商用机器上也能做到单机支持每秒100K条以上消息的
传输
支持Kafka Server间的消息分区,及分布式消费,同时保证每个Partition内的消息顺序

传输
同时支持离线数据处理和实时数据处理
Scale out:支持在线水平扩展
#####使用Kafka的原因:
解耦
在项目启动之初来预测将来项目会碰到什么需求,是极其困难的。消息系统在处理过
程中间插入了一个隐含的、基于数据的接口层,两边的处理过程都要实现这一接口。
这允许你独立的扩展或修改两边的处理过程,只要确保它们遵守同样的接口约束。
冗余
有些情况下,处理数据的过程会失败。除非数据被持久化,否则将造成丢失。消息队
列把数据进行持久化直到它们已经被完全处理,通过这一方式规避了数据丢失风险。
许多消息队列所采用的”插入-获取-删除”范式中,在把一个消息从队列中删除之前,需
要你的处理系统明确的指出该消息已经被处理完毕,从而确保你的数据被安全的保存
直到你使用完毕。
扩展性
因为消息队列解耦了你的处理过程,所以增大消息入队和处理的频率是很容易的,只
要另外增加处理过程即可。不需要改变代码、不需要调节参数。扩展就像调大电力按
钮一样简单。
灵活性 & 峰值处理能力
在访问量剧增的情况下,应用仍然需要继续发挥作用,但是这样的突发流量并不常
见;如果为以能处理这类峰值访问为标准来投入资源随时待命无疑是巨大的浪费。使
用消息队列能够使关键组件顶住突发的访问压力,而不会因为突发的超负荷的请求而
完全崩溃。
可恢复性
系统的一部分组件失效时,不会影响到整个系统。消息队列降低了进程间的耦合度,
所以即使一个处理消息的进程挂掉,加入队列中的消息仍然可以在系统恢复后被处
理。
顺序保证
在大多使用场景下,数据处理的顺序都很重要。大部分消息队列本来就是排序的,并
且能保证数据会按照特定的顺序来处理。Kafka保证一个Partition内的消息的有序性。
缓冲
在任何重要的系统中,都会有需要不同的处理时间的元素。例如,加载一张图片比应
用过滤器花费更少的时间。消息队列通过一个缓冲层来帮助任务最高效率的执行———
写入队列的处理会尽可能的快速。该缓冲有助于控制和优化数据流经过系统的速度。
异步通信
很多时候,用户不想也不需要立即处理消息。消息队列提供了异步处理机制,允许用
户把一个消息放入队列,但并不立即处理它。想向队列中放入多少消息就放多少,然
后在需要的时候再去处理它们。
#####常用Message Queue对比:
RabbitMQ
RabbitMQ是使用Erlang编写的一个开源的消息队列,本身支持很多的协议:AMQP,
XMPP, SMTP, STOMP,也正因如此,它非常重量级,更适合于企业级的开发。同时实
现了Broker构架,这意味着消息在发送给客户端时先在中心队列排队。对路由,负载
均衡或者数据持久化都有很好的支持。
Redis
Redis是一个基于Key-Value对的NoSQL数据库,开发维护很活跃。虽然它是一个Key-
Value数据库存储系统,但它本身支持MQ功能,所以完全可以当做一个轻量级的队列
服务来使用。对于RabbitMQ和Redis的入队和出队操作,各执行100万次,每10万次记
录一次执行时间。测试数据分为128Bytes、512Bytes、1K和10K四个不同大小的数据。
实验表明:入队时,当数据比较小时Redis的性能要高于RabbitMQ,而如果数据大小超
过了10K,Redis则慢的无法忍受;出队时,无论数据大小,Redis都表现出非常好的性
能,而RabbitMQ的出队性能则远低于Redis。
ZeroMQ
ZeroMQ号称最快的消息队列系统,尤其针对大吞吐量的需求场景。ZMQ能够实现
RabbitMQ不擅长的高级/复杂的队列,但是开发人员需要自己组合多种技术框架,技术
上的复杂度是对这MQ能够应用成功的挑战。ZeroMQ具有一个独特的非中间件的模
式,你不需要安装和运行一个消息服务器或中间件,因为你的应用程序将扮演这个服
务器角色。你只需要简单的引用ZeroMQ程序库,可以使用NuGet安装,然后你就可以
愉快的在应用程序之间发送消息了。但是ZeroMQ仅提供非持久性的队列,也就是说如
果宕机,数据将会丢失。其中,Twitter的Storm 0.9.0以前的版本中默认使用ZeroMQ作
为数据流的传输(Storm从0.9版本开始同时支持ZeroMQ和Netty作为传输模块)。
ActiveMQ
ActiveMQ是Apache下的一个子项目。 类似于ZeroMQ,它能够以代理人和点对点的技术
实现队列。同时类似于RabbitMQ,它少量代码就可以高效地实现高级应用场景。
Kafka/Jafka
Kafka是Apache下的一个子项目,是一个高性能跨语言分布式发布/订阅消息队列系统,
而Jafka是在Kafka之上孵化而来的,即Kafka的一个升级版。具有以下特性:快速持久
化,可以在O(1)的系统开销下进行消息持久化;高吞吐,在一台普通的服务器上既可
以达到10W/s的吞吐速率;完全的分布式系统,Broker、Producer、Consumer都原生自
动支持分布式,自动实现负载均衡;支持Hadoop数据并行加载,对于像Hadoop的一样
的日志数据和离线分析系统,但又要求实时处理的限制,这是一个可行的解决方案。
Kafka通过Hadoop的并行加载机制统一了在线和离线的消息处理。Apache Kafka相对于
ActiveMQ是一个非常轻量级的消息系统,除了性能非常好之外,还是一个工作良好的
分布式系统。
###3,简单描述介绍Spark、HDFS、HBase、Kafka等所提及工具的应用场景和优劣之处


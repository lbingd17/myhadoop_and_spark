2. TopK编程实例

TopK程序的任务是对一堆文本进行词频统计，并返回出现频率最高的K个词。如果采用MapReduce实现，则需要编写两个作业：WordCount和TopK，而使用Spark则只需一个作业，其中WordCount部分已由前面实现了，接下来顺着前面的实现，找到Top K个词。注意，本文的实现并不是最优的，有很大改进空间。

步骤1：首先需要对所有词按照词频排序，如下：

    val sorted = result.map {
      case(key, value) => (value, key); //exchange key and value
    }.sortByKey(true, 1)

步骤2：返回前K个：

val topK = sorted.top(args(3).toInt)

步骤3：将K各词打印出来：

topK.foreach(println)

注意，对于应用程序标准输出的内容，YARN将保存到Container的stdout日志中。在YARN中，每个Container存在三个日志文件，分别是stdout、stderr和syslog，前两个保存的是标准输出产生的内容，第三个保存的是log4j打印的日志，通常只有第三个日志中有内容。

本程序完整代码、编译好的jar包和运行脚本可以从这里下载。下载之后，按照“Apache Spark学习：利用Eclipse构建Spark集成开发环境”一文操作流程运行即可。


本文链接地址: http://dongxicheng.org/framework-on-yarn/spark-scala-writing-application/

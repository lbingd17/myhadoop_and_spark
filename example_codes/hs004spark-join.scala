3. SparkJoin编程实例

在推荐领域有一个著名的开放测试集是movielens给的，下载链接是：http://grouplens.org/datasets/movielens/，该测试集包含三个文件，分别是ratings.dat、sers.dat、movies.dat，具体介绍可阅读：README.txt，本节给出的SparkJoin实例则通过连接ratings.dat和movies.dat两个文件得到平均得分超过4.0的电影列表，采用的数据集是：ml-1m。程序代码如下：

import org.apache.spark._
import SparkContext._
object SparkJoin {
  def main(args: Array[String]) {
    if (args.length != 4 ){
      println("usage is org.test.WordCount <master> <rating> <movie> <output>")
      return
    }
    val sc = new SparkContext(args(0), "WordCount",
    System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))

    // Read rating from HDFS file
    val textFile = sc.textFile(args(1))

    //extract (movieid, rating)
    val rating = textFile.map(line => {
        val fileds = line.split("::")
        (fileds(1).toInt, fileds(2).toDouble)
       })

    val movieScores = rating
       .groupByKey()
       .map(data => {
         val avg = data._2.sum / data._2.size
         (data._1, avg)
       })

     // Read movie from HDFS file
     val movies = sc.textFile(args(2))
     val movieskey = movies.map(line => {
       val fileds = line.split("::")
        (fileds(0).toInt, fileds(1))
     }).keyBy(tup => tup._1)

     // by join, we get <movie, averageRating, movieName>
     val result = movieScores
       .keyBy(tup => tup._1)
       .join(movieskey)
       .filter(f => f._2._1._2 > 4.0)
       .map(f => (f._1, f._2._1._2, f._2._2._2))

    result.saveAsTextFile(args(3))
  }
}

你可以从这里下载代码、编译好的jar包和运行脚本。

这个程序直接使用Spark编写有些麻烦，可以直接在Shark上编写HQL实现，Shark是基于Spark的类似Hive的交互式查询引擎，具体可参考：Shark。

4. 总结

Spark 程序设计对Scala语言的要求不高，正如Hadoop程序设计对Java语言要求不高一样，只要掌握了最基本的语法就能编写程序，且常见的语法和表达方式是很少的。通常，刚开始仿照官方实例编写程序，包括Scala、Java和Python三种语言实例。

原创文章，转载请注明： 转载自董的博客

本文链接地址: http://dongxicheng.org/framework-on-yarn/spark-scala-writing-application/

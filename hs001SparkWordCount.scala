import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
//统计字符出现次数
object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    
    //word count example
    val textFile = sc.textFile(args(0))
    textFile.flatMap(_.split(" ")).map((_, )).reduceByKey(_+_).collect().foreach(println) 
    val counts = textFile.flatMap(line => line.split(“ “)).map(word => (word,1)).reduceByKey(+) 
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b) 

    // Estimating Pi
    val count = sc.parallelize(1 to 10000).map{
      i => 
      val x = Math.random() 
      val y = Math.random() 
      if (xx + yy < 1) 
        1 
      else 
        0
    }.reduce( + )
    
    //
    var textFile = sc.textFile("/mnt/jediael/spark‐1.3.1‐bin‐hadoop2.6/README.md") 
    textFile.count() 
    textFile.first();
    val linesWithSpark = textFile.filter(line => line.contains("Spark")) 
    linesWithSpark.count() 
    textFile.map(line => line.split(" ").size)
    .reduce( (a, b) => if (a > b) a   else  b ) 
  
    import java.lang.Math 
    textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))

    linesWithSpark.cache() 


    sc.stop()
  }
}

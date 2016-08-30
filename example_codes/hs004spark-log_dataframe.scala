    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

    var StructFieldArr = new Array[StructField](4)
    StructFieldArr(0) = new StructField("Id", StringType, true)
    StructFieldArr(1) = new StructField("Timestamp", StringType, true)
    StructFieldArr(2) = new StructField("name", StringType, true)
    StructFieldArr(3) = new StructField("age", LongType, true)
  val log_schema = StructType(StructFieldArr)

    val inputFilePath = "myTestInput"
    val log_arraystring_rdd =  sc.textFile(inputFilePath).map(_.split("\t"))
    val log_rowrdd = log_arraystring_rdd.
      map(p => {  
        Row(p(0).toString, p(1).toString,p(2).toString, p(3).toLong )
      }
     )
    val log_df = sqlContext.createDataFrame(log_rowrdd, log_schema)
    log_df.show
/*
scala> log_df.show
+---+---------+--------+---+
| Id|Timestamp|    name|age|
+---+---------+--------+---+
|  1| 20160101|zhangsan| 20|
|  2| 20160102|    lisi| 30|
|  3| 20160103|  wangwu| 40|
+---+---------+--------+---+
*/

//case class

 case class myTupleLogSample(
                               userid : String,
                               timestamp : Long,
                               line_flag : Int
                             )

  val myTupleLog = sc.parallelize( List(("q1", 1,1 ), ("q1", 2,1)
          ,("q2",  1,1), ("q2",2,1)
          ,  ("q2", 1,1), ("q3",12,1) ), 1).
        map(line => {myTupleLogSample(line._1, line._2, line._3) }).toDF()

  myTupleLog.show
 /**
 scala> myTupleLog.show
+------+---------+---------+
|userid|timestamp|line_flag|
+------+---------+---------+
|    q1|        1|        1|
|    q1|        2|        1|
|    q2|        1|        1|
|    q2|        2|        1|
|    q2|        1|        1|
|    q3|       12|        1|
+------+---------+---------+
 **/
                             

  
  
  
  

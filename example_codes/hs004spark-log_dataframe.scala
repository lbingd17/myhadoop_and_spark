/***
 * 包括基本的log预处理以及生成dataframe的过程
 * */
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

/***
 * case class隐式推断数据类型
***/
    case class myTupleLogSchema(
                               userid : String,
                               timestamp : Long,
                               line_flag : Int
                             )

    val myTupleLog = sc.parallelize( List(("q1", 1,1 ), ("q1", 2,1)
          ,("q2",  1,1), ("q2",2,1)
          ,  ("q2", 1,1), ("q3",12,1) ), 1).
        map(line => {myTupleLogSchema(line._1, line._2, line._3) }).toDF()

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
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.functions._
    val df_wholelog3 = myTupleLog
    val df_computed = df_wholelog3.groupBy("userid").
        agg(sum("line_flag") as "userid_line_cnt", max("timestamp") as  "maxts", min("timestamp") as "mints")
/**
scala> df_computed.show
+------+---------------+-----+-----+                                            
|userid|userid_line_cnt|maxts|mints|
+------+---------------+-----+-----+
|    q1|              2|    2|    1|
|    q2|              3|    2|    1|
|    q3|              1|   12|   12|
+------+---------------+-----+-----+
**/

    val compute_time_duration = udf[Long, Long, Long]((maxts, mints) => {
        (maxts - mints)
    })
    val df_computed_with_duration = df_computed.withColumn("duration",compute_time_duration(col("maxts"), col("mints") ))
    df_computed_with_duration.show
      
/**
 * df_computed_with_duration.show
+------+---------------+-----+-----+--------+                                   
|userid|userid_line_cnt|maxts|mints|duration|
+------+---------------+-----+-----+--------+
|    q1|              2|    2|    1|       1|
|    q2|              3|    2|    1|       1|
|    q3|              1|   12|   12|       0|
+------+---------------+-----+-----+--------+
**/

    val parts = 1
    val output_path = "."
    df_computed_with_duration.repartition(parts).write.parquet(output_path + "/parquet")
    df_computed_with_duration.repartition(parts).rdd.saveAsTextFile(output_path + "/text")
          

  
  
  
  

/***
 * 包括基本的log预处理以及生成dataframe的过程
 * //Part1. 读取parquet格式文件
 * //Part2. 读取txt文件，并利用由StructField数组构造的StructType来添加schema信息
 * //Part3. case class隐式推断数据类型
 * //Part4. 保存结果rdd，分别按照parquet格式和txt格式
 * */
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
    
/////////////////////////////////////////////////////////////////////////    
//Part1. 读取parquet格式文件
/////////////////////////////////////////////////////////////////////////

    val myparquet_path = "./parquet/part-r-00000-2bc12e2b-e375-492d-88cf-df810b43969d.gz.parquet"
    val df_tmp_parquet = sqlContext.read.parquet(myparquet_path)
/**
 scala> df_tmp_parquet.show
+------+---------------+-----+-----+--------+
|userid|userid_line_cnt|maxts|mints|duration|
+------+---------------+-----+-----+--------+
|    q1|              2|    2|    1|       1|
|    q2|              3|    2|    1|       1|
|    q3|              1|   12|   12|       0|
+------+---------------+-----+-----+--------+
**/ 

/////////////////////////////////////////////////////////////////////////
//Part2. 读取txt文件，并利用由StructField数组构造的StructType来添加schema信息
/////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////
//Part3. case class隐式推断数据类型
//////////////////////////////////////////////////////////////////////////
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

/////////////////////////////////////////////////////////////////////////
//Part4. 保存结果rdd，分别按照parquet格式和txt格式
/////////////////////////////////////////////////////////////////////////
    val parts = 1
    val output_path = "."
    df_computed_with_duration.repartition(parts).write.parquet(output_path + "/parquet")
    df_computed_with_duration.repartition(parts).rdd.saveAsTextFile(output_path + "/text")
          
/////////////////////////////////////////////////////////////////////////
//Part5. dataframe join操作
/////////////////////////////////////////////////////////////////////////
    val df_tmp1 = df_computed_with_duration
    val df_tmp2 = df_computed_with_duration.
    withColumnRenamed("userid", "userid2").
    withColumnRenamed("userid_line_cnt", "userid_line_cnt2").
    withColumnRenamed("maxts", "maxts2").
    withColumnRenamed("mints", "mints2").
    withColumnRenamed("duration", "duration2")
    
    
    val df_join = df_tmp1.join(df_tmp2, df_tmp1("userid") === df_tmp2("userid2"))
    df_join.show
     
/**
scala>     df_join.show
+------+---------------+-----+-----+--------+-------+----------------+------+------+---------+
|userid|userid_line_cnt|maxts|mints|duration|userid2|userid_line_cnt2|maxts2|mints2|duration2|
+------+---------------+-----+-----+--------+-------+----------------+------+------+---------+
|    q1|              2|    2|    1|       1|     q1|               2|     2|     1|        1|
|    q2|              3|    2|    1|       1|     q2|               3|     2|     1|        1|
|    q3|              1|   12|   12|       0|     q3|               1|    12|    12|        0|
+------+---------------+-----+-----+--------+-------+----------------+------+------+---------+

**/
  
  
  
  

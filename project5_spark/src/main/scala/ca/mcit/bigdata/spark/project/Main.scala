package ca.mcit.bigdata.spark.project

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait Main {

  /** Hadoop Configuration */
  val conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/hdfs-site.xml"))

  /** Hadoop FileSystem Configuration */
  val fs = FileSystem.get(conf)

  /** SparkSession */
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Streaming Project")
    .master("local[*]")
    .getOrCreate()

  /** SparkContext */
  val sc: SparkContext = spark.sparkContext

  /** SparkStreamingContext */
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

}

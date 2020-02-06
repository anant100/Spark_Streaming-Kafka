package ca.mcit.bigdata.spark.project

import ca.mcit.bigdata.spark.project.schema.StopTimes
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkStreamingEnricher extends Main with App {

  /** Create new Directory for the project (If already exist Replace it) */
  val projectDir = new Path("/user/cloudera/summer2019/project5/thakkar")
  if (fs.exists(projectDir)) fs.delete(projectDir, true)
  fs.mkdirs(projectDir)

  // Data Pipeline Installation
  fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/trips.txt"), new Path("/user/cloudera/summer2019/project5/thakkar/trips/trips"))
  fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/frequencies.txt"), new Path("/user/cloudera/summer2019/project5/thakkar/frequencies/frequencies"))
  fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/calendar_dates.txt"), new Path("/user/cloudera/summer2019/project5/thakkar/calendar_dates/calendar_dates"))

  // Create Output Directory
  /*  val outputDir = new Path("/user/cloudera/summer2019/project5/thakkar/enriched_stop_time")
  fs.mkdirs(outputDir)*/

  val tripsDF = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/cloudera/summer2019/project5/thakkar/trips/trips")

  val frequenciesDF = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/cloudera/summer2019/project5/thakkar/frequencies/frequencies")

  val calendarDatesDF = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .csv("/user/cloudera/summer2019/project5/thakkar/calendar_dates/calendar_dates")

  tripsDF.createOrReplaceTempView("trips")
  frequenciesDF.createOrReplaceTempView("frequencies")
  calendarDatesDF.createOrReplaceTempView("calendarDates")

  val enrichedTrip: DataFrame = spark.sql(
    """ SELECT t.route_id, t.service_id, t.trip_id, t.trip_headsign, t.direction_id, t.shape_id, t.wheelchair_accessible, t.note_fr, t.note_en, d.date,d.exception_type, f.start_time,f.end_time,f.headway_secs
             FROM trips t
             LEFT JOIN calendarDates d ON t.service_id = d.service_id
             LEFT JOIN frequencies f ON t.trip_id = f.trip_id""")

  //enrichedTrip.printSchema()
  //enrichedTrip.show(50, false)

  // Configuration
  val kafkaParams = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "172.16.129.58:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "thakkar1",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val topic = "summer2019_thakkar_stop_times"

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
  )

  val stopTimes: DStream[String] = stream.map(_.value())

  import spark.implicits._

  stopTimes.foreachRDD(microRDD => {
    val stopTimesDF: DataFrame = microRDD.map(_.split(",", -1))
      .map(x => StopTimes(x(0).toInt, x(1), x(2), x(3).toInt, x(4).toInt))
      .toDF

    val output: DataFrame = stopTimesDF.join(enrichedTrip, "trip_id")
    output.write.format("csv").save("/user/cloudera/summer2019/project5/thakkar/enriched_stop_time/")

  })

  ssc.start()
  ssc.awaitTermination()
  //spark.close()
}
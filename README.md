# Spark_Streaming-Kafka

Objective
The objectives of this project are to get experience of coding with
•	Spark
•	SparkSQL
•	Spark Streaming
•	Scala and functional programming

Data set
The data set is the STM GTFS data. 

Problem statement
We get the information of STM every day and need to run an ETL pipeline to enrich data for reporting and analysis purpose in real-time. Data is split in two
1.	A set of tables that build dimension (batch style)
2.	Stop times that needed to be enriched for analysis and reporting (streaming)

Project requirements

1.	Data pipeline installation
Create a directory on HDFS for staging area called /user/cloudera/[GROUP] /project4/[YOUR NAME] where [GROUP] is the program. 
Create a directory for each source table called /user/cloudera/[GROUP]/project5/[YOUR NAME]/[TABLE NAME] where [TABLE NAME] is from the following list
•	trips
•	calendar_dates
•	frequencies
Create Kafka topics called stop_times
Create a directory for the result: /user/cloudera/[GROUP]/project5/[YOUR NAME]/enriched_stop_time

2.	Extract data from STM to staging area
Put extracted version into /user/cloudera/[GROUP] /project4/[YOUR NAME]/[TABLE NAME] path on HDFS where [TABLE NAME] here is the name of file without extension.
We just need the following tables
•	trips
•	calendar_dates
•	frequencies

3.	Data pipeline
Enrich trips with calendar dates and frequencies and write it to the enriched_trip table.
1.	Read trips, calendar dates and frequencies into DataFrame
2.	Create temp view for each one of them 
3.	Use SQL queries to enrich trip with calendar date and frequencies
4.	Stream stop times from Kafka (CSV records)
5.	For each micro-batch, enrich the RDD of stop times with enriched trips dimension
6.	Save enriched stop times on HDFS under result directory

Bonus
-	Convert records to Avro format. You need to define an Avro schema for stop_times, register it to Schema Registry, create POJOs from Avro schema, convert each message to Avro and produce to Kafka, stream Kafka in Spark with help of Schema registry.

 

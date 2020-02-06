package ca.mcit.bigdata.spark.project.schema

case class StopTimes(
                      trip_id: Int,
                      arrival_time: String,
                      departure_time: String,
                      stop_id: Int,
                      stop_sequence: Int
                    )

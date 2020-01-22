package de.hpi.spark_tutorial

import java.io.File

import com.beust.jcommander.JCommander
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    val arguments = new Args()

    JCommander.newBuilder()
      .addObject(arguments)
      .build()
      .parse(args:_*);

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local["+arguments.cores+"]")
    val spark = sparkBuilder.getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "8")

    val inputs = new File(arguments.path).listFiles().map(f => f.getAbsolutePath).toList
    //val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders").map(name => s"data/TPCH/tpch_$name.csv")

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    time {
      Sindy.discoverINDs(inputs, spark)
    }
  }
}

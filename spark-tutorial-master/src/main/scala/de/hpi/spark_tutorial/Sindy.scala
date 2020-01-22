package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val csvs = inputs.map(file => spark
      .read
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file))

    // Check if csvs are loaded successfully
    //csvs.foreach(x => x.explain())

    val cells = csvs.map(
      csv => csv.flatMap(
        row => row.schema.fieldNames.map(
          c => (row.getAs(c).toString, c)
        )
      )
    )

    // 2 columns (value, columnName) e.g (AFRICA, R_NAME)
    val preaggr = cells.reduce(_.union(_))

    //preaggr.show()

    //preaggr.explain()

    // grouped by first column (value)
    val globalPart = preaggr.groupBy($"_1")
    //println(globalPart.toString())

    // 2 columns
    val attrSet = globalPart.agg(collect_set($"_2").as("set"))
    //attrSet.show(20,false)


    val filtered = attrSet.flatMap(row => {
      val attr_set = row.getAs[Seq[String]]("set")
      attr_set.map(r => (r, attr_set.filter(_ != r)))
    }).rdd
    // val filtered = attrSets
    // filtered.show()
    // println(filteredRDD.collect())
    val reduced = filtered.reduceByKey(_ intersect _).toDF()
    val filteredNonEmpty = reduced.filter(row => row.getAs[Seq[String]]("_2").nonEmpty)
    val sorted = filteredNonEmpty.sort($"_1")

    val getResult = sorted.collect()

    for (i <- getResult){
      println(i.get(0)+" < "+i.getAs[Seq[String]](1).mkString(", "))
    }

    //cells.foreach(x =>x.explain())
  }
}

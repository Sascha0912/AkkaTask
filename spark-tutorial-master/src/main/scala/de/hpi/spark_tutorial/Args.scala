package de.hpi.spark_tutorial

import com.beust.jcommander.Parameter

class Args {

  @Parameter(names = Array("--path", "-p"))
  var path = "./TPCH"
  @Parameter(names = Array("--cores", "-c"))
  var cores = 4
}

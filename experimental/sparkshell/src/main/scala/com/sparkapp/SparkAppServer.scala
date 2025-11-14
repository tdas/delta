package com.sparkapp

import org.apache.spark.sql.SparkSession

object SparkAppServer {
  private val DEFAULT_PORT = 8080

  def main(args: Array[String]): Unit = {
    val port = if (args.length > 0) args(0).toInt else DEFAULT_PORT

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("SparkApp SQL REST Server")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"Spark Session initialized: ${spark.version}")

    // Eagerly initialize Spark internals to avoid lazy loading issues
    try {
      spark.sql("SELECT 1").collect()
      println("Spark internals pre-initialized successfully")
    } catch {
      case e: Exception =>
        println(s"Warning during Spark pre-initialization: ${e.getMessage}")
    }

    // Start REST API Server
    val server = new SparkAppServer(spark, port)
    server.start()
    server.blockUntilShutdown()
  }
}

class SparkAppServer(spark: SparkSession, port: Int) {
  private var restApi: RestApi = _

  def start(): Unit = {
    restApi = new RestApi(spark, port)
    restApi.start()

    sys.addShutdownHook {
      System.err.println("Shutting down REST server...")
      stop()
      spark.stop()
      System.err.println("Server shut down.")
    }
  }

  def stop(): Unit = {
    if (restApi != null) {
      restApi.stop()
    }
  }

  def blockUntilShutdown(): Unit = {
    // Keep the main thread alive
    try {
      Thread.currentThread().join()
    } catch {
      case _: InterruptedException =>
        println("Server interrupted, shutting down...")
    }
  }
}

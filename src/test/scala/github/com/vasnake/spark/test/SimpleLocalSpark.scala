/*
 * Copyright (c) 2020-2021, vasnake@gmail.com
 *
 * Licensed under GNU General Public License v3.0 (see LICENSE)
 *
 */

package github.com.vasnake.spark.test

import org.scalatest._
import org.apache.spark.sql.SparkSession
import com.holdenkarau.spark.testing.SparkSessionProvider

trait SimpleLocalSpark extends LocalSpark { this: Suite =>

  override def sparkBuilder: SparkSession.Builder = super.sparkBuilder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", 1)
}

trait LocalSpark extends
  SparkProvider with BeforeAndAfterAll { this: Suite =>

  def loadSpark(): SparkSession = SparkSessionProvider._sparkSession

  protected def sparkBuilder: SparkSession.Builder = SparkSession.builder()
    .master("local[2]")
    .appName(suiteName)
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", suiteId)
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.default.parallelism", 4)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.exec.dynamic.partition", "true")
    .config("spark.sql.warehouse.dir", "./spark_warehouse")
    .config("spark.checkpoint.dir", "./spark_checkpoints")
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=./spark_derby")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionProvider._sparkSession = sparkBuilder.getOrCreate()
  }

  override protected def afterAll(): Unit = {
    SparkSessionProvider._sparkSession.stop()
    SparkSessionProvider._sparkSession = null
    super.afterAll()
  }
}

trait SparkProvider {
  protected def loadSpark(): SparkSession
  @transient protected implicit lazy val spark: SparkSession = loadSpark()
}

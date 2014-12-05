package org.apache.spark.sql.hbasesource

import org.apache.spark.SparkContext
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
 * HBaseTestSparkContext used for test.
 *
 */
trait HBaseTestSparkContext extends BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  def sparkContext: SparkContext = sc

  override def beforeAll: Unit = {
    sc = new SparkContext("local", "test")
  }

  override def afterAll: Unit = {
    sc.stop()
    sc = null
  }
}

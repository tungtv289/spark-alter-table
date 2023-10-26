package vn.ghtk.bigdata

import org.apache.spark.sql.SparkSession
import vn.ghtk.bigdata.analysis.AddColumns

trait Execute {
  def run(sparkSession: SparkSession): Boolean
}

case class AlterTableAddColumnsExecute(addColumns: AddColumns) extends Execute {
  def run(sparkSession: SparkSession): Boolean = {
    val catalog = sparkSession.sessionState.catalog
    true
  }
}
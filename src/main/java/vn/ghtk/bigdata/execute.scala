package vn.ghtk.bigdata

import org.apache.spark.sql.SparkSession
import vn.ghtk.bigdata.analysis.AddColumns

trait Execute {
  def run(sparkSession: SparkSession): Boolean
}

case class AlterTableAddColumnsExecute(addColumns: AddColumns) extends Execute {
  def run(sparkSession: SparkSession): Boolean = {
    //    val catalog = sparkSession.sessionState.catalog
    val dfTblCheckpoint = sparkSession.read.option("mergeSchema", "true").parquet("/user/hive/warehouse/cdc/coordinate_verification_avro_postgres_bigdata.public.cod_validations")
    dfTblCheckpoint.createOrReplaceTempView("raw_table_checkpoint")
    sparkSession.sql(s"select _partition, min(_offset), min(source.ts_ms) as ext_ts_ms from raw_table_checkpoint where after.station_id is not null or after.type is not null group by _partition order by ext_ts_ms").show(false)


    val df = sparkSession.read.parquet("/user/hive/warehouse/cdc/ghtk_payment_avro_bigdata.ghtk_payment.cod_order_payment_trans")
    df.repartition(1000).write.format("parquet").mode("overwrite").partitionBy("data_date_key").save("/user/tungtv84/cod_order_payment_trans")
    println(addColumns.genSparkSql)
    true
  }
}

case class AlterTableRenameColumnsExecute() extends Execute {
  def run(sparkSession: SparkSession): Boolean = {
    //    val catalog = sparkSession.sessionState.catalog
    println("jojo")
    true
  }
}
package vn.ghtk.bigdata

import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.bigdata.analysis.AddColumns
import vn.ghtk.bigdata.utils.HdfsFileUtils.{getExistedHdfsFolder, getFilePathFromPartition, getListFileModifiedAfterCheckpoint}
import vn.ghtk.bigdata.utils.SnapshotCDCConfig._
import vn.ghtk.bigdata.utils.Utils.getPrivateKey
import vn.ghtk.bigdata.utils.{CheckpointUtils, RSAUtils, SnapshotCDCConfig}

trait Execute {
  def run(sparkSession: SparkSession): Boolean
}

case class AlterTableAddColumnsExecute(addColumns: AddColumns, alterTimestamp: Long) extends Execute {
  private val logger = Logger.getLogger(this.getClass)

  def run(spark: SparkSession): Boolean = {
    //    get topic
    //    val dfTblCheckpoint = spark.read.parquet("/user/hive/warehouse/cdc/coordinate_verification_avro_postgres_bigdata.pub.cod_validations")
    //    dfTblCheckpoint.createOrReplaceTempView("raw_table_checkpoint")
    //    spark.sql(s"select _partition, min(_offset), min(source.ts_ms) as ext_ts_ms from raw_table_checkpoint where after.station_id is not null or after.type is not null group by _partition order by ext_ts_ms").show(false)

    val keyLocation = spark.conf.get("spark.ghtk.altertable.key.location")
    val privateKey = getPrivateKey(keyLocation)

    val jdbcUrl = spark.conf.get("spark.ghtk.altertable.jdbc.bigdata.url")
    val username = spark.conf.get("spark.ghtk.altertable.jdbc.bigdata.username")
    val password = RSAUtils.decrypt(spark.conf.get("spark.ghtk.altertable.jdbc.bigdata.password"), privateKey)

    val snapshotCDCConfig = SnapshotCDCConfig.getSnapshotCdcConfig(
      "jdbcUrl", "username", "password", addColumns.table.getFullyQualifiedName)

    try {
      val tablePathAfterAlter = s"${snapshotCDCConfig.table_path}_alter_table_bk"
      var table = spark.read.parquet(snapshotCDCConfig.table_path)
      addColumns.columnsToAdd.foreach(col => {
        if (!col.default.equalsIgnoreCase("NULL")) {
          table = table.withColumn(col.colName, lit(col.default).cast(col.dataType))
        }
      })

      table.repartition(snapshotCDCConfig.partition_format.split(",").map(item => col(item)): _*)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .partitionBy(snapshotCDCConfig.partition_format.split(","): _*)
        .save(tablePathAfterAlter)

      val checkpointAfterAlter = alterTimestamp - snapshotCDCConfig.late_arriving_window
      val cdcAllChangedFileAfterCheckpoint = getListFileModifiedAfterCheckpoint(snapshotCDCConfig.cdc_path, checkpointAfterAlter, false)

      println("CDC file: ")
      cdcAllChangedFileAfterCheckpoint.foreach(
        cdc => {
          println(cdc)
        }
      )

      if (cdcAllChangedFileAfterCheckpoint.nonEmpty) {
        if (snapshotCDCConfig.checkpoint_max_modified > 0) {
          spark.read
            .option("mergeSchema", "true")
            .option("basePath", snapshotCDCConfig.cdc_path)
            .parquet(cdcAllChangedFileAfterCheckpoint: _*).createOrReplaceTempView("raw_table_checkpoint")
        }
        spark.sqlContext.cacheTable("raw_table_checkpoint")

        val filterCondition = addColumns.columnsToAdd.map(col => s"${col.colName} is not null").mkString(" or ")
        val filterData = spark.sql(f"select * from raw_table_checkpoint where $filterCondition")

        val maxCheckpointField = spark.sql(f"select max(${snapshotCDCConfig.checkpoint_field}) from raw_table_checkpoint")
          .collect()(0).getLong(0)
        val maxOffsetEachPartition = spark.sql(f"select max(`_offset`) as `max_offset`, `_partition` as `partition` from raw_table_checkpoint group by `_partition`")

        filterData
          .drop("data_date_key", "action_hour")
          .withColumn("alter_table_order", lit(2).cast("int"))
          .createOrReplaceTempView("reformatedData")

        val finalData = spark.sql(snapshotCDCConfig.sql_parser)
        finalData.createOrReplaceTempView("dataFromPartitionChanged")

        val partitionChanged = spark
          .sql(f"select distinct(${snapshotCDCConfig.partition_format}) from dataFromPartitionChanged")
          .collect().map(partition => partition.getInt(0).toString)

        for (elem <- partitionChanged) {
          println("Partition changed data :" + elem)
        }

        if (partitionChanged.nonEmpty) {
          //upsert
          val tablePathNeedRowNumber = getFilePathFromPartition(tablePathAfterAlter, snapshotCDCConfig.partition_format, partitionChanged)
          println("Table folder changed data :" + tablePathNeedRowNumber)

          val existedHdfsTablePath = getExistedHdfsFolder(tablePathNeedRowNumber)
          println("Partition existed data :" + existedHdfsTablePath)

          if (existedHdfsTablePath.nonEmpty) {
            val currentData = spark.read
              .option("basePath", tablePathAfterAlter)
              .parquet(existedHdfsTablePath: _*)
              .withColumn("alter_table_order", lit(1).cast("int"))
            currentData.unionByName(finalData).createOrReplaceTempView("temp_view")
          }
          else {
            println("CDC not existed data, snapshot mode")
            finalData.createOrReplaceTempView("temp_view")
          }


          val sqlOnly =
            s"""
               |SELECT *
               |FROM (
               |    SELECT
               |        *,
               |        ROW_NUMBER() OVER(
               |            PARTITION BY ${snapshotCDCConfig.partition_format} , ${snapshotCDCConfig.keys}
               |            ORDER BY `_external_ts_sec` DESC, `_external_pos` DESC, `_external_row` DESC, alter_table_order DESC
               |        ) _external_rn
               |    FROM temp_view
               |) tb
               |WHERE _external_rn = 1
                """.stripMargin
          val dfRs = spark.sql(sqlOnly).drop("_external_rn")


          if (StringUtils.isBlank(snapshotCDCConfig.partition_format)) {
            dfRs
              .write
              .format("parquet")
              .mode(SaveMode.Overwrite)
              .save(tablePathAfterAlter)
          } else {
            dfRs
              .repartition(snapshotCDCConfig.partition_format.split(",").map(item => col(item)): _*)
              .write
              .format("parquet")
              .mode(SaveMode.Overwrite)
              .partitionBy(snapshotCDCConfig.partition_format.split(","): _*)
              .save(tablePathAfterAlter)
          }
          if (snapshotCDCConfig.checkpoint_max_modified > 0) {
            asyncInvalidateMetadataTable(jdbcUrl, username, password,
              snapshotCDCConfig.table_name)
          }


          if (snapshotCDCConfig.checkpoint_max_modified > 0) {
            partitionChanged.foreach(
              partitionValue => {
                println(f"`${snapshotCDCConfig.partition_format}` =  $partitionValue")
                addPartitionToMySQL(jdbcUrl, username, password,
                  snapshotCDCConfig.table_name, f"`${snapshotCDCConfig.partition_format}` =  $partitionValue", snapshotCDCConfig.partition_format)
                updateRefreshTableWithPartition(jdbcUrl, username, password,
                  snapshotCDCConfig.table_name, f"`${snapshotCDCConfig.partition_format}` =  $partitionValue")
              }
            )
          }
        }


        updateLastModified(jdbcUrl, username, password,
          snapshotCDCConfig.table_name, maxCheckpointField)
        CheckpointUtils.updateCheckpoints(jdbcUrl, username, password,
          snapshotCDCConfig.table_name, snapshotCDCConfig.topic_name, maxOffsetEachPartition.collect())
      } else {
        logger.info("No file changed from cdc path " + snapshotCDCConfig.cdc_path + " after " + snapshotCDCConfig.checkpoint_max_modified)
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }


    //    tat ca xu ly trong nay
    val df = spark.read.parquet("/user/hive/warehouse/cdc/ghtk_payment_avro_bigdata.ghtk_payment.cod_order_payment_trans")
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

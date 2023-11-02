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
  protected val logger: Logger = Logger.getLogger(this.getClass)

  def run(sparkSession: SparkSession): Boolean
}

case class AlterTableAddColumnsExecute(addColumns: AddColumns, alterTimestamp: Long) extends Execute {
  def run(spark: SparkSession): Boolean = {
    val keyLocation = spark.conf.get("spark.ghtk.altertable.key.location")
    val privateKey = getPrivateKey(keyLocation)

    val jdbcUrl = spark.conf.get("spark.ghtk.altertable.jdbc.bigdata.url")
    val username = spark.conf.get("spark.ghtk.altertable.jdbc.bigdata.username")
    val password = RSAUtils.decrypt(spark.conf.get("spark.ghtk.altertable.jdbc.bigdata.password"), privateKey)

    val snapshotCDCConfig = SnapshotCDCConfig.getSnapshotCdcConfig(
      jdbcUrl, username, password, addColumns.table.getFullyQualifiedName)

    try {
      val tableAfterAddDefaultPath = s"${snapshotCDCConfig.table_path}_alter_tbl_path"
      var tableAfterAddDefaultDF = spark.read.parquet(snapshotCDCConfig.table_path)
      val sqlParserAfterAlter = fixedSqlParser(snapshotCDCConfig.sql_parser, addColumns.getSqlParseNeedAdd)

      addColumns.getSparkFieldToAdd.foreach(col => {
        if (!col.default.equalsIgnoreCase("NULL")) {
          tableAfterAddDefaultDF = tableAfterAddDefaultDF.withColumn(col.colName, lit(col.default).cast(col.dataType))
        }
      })

      tableAfterAddDefaultDF
        .repartition(snapshotCDCConfig.partition_format.split(",").map(item => col(item)): _*)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .partitionBy(snapshotCDCConfig.partition_format.split(","): _*)
        .save(tableAfterAddDefaultPath)

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
            .parquet(cdcAllChangedFileAfterCheckpoint: _*)
            .createOrReplaceTempView("raw_table_checkpoint")
        }
        spark.sqlContext.cacheTable("raw_table_checkpoint")

        val filterCondition = addColumns.getSparkFieldToAdd.map(col => s"${col.colName} is not null").mkString(" or ")
        val filterData = spark.sql(f"select * from raw_table_checkpoint where $filterCondition")

        val maxCheckpointField = spark.sql(f"select max(${snapshotCDCConfig.checkpoint_field}) from raw_table_checkpoint")
          .collect()(0).getLong(0)
        val maxOffsetEachPartition = spark.sql(f"select max(`_offset`) as `max_offset`, `_partition` as `partition` from raw_table_checkpoint group by `_partition`")

        filterData
          .drop("data_date_key", "action_hour")
          .withColumn("alter_table_order", lit(2).cast("int"))
          .createOrReplaceTempView("reformatedData")

        val finalData = spark.sql(sqlParserAfterAlter)
        finalData.createOrReplaceTempView("dataFromPartitionChanged")

        val partitionChanged = spark
          .sql(f"select distinct(${snapshotCDCConfig.partition_format}) from dataFromPartitionChanged")
          .collect().map(partition => partition.getInt(0).toString)

        for (elem <- partitionChanged) {
          println("Partition changed data :" + elem)
        }

        if (partitionChanged.nonEmpty) {
          //upsert
          val tablePathNeedRowNumber = getFilePathFromPartition(tableAfterAddDefaultPath, snapshotCDCConfig.partition_format, partitionChanged)
          println("Table folder changed data :" + tablePathNeedRowNumber)

          val existedHdfsTablePath = getExistedHdfsFolder(tablePathNeedRowNumber)
          println("Partition existed data :" + existedHdfsTablePath)

          if (existedHdfsTablePath.nonEmpty) {
            val currentData = spark.read
              .option("basePath", tableAfterAddDefaultPath)
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
          val dfRs = spark.sql(sqlOnly).drop("_external_rn").drop("alter_table_order")


          if (StringUtils.isBlank(snapshotCDCConfig.partition_format)) {
            dfRs
              .write
              .format("parquet")
              .mode(SaveMode.Overwrite)
              .save(tableAfterAddDefaultPath)
          } else {
            dfRs
              .repartition(snapshotCDCConfig.partition_format.split(",").map(item => col(item)): _*)
              .write
              .format("parquet")
              .mode(SaveMode.Overwrite)
              .partitionBy(snapshotCDCConfig.partition_format.split(","): _*)
              .save(tableAfterAddDefaultPath)
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


        updateSnapshotCdcConfig(jdbcUrl, username, password,
          snapshotCDCConfig.table_name, maxCheckpointField, sqlParserAfterAlter)
        CheckpointUtils.updateCheckpoints(jdbcUrl, username, password,
          snapshotCDCConfig.table_name, snapshotCDCConfig.topic_name, maxOffsetEachPartition.collect())
      } else {
        logger.info("No file changed from cdc path " + snapshotCDCConfig.cdc_path + " after " + snapshotCDCConfig.checkpoint_max_modified)
      }
      //      println(addColumns.genSparkSql)
      true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        false
      }
    }
  }

  def fixedSqlParser(old: String, needAdd: String): String = {
    val index = old.lastIndexOf("FROM reformatedData")
    if (index != -1) {
      val newString = s"${old.substring(0, index)}, ${needAdd} ${old.substring(index)}"
      newString
    } else {
      throw new Exception("Error format")
    }
  }
}

case class AlterTableRenameColumnsExecute() extends Execute {
  def run(sparkSession: SparkSession): Boolean = {
    //    val catalog = sparkSession.sessionState.catalog
    println("jojo")
    true
  }
}

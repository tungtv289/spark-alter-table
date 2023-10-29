package vn.ghtk.bigdata


import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.bigdata.utils.ArgumentsParseUtils.getETLArguments
import vn.ghtk.bigdata.utils.HdfsFileUtils.{getExistedHdfsFolder, getFilePathFromPartition, getListFileModifiedAfterCheckpoint}
import vn.ghtk.bigdata.utils.SnapshotCDCConfig._
import vn.ghtk.bigdata.utils.Utils.getPrivateKey
import vn.ghtk.bigdata.utils.{CheckpointUtils, RSAUtils, SnapshotCDCConfig}

import java.util.concurrent.Executors

object Main {
  private val logger = Logger.getLogger("vn.ghtk.bigdata.Main")


  val sql_parser =
    """
      |SELECT
      |	CAST(op AS STRING) AS _external_op,
      |	CAST(source.ts_ms AS BIGINT) AS _external_ts_sec,
      |	CAST(null AS BIGINT) AS _external_pos,
      |	CAST(null AS INT) _external_row,
      |	CAST(null AS STRING) _external_gtid,
      |	CAST(IF(op = 'd', before.id, after.id) AS BIGINT) AS id,
      |	CAST(IF(op = 'd', before.address_validation_id, after.address_validation_id) AS BIGINT) AS address_validation_id,
      |	CAST(IF(op = 'd', before.cod_id, after.cod_id) AS INT) AS cod_id,
      |	CAST(IF(op = 'd', before.pkg_order, after.pkg_order) AS BIGINT) AS pkg_order,
      |	CAST(IF(op = 'd', before.logistics_address_type, after.logistics_address_type) AS SMALLINT) AS logistics_address_type,
      |	CAST(IF(op = 'd', before.customer_address, after.customer_address) AS STRING) AS customer_address,
      |	CAST(IF(op = 'd', before.parsed_id, after.parsed_id) AS BIGINT) AS parsed_id,
      |	CAST(IF(op = 'd', before.parsed_source, after.parsed_source) AS SMALLINT) AS parsed_source,
      |	CAST(IF(op = 'd', before.parsed_lat, after.parsed_lat) AS DOUBLE) AS parsed_lat,
      |	CAST(IF(op = 'd', before.parsed_lng, after.parsed_lng) AS DOUBLE) AS parsed_lng,
      |	CAST(IF(op = 'd', before.distance, after.distance) AS DOUBLE) AS distance,
      |	CAST(IF(op = 'd', before.validate_status, after.validate_status) AS SMALLINT) AS validate_status,
      |	CAST(IF(op = 'd', before.dcc5_incorrect_count, after.dcc5_incorrect_count) AS SMALLINT) AS dcc5_incorrect_count,
      |	IF(op = 'd', CAST(before.modified / 1000000 AS TIMESTAMP) - INTERVAL 7 HOURS, CAST(after.modified / 1000000 AS TIMESTAMP) - INTERVAL 7 HOURS) AS modified,
      |	IF(op = 'd', CAST(before.created / 1000000 AS TIMESTAMP) - INTERVAL 7 HOURS, CAST(after.created / 1000000 AS TIMESTAMP) - INTERVAL 7 HOURS) AS created,
      |	CAST(
      |                DATE_FORMAT(
      |                    IF(
      |                        op = 'd',
      |                        CAST(before.created / 1000000 AS TIMESTAMP) - INTERVAL 7 HOURS,
      |                        CAST(after.created / 1000000 AS TIMESTAMP) - INTERVAL 7 HOURS
      |                    ),
      |                    'yyyyMMdd'
      |                )
      |                AS INT
      |            ) AS data_date_key,
      | CAST(IF(op = 'd', before.station_id, after.station_id) AS INT) AS station_id,
      | CAST(IF(op = 'd', before.type, after.type) AS INT) AS type
      |FROM reformatedData
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder().config(sparkConf)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    val arguments = getETLArguments(args)
    val keyLocation = arguments("keyLocation")
    val privateKey = getPrivateKey(keyLocation)

    val jdbcUrl = arguments("jdbcUrl")
    val appName = arguments("appName")
    val username = arguments("username")
    val password = RSAUtils.decrypt(arguments("password"), privateKey)

    val snapshotCDCConfigs = SnapshotCDCConfig.getListHdfsFolderPath(
      jdbcUrl, username, password, appName)

    val namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("ghtk-backend-thread-%d").build()
    val pool = Executors.newFixedThreadPool(5, namedThreadFactory)

    if (snapshotCDCConfigs.nonEmpty) {
      snapshotCDCConfigs.foreach(snapshotCDCConfig => {
        try {
          val saveMode = {
            if (snapshotCDCConfig.checkpoint_max_modified <= 0) SaveMode.Overwrite
            else {
              if (snapshotCDCConfig.table_type == "insert") SaveMode.Append else SaveMode.Overwrite
            }
          }
          val checkpointHdfs = snapshotCDCConfig.checkpoint_max_modified - snapshotCDCConfig.late_arriving_window
          val cdcAllChangedFileAfterCheckpoint = getListFileModifiedAfterCheckpoint(snapshotCDCConfig.cdc_path, checkpointHdfs, false)
          val snapshotChangedFileAfterCheckpoint = getListFileModifiedAfterCheckpoint(snapshotCDCConfig.snapshot_path, checkpointHdfs, true)

          var cdcChangedFileAfterCheckpoint = cdcAllChangedFileAfterCheckpoint
          if (snapshotCDCConfig.read_max_partition != -1) {
            cdcChangedFileAfterCheckpoint = cdcAllChangedFileAfterCheckpoint.take(snapshotCDCConfig.read_max_partition)
          }
          println("CDC file: ")
          cdcChangedFileAfterCheckpoint.foreach(
            cdc => {
              println(cdc)
            }
          )
          println("Snapshot file: ")
          snapshotChangedFileAfterCheckpoint.foreach(
            snapshot => {
              println(snapshot)
            }
          )

          if (snapshotChangedFileAfterCheckpoint.nonEmpty || cdcChangedFileAfterCheckpoint.nonEmpty) {
            if (snapshotCDCConfig.checkpoint_max_modified > 0) {
              spark.read
                .option("basePath", snapshotCDCConfig.cdc_path)
                .parquet(cdcChangedFileAfterCheckpoint: _*).createOrReplaceTempView("unionData")
            } else if (snapshotCDCConfig.checkpoint_max_modified == 0) {
              if (snapshotChangedFileAfterCheckpoint.nonEmpty) {
                println("Only read snapshot")
                spark.read
                  .parquet(snapshotChangedFileAfterCheckpoint: _*)
                  .createOrReplaceTempView("unionData")
              } else {
                spark.read
                  .option("basePath", snapshotCDCConfig.cdc_path)
                  .parquet(cdcChangedFileAfterCheckpoint: _*)
                  .createOrReplaceTempView("unionData")
              }
            } else if (snapshotCDCConfig.checkpoint_max_modified < 0) {
              val snapshot = spark.read.parquet(snapshotChangedFileAfterCheckpoint: _*)
              snapshot.createOrReplaceTempView("unionData")
              val cdc = spark.read
                .option("basePath", snapshotCDCConfig.cdc_path)
                .parquet(cdcChangedFileAfterCheckpoint: _*)
              snapshot.unionByName(cdc, allowMissingColumns = true).createOrReplaceTempView("unionData")
            }
            spark.sqlContext.cacheTable("unionData")

            val checkpoints = CheckpointUtils.getCheckpoints(jdbcUrl, username, password, snapshotCDCConfig.table_name, snapshotCDCConfig.topic_name)
            val filterCondition = CheckpointUtils.buildFilterCondition(checkpoints)
            val filterData = spark.sql(f"select * from unionData where $filterCondition")

            val maxCheckpointField = spark.sql(f"select max(${snapshotCDCConfig.checkpoint_field}) from unionData")
              .collect()(0).getLong(0)
            val maxOffsetEachPartition = spark.sql(f"select max(`_offset`) as `max_offset`, `_partition` as `partition` from unionData group by `_partition`")

            filterData.drop("data_date_key", "action_hour").createOrReplaceTempView("reformatedData")

            val finalData = spark.sql(snapshotCDCConfig.sql_parser)
            finalData.createOrReplaceTempView("dataFromPartitionChanged")

            val partitionChanged = spark
              .sql(f"select distinct(${snapshotCDCConfig.partition_format}) from dataFromPartitionChanged")
              .collect().map(partition => partition.getInt(0).toString)

            for (elem <- partitionChanged) {
              println("Partition changed data :" + elem)
            }

            if (partitionChanged.nonEmpty) {
              //some new data from cdc
              if (snapshotCDCConfig.table_type == "insert") {
                if (StringUtils.isBlank(snapshotCDCConfig.partition_format)) {
                  finalData
                    .write
                    .mode(saveMode)
                    .parquet(snapshotCDCConfig.table_path)
                } else {
                  finalData
                    .write
                    .mode(saveMode)
                    .partitionBy(snapshotCDCConfig.partition_format.split(","): _*)
                    .parquet(snapshotCDCConfig.table_path)
                }
              } else {
                //upsert
                if (snapshotCDCConfig.checkpoint_max_modified > 0) {
                  val tablePathNeedRowNumber = getFilePathFromPartition(snapshotCDCConfig.table_path, snapshotCDCConfig.partition_format, partitionChanged)
                  println("Table folder changed data :" + tablePathNeedRowNumber)

                  val existedHdfsTablePath = getExistedHdfsFolder(tablePathNeedRowNumber)
                  println("Partition existed data :" + existedHdfsTablePath)

                  if (existedHdfsTablePath.nonEmpty) {
                    val currentData = spark.read
                      .option("basePath", snapshotCDCConfig.table_path)
                      .parquet(existedHdfsTablePath: _*)
                    currentData.unionByName(finalData).createOrReplaceTempView("temp_view")
                  }
                  else {
                    println("CDC not existed data, snapshot mode")
                    finalData.createOrReplaceTempView("temp_view")
                  }
                }
                else {
                  println("Snapshot mode")
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
                     |            ORDER BY `_external_ts_sec` DESC, `_external_pos` DESC, `_external_row` DESC
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
                    .mode(saveMode)
                    .save(snapshotCDCConfig.table_path)
                } else {
                  dfRs
                    .repartition(snapshotCDCConfig.partition_format.split(",").map(item => col(item)): _*)
                    .write
                    .format("parquet")
                    .mode(saveMode)
                    .partitionBy(snapshotCDCConfig.partition_format.split(","): _*)
                    .save(snapshotCDCConfig.table_path)
                }
                if (snapshotCDCConfig.checkpoint_max_modified > 0) {
                  asyncInvalidateMetadataTable(jdbcUrl, username, password,
                    snapshotCDCConfig.table_name)
                }

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

            // update vao bang mysql cho luong tao bang.
            if (snapshotCDCConfig.checkpoint_max_modified == 0) {
              autoCreateTable(jdbcUrl, username, password,
                snapshotCDCConfig.table_name, snapshotCDCConfig.partition_format)
            }

            updateLastModified(jdbcUrl, username, password,
              appName, snapshotCDCConfig.table_name, maxCheckpointField)
            CheckpointUtils.updateCheckpoints(jdbcUrl, username, password,
              snapshotCDCConfig.table_name, snapshotCDCConfig.topic_name, maxOffsetEachPartition.collect())
            updateJobStatus(jdbcUrl, username, password, appName, snapshotCDCConfig.table_name, "SUCCESS")
          } else {
            logger.info("No file changed from cdc path " + snapshotCDCConfig.cdc_path + " after " + snapshotCDCConfig.checkpoint_max_modified)
          }
        } catch {
          case e: Exception => {
            updateJobStatus(jdbcUrl, username, password, appName, snapshotCDCConfig.table_name, "FAILED")
            e.printStackTrace()
          }
        }

      })
    }
    pool.shutdown()
  }
}

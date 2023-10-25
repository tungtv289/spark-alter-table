package vn.ghtk.bigdata.utils

import java.sql.DriverManager
import scala.collection.mutable.ListBuffer


case class SnapshotCDCConfig(
                              topic_name: String,
                              cdc_path: String,
                              snapshot_path: String,
                              table_path: String,
                              table_name: String,
                              table_type: String,
                              partition_format: String,
                              keys: String,
                              read_max_partition: Int,
                              late_arriving_window: Int,
                              sql_parser: String,
                              checkpoint_field: String,
                              checkpoint_max_modified: Long
                            )

object SnapshotCDCConfig {
  def addPartitionToMySQL(mysqlUrl: String, username: String, password: String,
                          table_name: String, prd_value: String, prd_id: String) = {
    val connection = DriverManager.getConnection(mysqlUrl, username, password)

    val query =
      """
        |INSERT IGNORE bigdata.async_add_partition (`impala_tbl_name`, `partition_value`, `partition_key`, `status`)
        |VALUES (?, ?, ?, ?)
        |""".stripMargin


    val stmt = connection.prepareStatement(query)

    stmt.setString(1, table_name)
    stmt.setString(2, prd_value)
    stmt.setString(3, prd_id)
    stmt.setString(4, "NEW")

    stmt.execute()
    stmt.close()


    connection.close()
  }

  def asyncInvalidateMetadataTable(mysqlUrl: String, username: String, password: String,
                                   table_name: String) = {
    val connection = DriverManager.getConnection(mysqlUrl, username, password)

    val query =
      """
        |INSERT INTO bigdata.async_invalidate_metadata_table (`tbl_name`, `status`)
        |VALUES (?, ?)
        |ON DUPLICATE KEY UPDATE status = VALUES(status)
        |""".stripMargin


    val stmt = connection.prepareStatement(query)

    stmt.setString(1, table_name)
    stmt.setString(2, "NEW")

    stmt.execute()
    stmt.close()


    connection.close()
  }

  def updateRefreshTableWithPartition(mysqlUrl: String, username: String, password: String,
                                      table_name: String, prd_value: String) = {
    val connection = DriverManager.getConnection(mysqlUrl, username, password)

    val query =
      """
        |INSERT INTO bigdata.async_refresh_table_with_partition (`tbl_name`, `partition_value`, `status`, `compute_status`)
        |VALUES (?, ?, ?, ?)
        |ON DUPLICATE KEY UPDATE status = VALUES(status), compute_status = VALUES(compute_status)
        |""".stripMargin


    val stmt = connection.prepareStatement(query)

    stmt.setString(1, table_name)
    stmt.setString(2, prd_value)
    stmt.setString(3, "NEW")
    stmt.setString(4, "NEW")

    stmt.execute()
    stmt.close()


    connection.close()
  }

  def updateLastModified(mysqlUrl: String, username: String, password: String, appName: String, tableName: String, lastCheckpoint: Long): Unit = {
    val connection = DriverManager.getConnection(mysqlUrl, username, password)

    val updateQuery =
      """
        |UPDATE bigdata.snapshot_cdc_config_ingest
        |SET checkpoint_time=%s
        |WHERE app_name = '%s' and table_name = '%s'
        |""".stripMargin.format(lastCheckpoint, appName, tableName)

    println(updateQuery)

    val stmt = connection.prepareStatement(updateQuery)
    stmt.execute()
    stmt.close()
    connection.close()
  }

  def getListHdfsFolderPath(url: String, username: String, password: String, jobName: String): List[SnapshotCDCConfig] = {
    val res: ListBuffer[SnapshotCDCConfig] = new ListBuffer[SnapshotCDCConfig]()

    try {
      val connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        s"""SELECT topic_name, cdc_base_path, snapshot_base_path, table_path, table_name, table_type, partition_format,
           |`keys`, read_max_partition, sql_parser, checkpoint_field, checkpoint_time, late_arriving_window
           |FROM bigdata.snapshot_cdc_config_ingest
           |WHERE `app_name` = '$jobName' AND `status` = 1 ORDER BY `order` DESC
           |""".stripMargin)
      print()
      while (resultSet.next()) {
        val topic_name = resultSet.getString("topic_name")
        val cdc_path = resultSet.getString("cdc_base_path") + "/" + topic_name
        val snapshot_path = resultSet.getString("snapshot_base_path") + "/" + topic_name
        val table_name = resultSet.getString("table_name")
        val table_path = resultSet.getString("table_path") + "/" + table_name.replace(".", ".db/")
        val table_type = resultSet.getString("table_type")
        val partition_format = resultSet.getString("partition_format")
        val keys = resultSet.getString("keys")
        val read_max_partition = resultSet.getInt("read_max_partition")
        val late_arriving_window = resultSet.getInt("late_arriving_window")
        val sql_parser = resultSet.getString("sql_parser")
        val checkpoint_field = resultSet.getString("checkpoint_field")
        val checkpoint_max_modified = resultSet.getLong("checkpoint_time")

        res += SnapshotCDCConfig(topic_name, cdc_path, snapshot_path, table_path, table_name, table_type,
          partition_format, keys, read_max_partition, late_arriving_window, sql_parser, checkpoint_field, checkpoint_max_modified)
      }
      statement.close()
      connection.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    res.toList
  }

  def autoCreateTable(url: String, username: String, password: String, tableName: String, partitionFormat: String): Unit = {
    val connection = DriverManager.getConnection(url, username, password)
    val insertQuery =
      """insert into bigdata.async_create_impala_table (tbl_name, prd_id, `status`)
        |values (?, ?, ?)
        |on duplicate key update prd_id = values(prd_id), `status` = values(`status`)""".stripMargin

    val stmt = connection.prepareStatement(insertQuery)
    stmt.setString(1, tableName)
    stmt.setString(2, partitionFormat)
    stmt.setString(3, "NEW")
    stmt.execute()
    stmt.close()
    connection.close()
  }

  def updateJobStatus(mysqlUrl: String, username: String, password: String, appName: String, tableName: String, jobStatus: String): Unit = {
    val connection = DriverManager.getConnection(mysqlUrl, username, password)

    val updateQuery =
      """
        |UPDATE bigdata.snapshot_cdc_config_ingest
        |SET job_status=?
        |WHERE app_name = ? and table_name = ?
        |""".stripMargin

    println(updateQuery)
    val stmt = connection.prepareStatement(updateQuery)
    stmt.setString(1, jobStatus)
    stmt.setString(2, appName)
    stmt.setString(3, tableName)
    stmt.execute()
    stmt.close()
    connection.close()
  }
}

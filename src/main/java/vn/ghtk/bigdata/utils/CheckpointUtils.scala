package vn.ghtk.bigdata.utils

import org.apache.spark.sql.Row

import java.sql.DriverManager


case class Checkpoint(
                       tableName: String,
                       topic: String,
                       partition: Int,
                       offset: Long
                     )

object CheckpointUtils {
  def getCheckpoints(mysqlUrl: String, username: String, password: String,
                     tableName: String, topic: String): List[Checkpoint] = {
    val query =
      """SELECT
        |    table_name,
        |    topic,
        |    `partition`,
        |    offset
        |FROM bigdata.ctl_checkpoint
        |WHERE table_name = ? AND topic = ?""".stripMargin

    val connection = DriverManager.getConnection(mysqlUrl, username, password)
    val stmt = connection.prepareStatement(query)
    try {
      stmt.setString(1, tableName)
      stmt.setString(2, topic)
      val rs = stmt.executeQuery()
      val checkpoints = new scala.collection.mutable.ListBuffer[Checkpoint]()
      while (rs.next()) {
        val checkpoint = Checkpoint(
          rs.getString("table_name"),
          rs.getString("topic"),
          rs.getInt("partition"),
          rs.getLong("offset")
        )
        checkpoints += checkpoint
      }
      checkpoints.toList
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    } finally {
      stmt.close()
      connection.close()
    }
  }

  def buildFilterCondition(checkpoints: List[Checkpoint]): String = {
    if (checkpoints.isEmpty) {
      return "1 = 1"
    }
    val filterCondition = checkpoints.map(checkpoint => {
      s"(_partition = ${checkpoint.partition} AND _offset > ${checkpoint.offset})"
    }).mkString(" OR ")
    s"($filterCondition) OR (_partition NOT IN (${checkpoints.map(_.partition).mkString(", ")}))"
  }

  def updateCheckpoints(mysqlUrl: String, username: String, password: String,
                        tableName: String, topic: String, checkpoints: Array[Row]): Unit = {
    val connection = DriverManager.getConnection(mysqlUrl, username, password)
    val query =
      """INSERT INTO bigdata.ctl_checkpoint (table_name, topic, `partition`, offset)
        |VALUES (?, ?, ?, ?)
        |ON DUPLICATE KEY UPDATE offset = VALUES(offset)""".stripMargin
    val stmt = connection.prepareStatement(query)
    try {
      for (checkpoint <- checkpoints) {
        val maxOffset = checkpoint.getLong(0)
        val partition = checkpoint.getInt(1)
        stmt.setString(1, tableName)
        stmt.setString(2, topic)
        stmt.setInt(3, partition)
        stmt.setLong(4, maxOffset)
        stmt.addBatch()
      }
      stmt.executeBatch()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    } finally {
      stmt.close()
      connection.close()
    }
  }
}

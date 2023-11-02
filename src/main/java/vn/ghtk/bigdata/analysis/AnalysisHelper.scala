package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.alter.Alter

import scala.collection.JavaConverters.asScalaBufferConverter

trait AnalysisHelper {
  def resolveOperatorsUp(statement: Statement): Command
}

class MysqlAnalysisHelper extends AnalysisHelper {
  def resolveOperatorsUp(statement: Statement): Command = {
    statement match {
      case statement: Alter => {
        //        check bigdataTbl is conf  ig alter
        val isAlter = true
        if (isAlter) {
          val expressions = statement.getAlterExpressions.asScala.toList
          var cols = Seq[QualifiedColType]()
          expressions.foreach(expression => {
            if (expression.getColDataTypeList != null && expression.getColDataTypeList.size() == 1) {
              val cdt = expression.getColDataTypeList.get(0)
              val defaultIndex = cdt.getColumnSpecs.asScala.indexWhere(_.equalsIgnoreCase("DEFAULT"))
              var defaultValue = "NULL"
              if (defaultIndex != 1) {
                defaultValue = cdt.getColumnSpecs.get(defaultIndex + 1)
              }
              cols = cols :+ QualifiedColType(cdt.getColumnName, cdt.getColDataType.getDataType, nullable = true, defaultValue, Some(""), Some(""))
            }
          })
          AddColumns(statement.getTable, new MysqlAddColumnsFunc, cols)
        } else {
          throw new Exception("Table not alter")
        }
      }
      case _ => throw new Exception("Not support")
    }
  }
}

class MysqlAddColumnsFunc extends AddColumnsFunc {
  override def sqlParseNeedAdd(originCols: Seq[QualifiedColType]): String = {
    originCols.map(col => {
      col.dataType.toUpperCase match {
        case "TIMESTAMP" =>
          s"IF(payload.op = 'd', TO_TIMESTAMP(payload.before.${col.colName}), TO_TIMESTAMP(payload.after.${col.colName})) + INTERVAL 7 HOURS AS ${col.colName}"
        case "DATETIME" =>
          s"IF(payload.op = 'd', CAST(payload.before.${col.colName} / 1000 AS TIMESTAMP), CAST(payload.after.${col.colName} / 1000 AS TIMESTAMP)) AS ${col.colName}"
        case "DATE" =>
          s"IF(payload.op = 'd', CAST(payload.before.${col.colName} * 86400 AS TIMESTAMP), CAST(payload.after.${col.colName} * 86400 AS TIMESTAMP)) AS ${col.colName}"
        case "TIME" =>
          s"CAST(DATE_FORMAT(CAST(IF(payload.op = 'd', payload.before.`${col.colName}`, payload.after.`${col.colName}`)/1000 - 28800 AS TIMESTAMP), 'HH:mm:ss') AS STRING) as `${col.colName}`"
        case dt if List("DECIMAL", "DOUBLE", "FLOAT", "REAL").contains(dt) =>
          s"CAST(IF(payload.op = 'd', payload.before.${col.colName}, payload.after.${col.colName}) AS DOUBLE) AS ${col.colName}"
        case dt if List("VARCHAR", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "LONGTEXT", "MEDIUMTEXT", "TEXT", "TINYTEXT", "ENUM", "SET", "CHAR", "BINARY", "VARBINARY", "JSON", "CHARACTER").contains(dt) =>
          s"CAST(IF(payload.op = 'd', payload.before.${col.colName}, payload.after.${col.colName}) AS STRING) AS ${col.colName}"
        case dt if dt == "BIGINT" || (dt == "INT") =>
          s"CAST(IF(payload.op = 'd', payload.before.${col.colName}, payload.after.${col.colName}) AS BIGINT) AS ${col.colName}"
        case dt if List("INT", "MEDIUMINT", "YEAR", "INTEGER").contains(dt) || (dt == "SMALLINT") =>
          s"CAST(IF(payload.op = 'd', payload.before.${col.colName}, payload.after.${col.colName}) AS INT) AS ${col.colName}"
        case dt if List("SMALLINT", "TINYINT").contains(dt) =>
          s"CAST(IF(payload.op = 'd', payload.before.${col.colName}, payload.after.${col.colName}) AS $dt) AS ${col.colName}"
        case dt if List("BOOLEAN", "BIT").contains(dt) =>
          s"CAST(IF(payload.op = 'd', payload.before.${col.colName}, payload.after.${col.colName}) AS BOOLEAN) AS ${col.colName}"
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported sql parser for data type ${col.dataType}")
      }
    }).mkString(", \n\t")


  }

  override def sqlAlterBigdataTable(originCols: Seq[QualifiedColType]): String = "???"

  override def convertToSparkField(originCols: Seq[QualifiedColType]): Seq[QualifiedColType] = {
    originCols.map(col => {
      val dataType = col.dataType.toUpperCase match {
        case "TIMESTAMP" => "TIMESTAMP"
        case "DATETIME" => "TIMESTAMP"
        case "DATE" => "TIMESTAMP"
        case "TIME" => "TIMESTAMP"
        case dt if List("DECIMAL", "DOUBLE", "FLOAT", "REAL").contains(dt) => "DOUBLE"
        case dt if List("VARCHAR", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "LONGTEXT", "MEDIUMTEXT", "TEXT", "TINYTEXT", "ENUM", "SET", "CHAR", "BINARY", "VARBINARY", "JSON", "CHARACTER").contains(dt) =>
          "STRING"
        case dt if dt == "BIGINT" || (dt == "INT") => "BIGINT"
        case dt if List("INT", "MEDIUMINT", "YEAR", "INTEGER").contains(dt) || (dt == "SMALLINT") => "INT"
        case "SMALLINT" => "SMALLINT"
        case "TINYINT" => "TINYINT"
        case dt if List("BOOLEAN", "BIT").contains(dt) => "BOOLEAN"
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported sql parser for data type col.dataType")
      }
      QualifiedColType(col.default, dataType = dataType, nullable = col.nullable, default = col.default, comment = col.comment, position = col.position)
    })
  }
}

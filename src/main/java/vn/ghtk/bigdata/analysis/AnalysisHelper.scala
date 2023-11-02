package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.alter.Alter

import scala.collection.JavaConverters.asScalaBufferConverter

object AnalysisHelper {
  def mysqlMappingDatatype(datatype: String): String = {
    if (datatype == "TIMESTAMP") {
      return "TIMESTAMP"
    }
    if (datatype == "DATETIME") {
      return "TIMESTAMP"
    }
    if (datatype == "DATE") {
      return "TIMESTAMP"
    }
    if (datatype == "TIME") {
      return s"CAST(DATE_FORMAT(CAST(IF(payload.op = 'd', payload.before.`$field`, payload.after.`$field`)/1000 - 28800 AS TIMESTAMP), 'HH:mm:ss') AS STRING) as `$field`"
    }
    if (Seq("DECIMAL", "DOUBLE", "FLOAT", "REAL").contains(datatype)) {
      return s"CAST(IF(payload.op = 'd', payload.before.$field, payload.after.$field) AS DOUBLE) AS $field"
    }
    if (Seq("VARCHAR", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "LONGTEXT", "MEDIUMTEXT", "TEXT", "TINYTEXT", "ENUM", "SET", "CHAR", "BINARY", "VARBINARY", "JSON", "CHARACTER").contains(datatype)) {
      return s"CAST(IF(payload.op = 'd', payload.before.$field, payload.after.$field) AS STRING) AS $field"
    }
    if ((datatype == "INT" && isUnsigned) || datatype == "BIGINT") {
      return s"CAST(IF(payload.op = 'd', payload.before.$field, payload.after.$field) AS BIGINT) AS $field"
    }
    if ((datatype == "SMALLINT" && isUnsigned) || Seq("INT", "MEDIUMINT", "YEAR", "INTEGER").contains(datatype)) {
      return s"CAST(IF(payload.op = 'd', payload.before.$field, payload.after.$field) AS INT) AS $field"
    }
    if (Seq("SMALLINT", "TINYINT").contains(datatype)) {
      return s"CAST(IF(payload.op = 'd', payload.before.$field, payload.after.$field) AS $datatype) AS $field"
    }
    if (Seq("BOOLEAN", "BIT").contains(datatype)) {
      return s"CAST(IF(payload.op = 'd', payload.before.$field, payload.after.$field) AS BOOLEAN) AS $field"
    }

    return ""
  }
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
          AddColumns(statement.getTable, cols)
        } else {
          throw new Exception("Table not alter")
        }
      }
      case _ => throw new Exception("Not support")
    }
  }
}

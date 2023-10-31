package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.alter.Alter

import scala.collection.JavaConverters.asScalaBufferConverter

object AnalysisHelper {
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

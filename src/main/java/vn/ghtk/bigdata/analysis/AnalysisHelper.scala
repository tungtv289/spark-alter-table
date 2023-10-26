package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.alter.Alter

import scala.collection.JavaConverters.asScalaBufferConverter

object AnalysisHelper {
  def resolveOperatorsUp(statement: Statement): Command = {
    statement match {
      case statement: Alter => {
        val bigdataTbl = statement.getTable.getFullyQualifiedName.replace("`", "")
        println(bigdataTbl)
        //        check bigdataTbl is conf  ig alter
        val isAlter = true
        if (isAlter) {
          val expressions = statement.getAlterExpressions.asScala.toList
          val cols = Seq[QualifiedColType]()
          expressions.foreach(expression => {
            if(expression.getColDataTypeList != null && expression.getColDataTypeList.size() == 1) {
              val cdt = expression.getColDataTypeList.get(0)
              cols :+ QualifiedColType(cdt.getColumnName, cdt.getColDataType.toString, nullable = true, "", Some(""), Some(""))
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

package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.schema.Table

trait AlterTableCommands extends Command {
  def table: Table

  def func: UtilsFunc
}

trait UtilsFunc

trait AddColumnsFunc extends UtilsFunc {
  def sqlParseNeedAdd(originCols: Seq[QualifiedColType]): String

  def sqlAlterBigdataTable(originCols: Seq[QualifiedColType]): String

  def convertToSparkField(originCols: Seq[QualifiedColType]): Seq[QualifiedColType]
}

case class AddColumns(table: Table, func: AddColumnsFunc, private val columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommands {
  def getSparkFieldToAdd: Seq[QualifiedColType] = func.convertToSparkField(columnsToAdd)

  def getSqlAlterBigdataTable: String = func.sqlAlterBigdataTable(columnsToAdd)

  def getSqlParseNeedAdd: String = func.sqlParseNeedAdd(columnsToAdd)
}

abstract case class RenameColumn(table: Table) extends AlterTableCommands {}

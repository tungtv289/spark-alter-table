package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.schema.Table

trait AlterTableCommands extends Command {
  def genSparkSql: String
  def table: Table
}


case class AddColumns(table: Table, columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommands {
  override def genSparkSql: String = ""
}
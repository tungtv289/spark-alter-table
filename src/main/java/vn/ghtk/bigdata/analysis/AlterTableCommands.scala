package vn.ghtk.bigdata.analysis

import net.sf.jsqlparser.schema.Table

trait AlterTableCommands extends Command {
  def table: Table
}


case class AddColumns(table: Table, columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommands {
  def genSparkSql: String = {
    val tableFullName = table.getFullyQualifiedName
    s"ALTER TABLE $tableFullName " + columnsToAdd.map(col => s"ADD COLUMN ${col.colName} ${col.dataType}").mkString(",")
  }
}

case class RenameColumn(table: Table) extends AlterTableCommands {
}
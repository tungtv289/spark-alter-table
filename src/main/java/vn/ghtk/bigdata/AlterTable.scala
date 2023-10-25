package vn.ghtk.bigdata

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.alter.Alter

import scala.collection.JavaConverters._


object AlterTable {
  def main(args: Array[String]): Unit = {
    //    val sqlStr = "ALTER TABLE `inter`.`shop_demand_detail_jobs` \nMODIFY COLUMN `created` datetime NULL DEFAULT CURRENT_TIMESTAMP AFTER `job_type`";
    //    val sqlStr = "ALTER TABLE `xteam`.`_xteam_exported_routes_new` ADD COLUMN `pick_time_from` datetime DEFAULT NULL COMMENT \"Mốc bắt đầu thời gian hẹn lấy\", ADD COLUMN `pick_time_to` datetime DEFAULT NULL COMMENT \"Mốc kết thúc thời gian hẹn lấy\"";
    val sqlStr =
    """
      |alter table bigdata.sqoop_transform_config change user_id responsible_users varchar(50) default 'quyvc' not null AFTER impala_db
      |""".stripMargin
    CCJSqlParserUtil.parse(sqlStr) match {
      case statement: Alter => {
        val bigdataTbl = statement.getTable.getFullyQualifiedName.replace("`", "")
        println(bigdataTbl)
        //        check bigdataTbl is conf  ig alter
        val isAlter = true
        if (isAlter) {
          val expressions = statement.getAlterExpressions.asScala.toList
          expressions.foreach(expression => {
            println(expression.getOperation)
          })
          println("a")
        } else {
          throw new Exception("Table not alter")
        }
      }
      case _ => throw new Exception("Not support")
    }
  }
}

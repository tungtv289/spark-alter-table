package vn.ghtk.bigdata

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.alter.Alter
import vn.ghtk.bigdata.analysis.{AddColumns, AnalysisHelper}

import scala.collection.JavaConverters._


object AlterTable {
  def main(args: Array[String]): Unit = {
    //    val sqlStr = "ALTER TABLE `inter`.`shop_demand_detail_jobs` \nMODIFY COLUMN `created` datetime NULL DEFAULT CURRENT_TIMESTAMP AFTER `job_type`";
    //    val sqlStr = "ALTER TABLE `xteam`.`_xteam_exported_routes_new` ADD COLUMN `pick_time_from` datetime DEFAULT NULL COMMENT \"Mốc bắt đầu thời gian hẹn lấy\", ADD COLUMN `pick_time_to` datetime DEFAULT NULL COMMENT \"Mốc kết thúc thời gian hẹn lấy\"";
    val sqlStr =
    """
      |ALTER TABLE payment_transactions
      |    ADD INDEX idx_link_id_amount (`link_id`, `amount`),
      |    ADD COLUMN `cip`    VARCHAR(39) DEFAULT NULl COMMENT 'Client IP' AFTER `frm`,
      |    ADD COLUMN `source` TINYINT NOT NULL DEFAULT 0 COMMENT '0: app, 1: web' AFTER `frm`,
      |    ADD link_id BIGINT not null comment 'id bảng web_payment_links' default 0 after user_id
      |""".stripMargin
    val stm = CCJSqlParserUtil.parse(sqlStr)
    AnalysisHelper.resolveOperatorsUp(stm) match {
      case stm: AddColumns => AlterTableAddColumnsExecute(stm)
    }
  }
}

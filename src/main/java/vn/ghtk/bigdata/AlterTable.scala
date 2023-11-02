package vn.ghtk.bigdata

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vn.ghtk.bigdata.analysis.{AddColumns, AnalysisHelper, RenameColumn}
import vn.ghtk.bigdata.utils.ArgumentsParseUtils.getArguments


object AlterTable {
  def main(args: Array[String]): Unit = {
    //    val sqlStr = "ALTER TABLE `inter`.`shop_demand_detail_jobs` \nMODIFY COLUMN `created` datetime NULL DEFAULT CURRENT_TIMESTAMP AFTER `job_type`";
    //    val sqlStr = "ALTER TABLE `xteam`.`_xteam_exported_routes_new` ADD COLUMN `pick_time_from` datetime DEFAULT NULL COMMENT \"Mốc bắt đầu thời gian hẹn lấy\", ADD COLUMN `pick_time_to` datetime DEFAULT NULL COMMENT \"Mốc kết thúc thời gian hẹn lấy\"";
    val arguments = getArguments(args)
    val ddlStatement = arguments("ddlStatement")
    val alterTimestamp = arguments("alterTimestamp").toLong
    val dbType = arguments("dbType")

    val sqlStr = """
      |ALTER TABLE payment_transactions
      |    ADD INDEX idx_link_id_amount (`link_id`, `amount`),
      |    ADD COLUMN `cip`    VARCHAR(39) DEFAULT NULl COMMENT 'Client IP' AFTER `frm`,
      |    ADD COLUMN `report_type` tinyint(4) DEFAULT '0' COMMENT '0: bc co ban\n1: bc dinh ky',
      |    ADD COLUMN `created_username` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'dunghtk2' COMMENT 'Tên người tạo chỉ số',
      |    ADD COLUMN `source` DECIMAL(13,4) NOT NULL DEFAULT 0 COMMENT '0: app, 1: web' AFTER `frm`,
      |    ADD link_id BIGINT not null comment 'id bảng web_payment_links' default 0 after user_id
      |""".stripMargin
    val stm = CCJSqlParserUtil.parse(ddlStatement)
    val execute = AnalysisHelper.resolveOperatorsUp(stm) match {
      case stm: AddColumns => AlterTableAddColumnsExecute(stm, alterTimestamp)
      case stm: RenameColumn => AlterTableRenameColumnsExecute()
    }

    val sparkConf = new SparkConf()
    val spark = SparkSession.builder().config(sparkConf)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    execute.run(spark)
  }
}

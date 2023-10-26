package vn.ghtk.bigdata.analysis

case class QualifiedColType(
                             colName: String,
                             dataType: String,
                             nullable: Boolean,
                             default: String,
                             comment: Option[String],
                             position: Option[String]
                           )

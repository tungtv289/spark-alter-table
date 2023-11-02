package vn.ghtk.bigdata.utils

object ArgumentsParseUtils {

  val argParser = new scopt.OptionParser[Arguments]("Parsing application") {
    opt[String]("ddl-statement")
      .valueName("")
      .action((value, arguments) => arguments.copy(ddlStatement = value))

    opt[String]("event-timestamp").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(eventTimestamp = value))

    opt[String]("jdbc-type").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(jdbcType = value))

  }

  def getArguments(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    val arguments: scala.collection.mutable.Map[String, String] = collection.mutable.HashMap.empty[String, String]

    var ddlStatement: String = null
    var eventTimestamp: String = "0"
    var jdbcType: String = null

    argParser.parse(args, Arguments()) match {
      case Some(arguments) =>
        ddlStatement = arguments.ddlStatement
        eventTimestamp = arguments.eventTimestamp
        jdbcType = arguments.jdbcType
      case None => throw new Exception("Invalid input arguments!!!")
    }
    arguments += ("ddlStatement" -> ddlStatement)
    arguments += ("eventTimestamp" -> eventTimestamp)
    arguments += ("jdbcType" -> jdbcType)
    arguments
  }

  case class Arguments(ddlStatement: String = "",
                       eventTimestamp: String = "0",
                       jdbcType: String = "")

}

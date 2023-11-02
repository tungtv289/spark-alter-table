package vn.ghtk.bigdata.utils

object ArgumentsParseUtils {

  case class Arguments(ddlStatement: String = "",
                       alterTimestamp: String = "0",
                       dbType: String = "")

  val argParser = new scopt.OptionParser[Arguments]("Parsing application") {
    opt[String]("ddl-statement")
      .valueName("")
      .action((value, arguments) => arguments.copy(ddlStatement = value))

    opt[String]("alter-timestamp").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(alterTimestamp = value))

    opt[String]("db-type").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(dbType = value))

  }

  def getArguments(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    val arguments: scala.collection.mutable.Map[String, String] = collection.mutable.HashMap.empty[String, String]

    var ddlStatement: String = null
    var alterTimestamp: String = "0"
    var dbType: String = null

    argParser.parse(args, Arguments()) match {
      case Some(arguments) =>
        ddlStatement = arguments.ddlStatement
        alterTimestamp = arguments.alterTimestamp
        dbType = arguments.dbType
      case None => throw new Exception("Invalid input arguments!!!")
    }
    arguments += ("ddlStatement" -> ddlStatement)
    arguments += ("alterTimestamp" -> alterTimestamp)
    arguments += ("dbType" -> dbType)
    arguments
  }

}

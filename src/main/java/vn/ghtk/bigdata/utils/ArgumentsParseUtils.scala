package vn.ghtk.bigdata.utils

object ArgumentsParseUtils {
  case class ETLArguments(appName: String = "",
                          keyLocation: String = "",
                          jdbcUrl: String = "",
                          username: String = "",
                          password: String = "",
                         )

  val etlParser = new scopt.OptionParser[ETLArguments]("Parsing application") {
    opt[String]("app-name").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(appName = value))

    opt[String]("key-location")
      .valueName("")
      .action((value, arguments) => arguments.copy(keyLocation = value))

    opt[String]("jdbc-url").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(jdbcUrl = value))

    opt[String]('u', "username").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(username = value))

    opt[String]('p', "password").required()
      .valueName("")
      .action((value, arguments) => arguments.copy(password = value))

  }

  def getETLArguments(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    val arguments: scala.collection.mutable.Map[String, String] = collection.mutable.HashMap.empty[String, String]


    var jdbcUrl: String = null
    var keyLocation: String = null
    var appName: String = null
    var username: String = null
    var password: String = null


    etlParser.parse(args, ETLArguments()) match {
      case Some(arguments) =>
        appName = arguments.appName
        keyLocation = arguments.keyLocation
        jdbcUrl = arguments.jdbcUrl
        username = arguments.username
        password = arguments.password
      case None => throw new Exception("Invalid input arguments!!!")
    }

    arguments += ("appName" -> appName)
    arguments += ("keyLocation" -> keyLocation)
    arguments += ("jdbcUrl" -> jdbcUrl)
    arguments += ("username" -> username)
    arguments += ("password" -> password)

    arguments
  }

}

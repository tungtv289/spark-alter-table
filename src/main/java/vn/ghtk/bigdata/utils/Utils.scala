package vn.ghtk.bigdata.utils

import scala.io.Source

object Utils {
  def getPrivateKey(keyLocation: String): String = {
    Source.fromFile(keyLocation).getLines.mkString
  }

}

package vn.ghtk.bigdata.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable.ListBuffer

object HdfsFileUtils {
  val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")

  def getPossiblePath(basePath: String, checkpointHdfsModified: Long): List[String] = {
    val res: ListBuffer[String] = new ListBuffer[String]()

    val timeZone = TimeZone.getTimeZone("Asia/Ho_Chi_Minh")
    val calendar = Calendar.getInstance(timeZone)
    calendar.set(Calendar.HOUR_OF_DAY, 23)
    calendar.set(Calendar.MINUTE, 59)
    calendar.set(Calendar.SECOND, 59)
    calendar.set(Calendar.MILLISECOND, 999)
    val endOfTheDayMillis = calendar.getTimeInMillis

    var currentCheckpoint = checkpointHdfsModified
    while (currentCheckpoint < endOfTheDayMillis) {
      val dateDateKey = simpleDateFormat.format(currentCheckpoint)
      res += f"$basePath/data_date_key=$dateDateKey"
      currentCheckpoint += 86400000
    }
    res.toList
  }

  /*
  Only support one level partition
   */
  def getFilePathFromPartition(basePath: String, partitionFormat: String, partitions: Array[String]): List[String] = {
    val res: ListBuffer[String] = new ListBuffer[String]()
    for (partition <- partitions) {
      res += f"$basePath/$partitionFormat=$partition"
    }
    res.toList
  }

  def getExistedHdfsFolder(folderPaths: List[String]): List[String] = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    folderPaths.filter(
      folderPath => {
        fs.exists(new Path(folderPath))
      }
    )
  }

  def checkFolderEmpty(folderPaths: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    if (fs.exists(new Path(folderPaths))) {
      val isEmpty = fs.listStatus(new Path(folderPaths)).isEmpty
      return isEmpty
    }
    true
  }

  def getListFileModifiedAfterCheckpoint(baseFilePath: String, checkpointHdfsModified: Long, isFromSnapshot: Boolean): List[String] = {
    if (isFromSnapshot && checkpointHdfsModified > 0) {
      return Nil
    }
    getExistedHdfsFolder(getPossiblePath(baseFilePath, checkpointHdfsModified))
  }
}

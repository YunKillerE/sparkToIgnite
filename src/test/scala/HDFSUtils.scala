package scala

import com.yunchen.utils.hdfsUtls
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

class HDFSUtils {

  def start(args: Array[String]): Unit = {

    new hdfsUtls("10.10.10.132:8020")

    val hdfs : FileSystem = FileSystem.get(new Configuration)

    args(0) match {
      case "list" => traverse(args(1))
      case "createFile" => hdfsUtls.createFile(args(1))
      case "createFolder" => hdfsUtls.createFolder(args(1))
      case "copyfile" => hdfsUtls.copyFile(args(1), args(2))
      case "copyfolder" => hdfsUtls.copyFolder(args(1), args(2))
      case "delete" => hdfsUtls.deleteFile(args(1))
      case "copyfilefrom" => hdfsUtls.copyFileFromLocal(args(1), args(2))
      case "copyfileto" => hdfsUtls.copyFileToLocal(args(1), args(2))
      case "copyfolderfrom" => hdfsUtls.copyFolderFromLocal(args(1), args(2))
      case "copyfolderto" => hdfsUtls.copyFolderToLocal(args(1), args(2))
    }
  }

  def traverse(hdfsPath : String) = {
    val holder : ListBuffer[String] = new ListBuffer[String]
    val paths : List[String] = hdfsUtls.listChildren(hdfsPath, holder).toList
    for(path <- paths){
      System.out.println("--------- path = " + path)
      System.out.println("--------- Path.getname = " + new Path(path).getName)
    }
  }

}

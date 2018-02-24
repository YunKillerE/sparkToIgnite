package com.yunchen.utils

import java.io.{IOException, FileSystem => _, _}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.mutable.ListBuffer


class hdfsUtls() {

  def this(hdfs_address: String) {
    this()
    hdfsUtls.conf.set("fs.defaultFS", hdfs_address)
  }

  def this(hdfs_address: String, krbFilePath: String = "/etc/krb5.conf", krbRelams : String = "CLOUDERA",
           kerberosUser: String = "bigf/bigf", kerberosKeyPath: String = "/home/bigf/bigf.keytab") {
    this(hdfs_address)
    hdfsUtls.conf.set("hadoop.security.authentication", "kerberos")
    hdfsUtls.conf.set("java.security.krb5.conf", krbFilePath)
    hdfsUtls.conf.set("java.security.krb5.relams", krbRelams)
    try {
      UserGroupInformation.loginUserFromKeytab(kerberosUser, kerberosKeyPath)
    } catch {
      case ex: IOException => {
        ex.printStackTrace()
      }
    }
  }
}

class MyPathFilter extends PathFilter {
  override def accept(path: Path): Boolean = true
}

object hdfsUtls {
  private[utils] val conf = new Configuration
  System.setProperty("HADOOP_USER_NAME", "hdfs")
  val hdfs, fileSystem = FileSystem.get(conf)

  /**
    * 关闭FileSystem
    *
    * @param fileSystem
    */
  def closeFS(fileSystem: FileSystem) {
    if (fileSystem != null) {
      try {
        fileSystem.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  /**
    * 获取文件列表
    *
    * @param path
    * @return
    */

  def getAllFileName(path: String) = {
    val hdfs = FileSystem.get(URI.create(path), conf)
    val fs = hdfs.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)
    var list = List[String]()
    for (p <- listPath) {
      System.out.println(p)
      if (isDirectory(p.toString) == false)
        list +: p.toString
    }
    list
  }

  /**
    * 过滤掉目录
    *
    * @param path
    * @return
    */
  def isDirectory(path: String): Boolean = { //Configuration conf = new Configuration();
    val hdfs = FileSystem.get(URI.create(path), conf)
    val fs = hdfs.listStatus(new Path(path))
    val paths = FileUtil.stat2Paths(fs)
    var bool = false
    for (p <- paths) {
      val fileFS = FileSystem.get(URI.create(p.toString), conf)
      val fileStatus = fileFS.getFileStatus(p)
      bool = fileStatus.isDirectory
    }
    bool
  }

  def isDirectoryEmety(path: String): Boolean = {
    val hdfs = FileSystem.get(URI.create(path), conf)
    val fs = hdfs.listStatus(new Path(path))
    var abc = true
    if (fs.length == 0) abc = false
    abc
  }

  /**
    * get file size
    */
  def getBlockSize(path: String) = {
    val hdfs = FileSystem.get(URI.create(path), conf)
    val fs = hdfs.getFileStatus(new Path(path))
    val fileSize = fs.getBlockSize
    fileSize
  }

  def getFileSize(path:String) = {
    val hdfs = FileSystem.get(URI.create(path), conf)
    val size = hdfs.getContentSummary(new Path(path)).getLength()
    size
  }

  /**
    * get file content
    */
  def getFileContent(path: String):String = {
    val inStream = fileSystem.open(new Path(path))
    var content = ""
    val  bos = new ByteArrayOutputStream()
    try {
      IOUtils.copyBytes(inStream, bos, 4096, false)
      val result = bos.toByteArray
      content = new String(result)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    content
  }

  def ls(fileSystem: FileSystem, path: String) = {
    println("list path:" + path)
    val fs = fileSystem.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)
    for (p <- listPath) {
      println(p)
    }
    println("----------------------------------------")
  }

  /**
    * 创建目录
    *
    * @param hdfsFilePath
    */
  def mkdir(hdfsFilePath: String) = {
    try {
      val success = fileSystem.mkdirs(new Path(hdfsFilePath))
      if (success) {
        println("Create directory or file successfully")
      }
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }

  /**
    * 删除文件或目录
    *
    * @param hdfsFilePath
    * @param recursive 递归
    */
  def rm(hdfsFilePath: String, recursive: Boolean): Unit = {
    val path = new Path(hdfsFilePath)
    try {
      if (fileSystem.exists(path)) {
        val success = fileSystem.delete(path, recursive)
        if (success) {
          System.out.println("delete successfully")
        }
      }
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }

  /**
    * 上传文件到HDFS
    *
    * @param localPath
    * @param hdfspath
    */
  def write(localPath: String, hdfspath: String) {

    val inStream = new FileInputStream(
      new File(localPath)
    )
    val writePath = new Path(hdfspath)
    val outStream = hdfsUtls.fileSystem.create(writePath)

    try {
      IOUtils.copyBytes(inStream, outStream, 4096, false)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inStream)
      IOUtils.closeStream(outStream)
    }
  }

  def cat(hdfsFilePath: String) {
    val readPath = new Path(hdfsFilePath)
    val inStream = fileSystem.open(readPath)

    try {
      IOUtils.copyBytes(inStream, System.out, 4096, false)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inStream)
    }
  }

  def getFileSystem: FileSystem = {
    val fileSystem = FileSystem.get(conf)
    return fileSystem
  }

  def isDir(name: String): Boolean = {
    hdfs.isDirectory(new Path(name))
  }

  def isDir(name: Path): Boolean = {
    hdfs.isDirectory(name)
  }

  def isFile(name: String): Boolean = {
    hdfs.isFile(new Path(name))
  }

  def isFile(name: Path): Boolean = {
    hdfs.isFile(name)
  }

  def createFile(name: String): Boolean = {
    hdfs.createNewFile(new Path(name))
  }

  def createFile(name: Path): Boolean = {
    hdfs.createNewFile(name)
  }

  def createFolder(name: String): Boolean = {
    hdfs.mkdirs(new Path(name))
  }

  def createFolder(name: Path): Boolean = {
    hdfs.mkdirs(name)
  }

  def exists(name: String): Boolean = {
    hdfs.exists(new Path(name))
  }

  def exists(name: Path): Boolean = {
    hdfs.exists(name)
  }

  def transport(inputStream: InputStream, outputStream: OutputStream): Unit = {
    val buffer = new Array[Byte](64 * 1000)
    var len = inputStream.read(buffer)
    while (len != -1) {
      outputStream.write(buffer, 0, len - 1)
      len = inputStream.read(buffer)
    }
    outputStream.flush()
    inputStream.close()
    outputStream.close()
  }

  /**
    * create a target file and provide parent folder if necessary
    */
  def createLocalFile(fullName: String): File = {
    val target: File = new File(fullName)
    if (!target.exists) {
      val index = fullName.lastIndexOf(File.separator)
      val parentFullName = fullName.substring(0, index)
      val parent: File = new File(parentFullName)

      if (!parent.exists)
        parent.mkdirs
      else if (!parent.isDirectory)
        parent.mkdir

      target.createNewFile
    }
    target
  }

  /**
    * delete file in hdfs
    *
    * @return true: success, false: failed
    */
  def deleteFile(path: String): Boolean = {
    if (isDir(path))
      hdfs.delete(new Path(path), true) //true: delete files recursively
    else
      hdfs.delete(new Path(path), false)
  }

  /**
    * get all file children's full name of a hdfs dir, not include dir children
    *
    * @param fullName the hdfs dir's full name
    */
  def listChildren(fullName: String, holder: ListBuffer[String]): ListBuffer[String] = {
    val filesStatus = hdfs.listStatus(new Path(fullName), new MyPathFilter)
    for (status <- filesStatus) {
      val filePath: Path = status.getPath
      if (isFile(filePath))
        holder += filePath.toString
      else
        listChildren(filePath.toString, holder)
    }
    holder
  }

  def copyFile(source: String, target: String): Unit = {

    val sourcePath = new Path(source)
    val targetPath = new Path(target)

    if (!exists(targetPath))
      createFile(targetPath)

    val inputStream: FSDataInputStream = hdfs.open(sourcePath)
    val outputStream: FSDataOutputStream = hdfs.create(targetPath)
    transport(inputStream, outputStream)
  }

  def copyFolder(sourceFolder: String, targetFolder: String): Unit = {
    val holder: ListBuffer[String] = new ListBuffer[String]
    val children: List[String] = listChildren(sourceFolder, holder).toList
    for (child <- children)
      copyFile(child, child.replaceFirst(sourceFolder, targetFolder))
  }

  def copyFileFromLocal(localSource: String, hdfsTarget: String): Unit = {
    val targetPath = new Path(hdfsTarget)
    if (!exists(targetPath))
      createFile(targetPath)

    val inputStream: FileInputStream = new FileInputStream(localSource)
    val outputStream: FSDataOutputStream = hdfs.create(targetPath)
    transport(inputStream, outputStream)
  }

  def copyFileToLocal(hdfsSource: String, localTarget: String): Unit = {
    val localFile: File = createLocalFile(localTarget)

    val inputStream: FSDataInputStream = hdfs.open(new Path(hdfsSource))
    val outputStream: FileOutputStream = new FileOutputStream(localFile)
    transport(inputStream, outputStream)
  }

  def copyFolderFromLocal(localSource: String, hdfsTarget: String): Unit = {
    val localFolder: File = new File(localSource)
    val allChildren: Array[File] = localFolder.listFiles
    for (child <- allChildren) {
      val fullName = child.getAbsolutePath
      val nameExcludeSource: String = fullName.substring(localSource.length)
      val targetFileFullName: String = hdfsTarget + Path.SEPARATOR + nameExcludeSource
      if (child.isFile)
        copyFileFromLocal(fullName, targetFileFullName)
      else
        copyFolderFromLocal(fullName, targetFileFullName)
    }
  }

  def copyFolderToLocal(hdfsSource: String, localTarget: String): Unit = {
    val holder: ListBuffer[String] = new ListBuffer[String]
    val children: List[String] = listChildren(hdfsSource, holder).toList
    val hdfsSourceFullName = hdfs.getFileStatus(new Path(hdfsSource)).getPath.toString
    val index = hdfsSourceFullName.length
    for (child <- children) {
      val nameExcludeSource: String = child.substring(index + 1)
      val targetFileFullName: String = localTarget + File.separator + nameExcludeSource
      copyFileToLocal(child, targetFileFullName)
    }
  }

}

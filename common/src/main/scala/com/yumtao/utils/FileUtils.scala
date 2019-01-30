package com.yumtao.utils

import java.io._

import org.apache.commons.io.IOUtils

import scala.io.Source
import scala.tools.nsc.io.JFile

/**
  * Created by yumtao on 2019/1/17.
  */
object FileUtils {

  /**
    * 获取resources文件夹下的文件路径
    *
    * @param name
    */
  def getPathInProject(name: String): String = {
    val url = this.getClass.getClassLoader.getResource("")
    url.getPath + name
  }

  def encryptFile(filePath: String) = fileSecurity(filePath)(true)

  def decryptFile(filePath: String) = fileSecurity(filePath)(false)

  private def fileSecurity(filePath: String)(flag: Boolean): Unit = {
    val tempPath = getPathInProject("tmp")

    val file: JFile = new JFile(filePath)
    val fileName = file.getName

    // 文件加密放入临时路径
    // 1. 读取文件，进行加密，存储至临时文件夹下
    val encryptContent = Source.fromFile(file).getLines().map(line => {
      if (flag) DesUtils.encryptBasedDes(line) else DesUtils.decryptBasedDes(line)
    }).toList

    val out = new PrintWriter(tempPath + fileName)

    var lineNum: Int = 0
    val size = encryptContent.size
    encryptContent.foreach(line => {
      lineNum += 1
      if (lineNum < size) out.println(line) else out.print(line)
    })
    //    out.flush()
    out.close()


    // 临时路径移动至源文件路径
    moveFile(tempPath + fileName, filePath)
  }

  /**
    * 拷贝文件
    *
    * @param inputPath  被复制文件路径
    * @param outputPath 文件复制后输出路径
    * @param delSrc     是否删除源文件
    */
  def copyFile(inputPath: String, outputPath: String)(delSrc: Boolean = false) = {
    val file: JFile = new JFile(inputPath)
    val input: InputStream = new FileInputStream(file)
    val output: OutputStream = new FileOutputStream(outputPath)
    IOUtils.copyLarge(input, output)

    input.close()
    output.close()
    if (delSrc) file.deleteOnExit()
  }

  /**
    * 移动文件
    */
  def moveFile(inputPath: String, outputPath: String) = {
    copyFile(inputPath, outputPath)(true)
  }

}

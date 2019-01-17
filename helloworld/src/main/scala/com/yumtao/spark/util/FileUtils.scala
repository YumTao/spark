package com.yumtao.spark.util

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
}

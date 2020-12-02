package com.wnswdwy.utils
import java.io.InputStreamReader
import java.util.Properties
/**
 * @author yycstart
 * @create 2020-12-01 20:37
 */


object PropertiesUtil {

  def load(propertieName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }

}


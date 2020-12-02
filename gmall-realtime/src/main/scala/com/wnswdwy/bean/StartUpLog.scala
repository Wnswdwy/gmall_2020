package com.wnswdwy.bean

/**
 * @author yycstart
 * @create 2020-12-01 20:59
 */
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      `type`: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long)

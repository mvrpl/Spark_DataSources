package models

import java.net.HttpCookie

case class ApiExtractorConf (
    headers: Map[String, String] = Map.empty[String, String],
    data: Map[String, String] = Map.empty[String, String],
    cookies: Map[String, HttpCookie] = Map.empty[String, HttpCookie],
    params: Map[String, String] = Map.empty[String, String],
    endpoint: String = "",
    requestMethod: String = "GET",
    paginatorAttr: Map[String, String] = Map.empty[String, String],
    paginator: Map[String, String] = Map.empty[String, String],
    connectTimeoutMili: Int = 0,
    readTimeoutMili: Int = 0,
    autoDecompress: Boolean = true,
    numPartitions: Int = 200,
    numValues: Int = 2147483647,
    waitMili: Long = 1001,
    waitChanSec: Int = 10
)
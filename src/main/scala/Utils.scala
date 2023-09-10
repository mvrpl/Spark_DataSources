package utils

import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

import nl.altindag.ssl.SSLFactory
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, Formats}
import com.jayway.jsonpath.JsonPath

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.Try
import scala.reflect.ClassTag

import java.util.concurrent.{TimeUnit, LinkedTransferQueue}
import java.nio.file.Paths
import java.net.HttpCookie
import java.security.SecureRandom
import javax.net.ssl.SSLContext

object Eval {
  def apply[A](string: String): A = {
    val toolbox = currentMirror.mkToolBox()
    val tree = toolbox.parse(string)
    toolbox.eval(tree).asInstanceOf[A]
  }
}

object UtilFuncs {
  def ccToMap(cc: AnyRef) = (Map[String, Any]() /: cc.getClass.getDeclaredFields) {
    (a, f) =>
    f.setAccessible(true)
    a + (f.getName -> f.get(cc))
  }

  def buildSSLCtx(conf: Map[String, String]): SSLContext = {
    val sslFactory = SSLFactory.builder()
    conf.foldLeft(sslFactory){case (acc: SSLFactory.Builder, act: (String, String)) => 
      act._1 match {
        case "ciphers" => acc.withCiphers(act._2.split("::"):_*)
        case "protocols" => acc.withProtocols(act._2.split("::"):_*)
        case "identityMaterial" => acc.withIdentityMaterial(
          Paths.get(act._2), conf.getOrElse("identityPassword", "").toCharArray, "jks"
        )
        case "trustMaterial" => acc.withTrustMaterial(
          Paths.get(act._2), conf.getOrElse("trustPassword", "").toCharArray, "jks"
        )
        case "secureRandom" => acc.withSecureRandom(new SecureRandom(act._2.getBytes))
      }
    }
    sslFactory.build().getSslContext
  }

  def mkRequest(
      conf: models.ApiExtractorConf,
      url: String,
      isFirst: Boolean,
      chan: LinkedTransferQueue[String]
  ): Row = {
    var newConf = conf

    implicit val formats: Formats = DefaultFormats

    if (!isFirst) {
      val incomeChan = chan.poll(newConf.waitChanSec, TimeUnit.SECONDS)
      if (incomeChan == null) {
        return Row(null)
      }
      newConf = JsonMethods.parse(incomeChan).noNulls.extract[models.ApiExtractorConf]
    }

    val fullUrl = s"""${url}/${newConf.endpoint.stripPrefix("/")}"""

    var r: requests.Response = null

    try {
      r = requests.send(newConf.requestMethod)(
        fullUrl,
        headers = newConf.headers,
        data = newConf.data,
        cookies = newConf.cookies.map{case (k: String, v: String) =>
          k -> HttpCookie.parse(v).get(0)
        },
        params = newConf.params,
        readTimeout = newConf.readTimeoutMili,
        connectTimeout = newConf.connectTimeoutMili,
        compress = newConf.compress.toLowerCase match {
          case "gzip" => requests.Compress.Gzip
          case "deflate" => requests.Compress.Deflate
          case _ => requests.Compress.None
        },
        autoDecompress = newConf.autoDecompress,
        maxRedirects = newConf.maxRedirects,
        verifySslCerts = newConf.verifySslCerts,
        sslContext = buildSSLCtx(newConf.sslContext),
        proxy = newConf match {
          case pc if pc.proxyHost != null && pc.proxyPort != 0 => (pc.proxyHost, pc.proxyPort)
          case _ => null
        }
      )
    } catch {
      case e: requests.RequestFailedException => {
        println(e.getMessage)
        val jsonMsg = Serialization.write(newConf)
        Thread.sleep(newConf.waitMili)
        chan.put(jsonMsg)
        return Row(null)
      }
      case e: Exception => throw e
    }

    val result = r.text

    var hasNext = false

    if (!newConf.paginator.isEmpty) {
      val keyPage = newConf.paginator.getOrElse("key", "$")
      val paginator = Try(JsonPath.read[Any](result, keyPage).toString).getOrElse("null")

      hasNext = Eval[Boolean](
        s""""${paginator}" ${newConf.paginator.get("validationFilter").get}"""
      )

      if (hasNext) {
        val reqLoc = ccToMap(newConf).get(newConf.paginatorAttr.get("reqLocation").get).get match {
          case rl if rl.isInstanceOf[String] => Map("endpoint" -> rl.asInstanceOf[String])
          case rl if rl.isInstanceOf[Map[String, String]] => rl.asInstanceOf[Map[String, String]]
          case _ => throw new Exception("paginatorAttr.reqLocation only 'string|map<string,string>' available")
        }
        val actualVal = reqLoc.getOrElse(newConf.paginatorAttr.get("attrName").get, "")
        val newVal = newConf.paginatorAttr.get("type").get match {
          case "increment" => (actualVal.toLong + newConf.paginatorAttr.getOrElse("incStep", "1").toLong).toString
          case "value_inc" => (paginator.toLong + newConf.paginatorAttr.getOrElse("incStep", "1").toLong).toString
          case "value" => paginator
          case "eval" => Eval[String](s""""${actualVal}"${newConf.paginatorAttr.getOrElse("eval", "")}""")
          case _ => throw new Exception("paginatorAttr.type only 'increment|value|value_inc|eval' available")
        }
        val objConfs = newConf.copy(
          headers = newConf.paginatorAttr.get("reqLocation").get match {
            case "headers" => newConf.headers ++ Map(newConf.paginatorAttr.get("attrName").get -> newVal)
            case _ => newConf.headers
          },
          data = newConf.paginatorAttr.get("reqLocation").get match {
            case "data" => newConf.data ++ Map(newConf.paginatorAttr.get("attrName").get -> newVal)
            case _ => newConf.data
          },
          params = newConf.paginatorAttr.get("reqLocation").get match {
            case "params" => newConf.params ++ Map(newConf.paginatorAttr.get("attrName").get -> newVal)
            case _ => newConf.params
          },
          cookies = newConf.paginatorAttr.get("reqLocation").get match {
            case "cookies" => newConf.cookies ++ Map(newConf.paginatorAttr.get("attrName").get -> newVal)
            case _ => newConf.cookies
          },
          endpoint = newConf.paginatorAttr.get("reqLocation").get match {
            case "endpoint" => newVal
            case _ => newConf.endpoint
          }
        )
        val jsonMsg = Serialization.write(objConfs)
        chan.put(jsonMsg)
      }
    }

    Row(result)
  }
}

final class RDDPartition(
    val index: Int,
    numValues: Int,
    args: models.IterArgs
) extends Partition {
  def values(chan: Broadcast[LinkedTransferQueue[String]]): Iterator[Row] = {
    Iterator.range(0, numValues).map(n => {
      val res = UtilFuncs.mkRequest(args.conf, args.url, (n == 0 && index == 0), chan.value)
      Thread.sleep(args.conf.waitMili)
      res
    }).filterNot(_.isNullAt(0))
  }
}

final class CreateRDD(
    @transient private val sc: SparkContext,
    numSlices: Int,
    numValues: Int,
    args: models.IterArgs
) extends RDD[Row](sc, deps = Seq.empty) {
  if (numValues < numSlices) {
    throw new Exception("numValues is lower than numSlices")
  }
  private val valuesPerSlice = numValues / numSlices
  private val slicesWithExtraItem = numValues % numSlices

  val channel = sc.broadcast(new LinkedTransferQueue[String])

  override def compute(part: Partition, context: TaskContext): Iterator[Row] = part.asInstanceOf[RDDPartition].values(channel)

  override protected def getPartitions: Array[Partition] = {
    (
      (0 until slicesWithExtraItem).view.map(
        new RDDPartition(_, valuesPerSlice + 1, args)
      ) ++
        (slicesWithExtraItem until numSlices).view.map(
          new RDDPartition(_, valuesPerSlice, args)
        )
    ).toArray
  }
}

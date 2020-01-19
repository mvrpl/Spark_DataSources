class Engine(sitemapURL: String, outDir: String, charsetData: String = "UTF-8") {
    import org.apache.log4j.Logger
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    import scalaj.http._
    import scala.util.{Try,Success,Failure}

    @transient lazy val log = Logger.getLogger("ExtratorHTML")
    @transient lazy val conf: Configuration = new Configuration
    @transient lazy val hdfs: FileSystem = FileSystem.get(conf)

    def getXML(url: String): scala.xml.Elem = scala.xml.XML.loadString(Http(url).charset(charsetData).asString.body)

    def saveHTML(url: String, fail: Boolean = false): Unit = {
        val fileName = url.split("/").last ++ ".html"
        if (! hdfs.exists(new Path(outDir ++ "/" ++ fileName)) || fail) {
            Try(Http(url).option(HttpOptions.followRedirects(true)).timeout(connTimeoutMs = 2000, readTimeoutMs = 5000).charset(charsetData).asBytes.body) match {
                case Success(conteudo) => {
                    val saidaHDFS = hdfs.create(new Path(outDir ++ "/" ++ fileName))
                    saidaHDFS.write(conteudo)
                    saidaHDFS.close
                    saidaHDFS.flush
                }
                case Failure(f) => {
                    println(f, url)
                    saveHTML(url, true)
                }
            }
        }
    }

    def parseXML(xml: scala.xml.Elem): Unit = {
        (xml \\ "_").par.foreach{child =>
            val loc = (child \ "loc").text
            if (loc.endsWith(".xml")) {
                parseXML(getXML(loc))
            } else if (loc.contains("produto")) {
                saveHTML(loc)
            }
        }
    }

    def run = {
        val root = getXML(sitemapURL)
        val rddSitemaps = parseXML(root)
    }
}

object ExtratorHTML {
    def main(args: Array[String]): Unit = {
        new Engine("https://www.kabum.com.br/sitemap.xml", "/tmp/kabum.com.br", "ISO-8859-1").run
    }
}
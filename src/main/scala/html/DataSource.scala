package html

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, RelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.{DataType, TimestampType, StringType, StructField, StructType, ArrayType, DateType, DoubleType}
import org.apache.spark.sql.{Row, SQLContext}
import org.jsoup._
import collection.JavaConverters._
import scala.util.Try
import java.sql.Date
import java.net.URLDecoder

class HTMLRelation(location: String, charset: String, urldecode: Boolean, userSchema: StructType, parallelize: Int = 10)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {
	override def schema: StructType = {
		if (this.userSchema != null) {
			return this.userSchema
		}
		else {
			return StructType(Seq(
				StructField("nome", StringType, true), 
				StructField("codigo", StringType, true), 
				StructField("preco_avista", DoubleType, true), 
				StructField("preco_normal", DoubleType, true), 
				StructField("preco_valido_ate", DateType, true), 
				StructField("marca", StringType, true)
			))
		}
	}

	def tryToDate(str: String): Option[Date] = {
		val format = new java.text.SimpleDateFormat("yyyy/MM/dd", new java.util.Locale("pt", "BR"))
		Try(new Date(format.parse(str).getTime)).toOption
	}

	def extractPreco(str: String): Option[Double] = {
		val valorRaw = str.replace(".", "").replace(",", ".").replace("R$ ", "")
		Try(valorRaw.toDouble).toOption
	}

	override def buildScan(): RDD[Row] = {
		sqlContext.sparkContext.binaryFiles(location, parallelize).map{case (_, content) =>
			val data = new String(content.toArray, charset)
			val doc = Jsoup.parse(data, charset)
			val nome = doc.select("h1.titulo_det").text
			val codigo = doc.select("span[itemprop=sku]").first.text
			val preco_avista = Try(doc.select("meta[itemprop=price]").attr("content").toDouble).toOption
			val preco_normal = extractPreco(doc.select("div.preco_normal").text)
			val preco_valido_ate = tryToDate(doc.select("meta[itemprop=priceValidUntil]").attr("content"))
			val marca = doc.select("div.subtitle div.right").text
			Row(nome, codigo, preco_avista, preco_normal, preco_valido_ate, marca)
		}
	}
}

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
	override def shortName(): String = "html"

	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		createRelation(sqlContext, parameters, null)
	}
	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
		parameters.getOrElse("path", sys.error("'caminho' deve ser especificado."))
		val urlDecode: Boolean = "true" equalsIgnoreCase parameters.getOrElse("urldecode", "false")
		new HTMLRelation(parameters.get("path").get, parameters.getOrElse("charset", "UTF-8"), urlDecode, schema)(sqlContext)
	}
}

object DataFrameReaderConfigurator {
	implicit class HTMLDataFrameReader(val reader: DataFrameReader) extends AnyVal {
		def html(path: String) = reader.format("html").load(path)
	}
}
package api.batch

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, RelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import java.net.HttpCookie
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Formats}
import scala.collection.JavaConverters._

class ApiRelation(url: String, options: Map[String, String], userSchema: StructType)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {
	override def schema: StructType = {
		if (this.userSchema != null) {
			return this.userSchema
		}
		else {
			return StructType(Seq(
				StructField("value", StringType, true)
			))
		}
	}

	override def buildScan(): RDD[Row] = {
		implicit val formats: Formats = DefaultFormats

        val headers = JsonMethods.parse(options.getOrElse("headers", "{}").toString).noNulls.extract[Map[String, String]]
        val data = JsonMethods.parse(options.getOrElse("data", "{}").toString).noNulls.extract[Map[String, String]]
        val cookies = JsonMethods.parse(options.getOrElse("cookies", "{}").toString).noNulls.extract[Map[String, String]].map{case (k: String, v: String) =>
			k -> HttpCookie.parse(v).asScala.head
		}
        val params = JsonMethods.parse(options.getOrElse("params", "[]").toString).noNulls.extract[Seq[Map[String, String]]]

        val method = options.getOrElse("method", "GET")

		sqlContext.sparkContext.parallelize(params, options.getOrElse("numPartitions", "1").toInt).mapPartitions(partition => {
			partition.map(urlParams => {
				val r = requests.send(method)(url, headers = headers, data = data, cookies = cookies, params = urlParams)
				Row(r.text)
			})
		})
	}
}

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
	override def shortName(): String = "api_batch"

	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		createRelation(sqlContext, parameters, null)
	}
	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
		val url = parameters.getOrElse("path", sys.error("path deve ser especificado."))
		new ApiRelation(url, parameters, schema)(sqlContext)
	}
}

object DataFrameReaderConfigurator {
	implicit class ApiDataFrameReader(val reader: DataFrameReader) extends AnyVal {
		def apiBatch(urls: String) = reader.format("api.batch").load(urls)
	}
}
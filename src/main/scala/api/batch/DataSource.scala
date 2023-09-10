package api.batch

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, RelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import java.net.HttpCookie
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Formats}
import collection.mutable.ArrayBuffer
import scala.io.Source

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

		val source = Source.fromFile(options.getOrElse("conf_file", "request_confs.json"))
		val lines = try source.mkString finally source.close
		val conf = JsonMethods.parse(lines).noNulls.extract[models.ApiExtractorConf]

		val args = models.IterArgs(conf, url)

		new utils.CreateRDD(sqlContext.sparkContext, conf.numPartitions, conf.numValues, args)
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
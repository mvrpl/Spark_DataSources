package api.stream

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{BaseRelation, TableScan, StreamSourceProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import api.batch.ApiRelation

class DefaultSource extends StreamSourceProvider with SchemaRelationProvider with DataSourceRegister {
	override def shortName(): String = "api_stream"

    def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		createRelation(sqlContext, parameters, null)
	}

	def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
		val url = parameters.getOrElse("path", sys.error("path deve ser especificado."))
		new ApiRelation(url, parameters, schema)(sqlContext)
	}

    override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String,String]): (String, StructType) = {
        providerName -> schema.getOrElse{
			StructType(Seq(
				StructField("value", StringType, true)
			))
		}
    }

    override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
        ???
    }
}

object DataFrameReaderConfigurator {
	implicit class ApiDataFrameReader(val reader: DataFrameReader) extends AnyVal {
		def apiStream(urls: String) = reader.format("api.stream").load(urls)
	}
}
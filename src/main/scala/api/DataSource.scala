package api

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, RelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class ApiRelation(urls: Array[String], options: Map[String, String], userSchema: StructType, numPartitions: Int = 10)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {
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
		sqlContext.sparkContext.parallelize(urls, numPartitions).mapPartitions(partition => {
			partition.map(url => {
				val r = requests.get(url, headers = options)
				Row(r.text)
			})
		})
	}
}

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
	override def shortName(): String = "api"

	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		createRelation(sqlContext, parameters, null)
	}
	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
		parameters.getOrElse("path", sys.error("lista de URLs deve ser especificado."))
		val urls = parameters.get("path").get.split(",")
		new ApiRelation(urls, parameters, schema)(sqlContext)
	}
}

object DataFrameReaderConfigurator {
	implicit class ApiDataFrameReader(val reader: DataFrameReader) extends AnyVal {
		def api(urls: String) = reader.format("api").load(urls)
	}
}
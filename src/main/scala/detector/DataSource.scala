package detector

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan, RelationProvider, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType}
import org.apache.spark.sql.{Row, SQLContext}
import com.github.chen0040.objdetect.models.DetectedObj
import com.github.chen0040.objdetect.ObjectDetector
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.io.File
import java.util.List
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream

class DetectorRelation(location: String, userSchema: StructType, parallelize: Int)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {
	override def schema: StructType = {
		if (this.userSchema != null) {
			return this.userSchema
		}
		else {
			return StructType(Seq(
				StructField("file_name", StringType, true), 
				StructField("object", StringType, true), 
				StructField("score", DoubleType, true)
			))
		}
	}

	override def buildScan(): RDD[Row] = {
		val detector = new ObjectDetector()
    	detector.loadModel
		sqlContext.sparkContext.binaryFiles(location, parallelize).flatMap{case (fileName, content) =>
			val img = ImageIO.read(new ByteArrayInputStream(content.toArray()))
    		val result = detector.detectObjects(img)
			for {
				data <- result.toList
				dataObj = data.toString
				dataMap = dataObj.substring(1, dataObj.length - 1).trim.split(",").map(_.split(":")).take(2).map{case Array(k, v) => (k.trim,v.trim)}.toMap
				objectName = dataMap.get("label").get
				score = dataMap.get("score").get.toDouble
			} yield Row(fileName, objectName, score)
		}
	}
}

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
	override def shortName(): String = "detector"

	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		createRelation(sqlContext, parameters, null)
	}
	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
		parameters.getOrElse("path", sys.error("'caminho' deve ser especificado."))
		new DetectorRelation(parameters.get("path").get, schema, parameters.getOrElse("parallelize", "5").toInt)(sqlContext)
	}
}

object DataFrameReaderConfigurator {
	implicit class DetectorDataFrameReader(val reader: DataFrameReader) extends AnyVal {
		def detector(path: String) = reader.format("detector").load(path)
	}
}
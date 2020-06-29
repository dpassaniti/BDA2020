import util.{CommandLineOptions, FileUtil, TextUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors, Vector}

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.sql.functions.udf

import scala.math.pow

object task1
{
	//class used to iterate over segment dataframe
	case class segRow(pred_cluster:Int, OBJECTID:Int,
							startT:Timestamp, startX:Double, startY:Double,
							endT:Timestamp, endX:Double, endY:Double)

	//show entries from dataframe
	def dfPeek(df: Dataset[_], numRows: Int) : Unit =
	{
		val lines =  df.count()
		println(s"~~~ entries: $lines ~~~")	
		df.show(numRows,false)
   }

	//determine similarity of two segments
	def segmentSimilarity(inf: Row, other: segRow): Boolean = 
	{
		var similar = false

		//check that the two segments exist at the same time
		val infStart = inf(2).asInstanceOf[Timestamp]
		val infEnd = inf(5).asInstanceOf[Timestamp]
		
		//if other starts before infected
		if((other.startT.compareTo(infStart)) >= 0)
		{
			//if infected ends after other's start
			similar = infEnd.compareTo(other.startT) >= 0 
		}
		else
		{
			//if other ends after infected's start
			similar = other.endT.compareTo(infStart) >= 0
		}
		
		//check distance
		val infSX = inf(3).asInstanceOf[Double]
		val infSY = inf(4).asInstanceOf[Double]
		val infEX = inf(6).asInstanceOf[Double]
		val infEY = inf(7).asInstanceOf[Double]
		if(similar)
		{
			val sqrdStart = pow(infSX - other.startX,2) + pow(infSY - other.startY,2)
			val sqrdEnd = pow(infEX - other.endX,2) + pow(infEY - other.endY,2)
			val avgSqrdDist = (sqrdStart + sqrdEnd)/2
			similar = avgSqrdDist <= 4//2 meters squared = 4
		}
		
		return similar
	}
	
	//funcion to add exposure time to final candidates dataframe
	val idf_add = udf((prev: Int, post: Int) => prev + post)

	def main(args: Array[String]): Unit =
	{
    	//command-line processing code
		val options = CommandLineOptions(
			this.getClass.getSimpleName,
			CommandLineOptions.inputPath("data/oldDataTest/*.csv"),
			CommandLineOptions.outputPath("output/task1"),
			CommandLineOptions.master("local"),
			CommandLineOptions.quiet)

		val argz   = options(args.toList)
		val master = argz("master")
		val quiet  = argz("quiet").toBoolean
		val in     = argz("input-path")
		val out    = argz("output-path")
		if (master.startsWith("local"))
		{
			if (!quiet) println(s" **** Deleting old output (if any), $out:")
			FileUtil.rmrf(out)
		}

		// Kryo serialization setup.
		// If the data had a custom type, we would want to register it. Kryo already
		// handles common types, like String, which is all we use here:
		// config.registerKryoClasses(Array(classOf[MyCustomClass]))

		val name = "Task1"
		val spark = SparkSession.builder.
			master(master).
			appName(name).
			config("spark.app.id", name).   // To silence Metrics warning.
			config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
			getOrCreate()
			//val sc = spark.sparkContext
		import spark.implicits._
		try
		{
			//LOAD DATA
			
			// schema
			val schema = StructType(
				StructField("", IntegerType, nullable = false) ::
				StructField("OBJECTID", IntegerType, nullable = false) ::
				StructField("FLOORID", IntegerType, nullable = false) ::
				StructField("ROOMID", IntegerType, nullable = false) ::
				StructField("X", DoubleType, nullable = false) ::
				StructField("Y", DoubleType, nullable = false) ::
				StructField("TIMESTAMP", TimestampType, nullable = false) ::
				Nil
			)
			// read to DataFrame
			val df = spark.read.format("csv")
			  .option("header", value = true)
			  .option("delimiter", ",")
			  .option("mode", "DROPMALFORMED")
			  .option("timestampFormat", "yy/MM/dd HH:mm:ss")
			  .schema(schema)
			  .load(in)
			  .drop("FLOORID","ROOMID")

			//FIND THE INFECTED TRAJECTORY START AND END TIMES
			
			val infect_id = 29//file traj24_19.csv
			val df_infect = df.filter(df("OBJECTID")===infect_id)
			
			//we know a single traj can always fit into memory so it's ok to do this
			val start = df_infect.agg(min("TIMESTAMP")).head.getTimestamp(0)
			val end = df_infect.agg(max("TIMESTAMP")).head.getTimestamp(0)
			
			//FILTER OUT COORDINATES RECORDED BEFORE OR AFTER
			
			val df_timeRelevant = df.filter(df("TIMESTAMP") >= start && df("TIMESTAMP") <= end)
			//println(s"start: $start\nend: $end")
			//dfPeek(df_timeRelevant,10)
		
			//TRAIN THE CLUSTERING MODEL ON X,Y
			
			val cols = Array("X","Y")
			val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
			val df_features = assembler.transform(df_timeRelevant).cache()
			//println(df_features.select("features").collect()(0))
	
			//TODO DON'T TRAIN ON WHOLE DATA?
			
			//val seed = 5043
			//val Array(trainingData, testData) = df_features.randomSplit(Array(0.7, 0.3), seed)

			//CREATE AND TRAIN MODEL

			val nClusters = 50
			val bkmeans = new BisectingKMeans()
				.setK(nClusters)
				.setFeaturesCol("features")
				.setPredictionCol("pred_cluster")
			val bkmeansModel = bkmeans.fit(df_features)
			
			//CLUSTER DATA

			//TODO save model and reuse it here so it's faster on future runs	
			val df_clustered = bkmeansModel.transform(df_features)
			/*
			dfPeek(df_clustered.filter(df_clustered("pred_cluster")===0),20)
			dfPeek(df_clustered.filter(df_clustered("pred_cluster")===10),20)
			dfPeek(df_clustered.filter(df_clustered("pred_cluster")===70),3)
			*/

			//FILTER OUT CLUSTERS NOT CONTAINING ANY INFECTED TRAJECTORY PARTS
			
			val df_relClusList = df_clustered
				.filter(df_clustered("OBJECTID")===infect_id)
				.select(df_clustered("pred_cluster"))
				.distinct	
			
			val relevantClusters = df_relClusList.rdd.collect().map(v => v.getInt(0)).toList
			//relevantClusters.foreach{println}
			
			val df_relClus = df_clustered
				.filter(df_clustered("pred_cluster").isin(relevantClusters:_*))
				.drop("features")
			//dfPeek(df_relClus,5)
		
			//we should use TIMESTAMP but our data has many entries with the same TIMESTAMP
			//use row number instread (the "" field in csv) to find the earliest and latest coordinate
			val df_segmentsStart = df_relClus
				//.where($"OBJECTID"=!=infect_id)
				.withColumn("minrow", min($"").over(Window.partitionBy($"pred_cluster",$"OBJECTID")))
				.where($"" === $"minrow") 
				.drop("minrow")
				.orderBy("pred_cluster","OBJECTID")
			//df_segmentsStart.show(20,true)
			
			val df_segmentsEnd = df_relClus
				//.where($"OBJECTID"=!=infect_id)
				.withColumn("maxrow", max($"").over(Window.partitionBy($"pred_cluster",$"OBJECTID")))
				.where($"" === $"maxrow")
				.drop("maxrow")
				.orderBy("pred_cluster","OBJECTID")
			//df_segmentsEnd.show(20,true)
			
			val df_segmentsAll = df_segmentsStart
				.select($"pred_cluster",$"OBJECTID",$"TIMESTAMP".as("startT"),$"X".as("startX"),$"Y".as("startY"))
				.join(
					df_segmentsEnd
						.select($"pred_cluster",$"OBJECTID",$"TIMESTAMP".as("endT"),$"X".as("endX"),$"Y".as("endY")),
					Seq("pred_cluster","OBJECTID"),
					"inner")

			val df_infectSegs = df_segmentsAll.where($"OBJECTID" === infect_id).cache()
			val df_segments = df_segmentsAll.where($"OBJECTID" =!= infect_id).cache()

			//prepare map to record results
			val df_allCandidates = df_segments.select($"OBJECTID").distinct()
			val allCandidates = df_allCandidates.rdd.collect().map(v => v.getInt(0)).toList
			val finCandidates = collection.mutable.Map(allCandidates.map(c => (c,0f)): _*)	

			for(clus <- relevantClusters)
			{
				//print(s"|$clus|")//debug

				val infRow = df_infectSegs.where($"pred_cluster" === clus).rdd.collect()//.foreach{println}
				val df_currentC = df_segments.where($"pred_cluster"===clus).cache()
				
				//loop through all segments in the current cluster
				//var count = 0//debug
				df_currentC.as[segRow]
					.collect()
					.foreach(r =>
					{
						val toAdd = segmentSimilarity(infRow(0), r)
						if(toAdd)
						{
							//count+=1//debug
							val infStart = infRow(0)(2).asInstanceOf[Timestamp].getSeconds()
							val infEnd = infRow(0)(5).asInstanceOf[Timestamp].getSeconds()
							val exposure = (r.endT.getSeconds() - r.startT.getSeconds() + infEnd - infStart)/2f
							finCandidates(r.OBJECTID) += exposure
						}
					})
				//val tot = df_currentC.count.toInt//debug
				//println(s"$count/$tot")//debug
			}
			
			//filter and write results
			finCandidates.retain((k,v) => v >= 15)//15 seconds or higher -> possible infection
			val df_final = finCandidates.toSeq.toDF("OBJECTID", "exposureT")
			df_final.write
				.format("com.databricks.spark.csv")
				.option("header","true")
				.save("./output/task1")
		}
		finally
		{
			spark.stop()
		}
	}
}


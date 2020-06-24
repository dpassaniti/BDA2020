/*
NAIVE: JUST COMPARE ALL TRAJS AFTER FIRST TIME FILTER
look at cosine sim? but we might need to have subtrajs first to compare...
AFTER CAN IMPROVE WITH LSH OR CLUSTER OR SMT
	//SPLIT DATA IN BUCKETS BASED ON TIME (size = duration of infected/n, where n is COVID window?) TO OBTAIN SUBTRAJECTORIES
	//OR CLUSTER BASED ON TIME? how to represent time as points in 2d plane?
	//clustering based on point repr wont work since we can have different clusters but common subtraj
	//COMPARE THE SUBTRAJECTORIES

CLUSTER:
do clustering, not by reducing each traj to a point. treat each coord as point because we dont care about full
	trajectory!!! we get clusters of points that are nearby each other. then we can create subtrajectories by taking all
	the points within a cluster that belong to the same trajectory. then the candidate pairs are the subtrajectories
	inside the same cluster!!!
need a special (kmeans) version that takes into account COVID range?

SUBTRAJ MAKING:
FOR STARTERS, ALL POINTS WITHIN SAME CLUSTER ARE 1 SUBTRAJ
in a traj, for each 3 consecutive points:
	make vectors 1->2 and 2->3
	if cosine sim is too high: end the current subtraj and start next one
	each subtraj is represented by first and last point since they are mostly linear(line segment breakdown like traclus)
	now we can compare subtraj from different TRAJIDS

COMPARISON:
for each cand pair:
	count = 0
	run along traj to find find the first points in the 2 trajs that are in range
		foreach pointA:
			foreach pointB:
				if(dist() < treshold)
					break
	keep moving along traj increasing count as long as next point pair is in range
	if next pair is out of range: dont stop, remember count, could find more later
	at end of traj, if count > exT -> out ID

or if we have clusters of segments:
calc time exposures per segment pair
from all clusters, sum all duratoins with same TrajInfectID-TrajID2 key
per key: if > T -> out id
clusters
*/

//SNIPPETS FOR REFERENCE

////create a vector from x column of infected dframe (TEST for reference, we will use VectorAssembler)
////in file row 1 = column names, row 2 = first values row, therefore in vector here, vecname(0) = row 2 in file
//val rowsRdd = df_infect.select("X").rdd
//val xs = Vectors.dense(rowsRdd.collect().map(x => x.getString(0).toDouble))

////udf example
// Define your udf. In this case I defined a simple function, but they can get complicated.
//val myTransformationUDF = udf(value => value / 10)
//// Run that transformation "over" your DataFrame
//val afterTransformationDF = distinctValuesDF.select(myTransformationUDF(col("age")))

//// vector from dataset and foreach
//val relevantClusters = Vectors.dense(relevantClustersDf.rdd.collect().map(v => v.getInt(0).toDouble))
//relevantClusters.foreachActive{(indx,id) => println(id)}

import util.{CommandLineOptions, FileUtil, TextUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{min,max}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors, Vector}

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.sql.functions.udf

object task1
{
	def dfPeek(df: Dataset[_], numRows: Int) : Unit =
	{
		val lines =  df.count()
		println(s"~~~ entries: $lines ~~~")	
		df.show(numRows,false)
   }
	
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
			  .cache()

			//FIND THE INFECTED TRAJECTORY START AND END TIMES
			
			val infect_id = 29//file traj24_19.csv
			val df_infect = df.filter(df("OBJECTID")===infect_id)

			val start = df_infect.agg(min("TIMESTAMP")).head.getTimestamp(0)//we know a single traj can always fit into memory so it's ok to do this
			val end = df_infect.agg(max("TIMESTAMP")).head.getTimestamp(0)
			
			//FILTER OUT COORDINATES RECORDED BEFORE OR AFTER
			
			val df_timeRelevant = df.filter(df("TIMESTAMP") >= start && df("TIMESTAMP") <= end)
			//println(s"start: $start\nend: $end")
			//dfPeek(df_timeRelevant,10)
		
			//TRAIN THE CLUSTERING MODEL ON X,Y
			
			val cols = Array("X","Y")
			val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
			val featureDf = assembler.transform(df_timeRelevant)
			//println(featureDf.select("features").collect()(0))
	
			//DON'T TRAIN ON WHOLE DATA?
			
			//val seed = 5043
			//val Array(trainingData, testData) = featureDf.randomSplit(Array(0.7, 0.3), seed)

			//CREATE AND TRAIN MODEL

			val nClusters = 100
			val bkmeans = new BisectingKMeans()
				.setK(nClusters)
				.setFeaturesCol("features")
				.setPredictionCol("prediction")
			val bkmeansModel = bkmeans.fit(featureDf)
			
			//CLUSTER DATA

			//TODO save model and reuse it here so it's faster on future runs	
			val clusteredDf = bkmeansModel.transform(featureDf)
			/*
			dfPeek(clusteredDf.filter(clusteredDf("prediction")===0),20)
			dfPeek(clusteredDf.filter(clusteredDf("prediction")===10),20)
			dfPeek(clusteredDf.filter(clusteredDf("prediction")===70),3)
			*/

			//FILTER OUT CLUSTERS NOT CONTAINING ANY INFECTED TRAJECTORY PARTS
			
			val relevantClustersDf = clusteredDf
				.filter(clusteredDf("OBJECTID")===infect_id)
				.select(clusteredDf("prediction"))
				.distinct	
			
			val relevantClusters = relevantClustersDf.rdd.collect().map(v => v.getInt(0)).toList
			//relevantClusters.foreach{println}
			
			val relClusDf = clusteredDf.filter(clusteredDf("prediction").isin(relevantClusters:_*))
			dfPeek(relClusDf,5)
		
		}
		finally
		{
			spark.stop()
		}
	}
}

import util.{CommandLineOptions, FileUtil, TextUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{min,max}
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
			val df = spark.read
				.format("csv")
				.option("header", "true")
				.option("mode", "DROPMALFORMED")
				.load(in)
			//df.cache //can cache a dataframe if we use it many times

			//FIND THE INFECTED TRAJECTORY START AND END TIMES

			val infect_id = 29//file traj24_19.csv
			val df_infect = df.filter(df("OBJECTID")===infect_id)
			val start = df_infect.agg(min("TIMESTAMP")).head.getString(0)//we know a single traj can always fit into memory so it's ok to do this
			val end = df_infect.agg(max("TIMESTAMP")).head.getString(0)
			
			//FILTER COORDINATES RECORDED BEFORE OR AFTER
			
			val df_timeRelevant = df.filter(df("TIMESTAMP") >= start && df("TIMESTAMP") <= end)
			//val df_timeRelevant = df.filter(df("TIMESTAMP") < start || df("TIMESTAMP") > end)
			
			println(s"start: $start\nend: $end")
			dfPeek(df_timeRelevant,10)

/*

DO THIS NOW FOR NAIVE IMPLEMENTATION:

for each cand pair:
	count = 0
	run along traj to find find the first points in the 2 trajs that are in range
	keep moving along traj increasing count as long as next point pair is in range
	if next pair is out of range: dont stop, remember count, could find more later
	at end of traj, if count > exT -> out ID
		
AFTER CAN IMPROVE WITH LSH OR CLUSTER OR SMT
	//SPLIT DATA IN BUCKETS BASED ON TIME (size = duration of infected/n, where n is COVID window?) TO OBTAIN SUBTRAJECTORIES
	//OR CLUSTER BASED ON TIME? how to represent time as points in 2d plane?
	//clustering based on point repr wont work since we can have different clusters but common subtraj
	//COMPARE THE SUBTRAJECTORIES

*/			


		}
		finally
		{
			spark.stop()
		}
	}
}

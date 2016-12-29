import org.apache.spark.serializer.{ KryoSerializer => SparkKryoSerializer }

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.math._
import scala.util.Random

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import collection.JavaConversions._
import java.io.File
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import org.apache.commons.math3.fitting.PolynomialCurveFitter
import org.apache.commons.math3.fitting.WeightedObservedPoints

//import shark.execution.serialization.KryoSerializationWrapper

object LoadModeling {

  val mdmHome = scala.util.Properties.envOrElse("MDM_HOME", "MDM/")
  val config = ConfigFactory.parseFile(new File(mdmHome + "src/main/resources/application.conf"))
  val numProcesses = config.getInt("mdms.numProcesses")
  println(s"numProcesses=$numProcesses")

  val runmode = config.getInt("mdms.runmode")
  //val srcurl = "jdbc:postgresql://192.168.5.2:5433/sgdm_for_etl?user=wendong&password=wendong"
  val tgturl = config.getString("mdms.tgturl")
  val srcurl = tgturl

  val numClusters = config.getInt("mdms.numClusters")
  val numIters = config.getInt("mdms.numIters")
  val numRuns = config.getInt("mdms.numRuns")
  val numDays = 3258

  val numLoadtypes = config.getInt("mdms.numLoadtypes") // Industrial, Commercial, Residential
  val numSeasons   = config.getInt("mdms.numSeasons")   // Spring, Summer, Fall, Winter
  val numDaytypes  = config.getInt("mdms.numDaytypes")  // Weekday, Weekend, Holiday
  val minNumPoints = config.getInt("mdms.minNumPoints")
 
  // Number of Buckets to bin a range of active power data points
  val numBucketAP  = config.getInt("mdms.numBucketAP")
  val numBucketV   = config.getInt("mdms.numBucketV")

  val volt_low  = config.getDouble("mdms.volt_low")
  val volt_high = config.getDouble("mdms.volt_high")
  val volt220_nominal = config.getDouble("mdms.volt220_nominal")
  val volt110_nominal = config.getDouble("mdms.volt110_nominal")
  
  val interactiveMeter = config.getString("mdms.interactive_meter")
  val meterIDs = config.getLongList("mdms.meterids.ids")

  val partitionLB =  config.getString("mdms.partitionLB") // partition Lower Bound
  val partitionUB =  config.getString("mdms.partitionUB") // partition Upper Bound
  val numPartitions =  config.getString("mdms.numPartitions") // number of partitions for loading data
  val partCol = config.getString("mdms.partCol") // partition column 

  val pgHourGroup = "data_quality.hourgroup"
  val pgPVHG = "data_quality.pvhg"

  val prop = new java.util.Properties
  prop.setProperty("batchsize", "10000000") 
    
  val voltLow1 = 10.0
  val voltLow2 = 88.0
  val voltLow3 = 132.0
  val voltLow4 = 188.0
  val voltHigh = 236.0
  val voltHigh2 = 253.0
  val voltDefault = 220.0

  // table to write
  val pgdqVolt = "data_quality.volt"
  val pgdqVl = "data_quality.voltagelow"
  val pgdqVh = "data_quality.voltagehigh"
  val pgdqVo = "data_quality.voltageout"
  val pgtestvop = "data_quality.voltageout_phc"
  val pgbasereading = "basereading"
  val pgenddevice = "enddevice"
  val pgido = "identifiedobject"
  val pgmeter = "meter"
  val pgpvcurve = "data_quality.pvcurve"
  val pgqvcurve = "data_quality.qvcurve"

  val tblMIDs = "data_quality.meterids"

def main(args: Array[String]) {

  val sparkConf = new SparkConf().setAppName("LoadModeling")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  //val sqlContext = new HiveContext(sc)

  import sqlContext.implicits._

  val toIntg2 = udf((d: java.math.BigDecimal) => d.toString.replaceAll("""\.0+$""", "").toInt)
  val toLong2 = udf((d: java.math.BigDecimal) => d.toString.replaceAll("""\.0+$""", "").toLong)
  val toDecimal = udf((d: Int) => scala.math.BigDecimal(d))

  var hgMap = scala.collection.mutable.Map[(Long, Int, Int, Long),Int]()

  def getHG(id: Long, se: Int, dy: Int, hi: Long): Int = {
    hgMap.getOrElse((id, se, dy, hi), -1)
  }

  def conv2HG(id: Long, dti: Long, se: Int, dy: Int): Int = {
    var hi = (dti-1) % 96L / 4
    val hg = getHG(id, se, dy, hi)
    return hg
  }

  //val toHG = udf(conv2HG(_: Long, _: Long, _: Int, _: Int)) //for some reason, this not works, so we create udf directly.
  val toHG = udf {(id: Long, dti: Long, se: Int, dy: Int) =>
                     var hi = (dti-1) % 96L / 4
                     hgMap.getOrElse((id, se, dy, hi), -1) 
                 } 

  /**
   * Loading data
   */
  val pvsdDF = sqlContext.read.format("jdbc").option("url", srcurl).option("dbtable", pgpvcurve).option("partitionColumn", partCol).option("lowerBound", partitionLB).option("upperBound", partitionUB).option("numPartitions", numPartitions).load().cache
  val qvsdDF = sqlContext.read.format("jdbc").option("url", srcurl).option("dbtable", pgqvcurve).option("partitionColumn", partCol).option("lowerBound", partitionLB).option("upperBound", partitionUB).option("numPartitions", numPartitions).load().cache
  //val id_match = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "id_match")).cache
  pvsdDF.count
  qvsdDF.count
   
  var urdataMap =  scala.collection.mutable.Map[(Long, Int, Int),Int]()
  var urdata = new ArrayBuffer[Row @unchecked]()

  // Lower case column name (Postgresql)
  // function to define PV features
  def get24HoursPVFAll(sc: SparkContext, sqlContext: SQLContext, pvsdDF: DataFrame, qvsdDF: DataFrame) = {

    import sqlContext.implicits._
    var pvsdBuckets = pvsdDF.na.drop()
                        .filter(s"volt_c <= $volt_high and volt_c >= $voltLow2")
                        .withColumn("bucket", pmod($"dti", lit(96)))
                        .select("id", "ts", "volt_c", "power", "dti", "sdti", "season", "daytype", "bucket")
    // P feature
    var featureP = pvsdBuckets.groupBy($"id", $"bucket", $"season", $"daytype").agg(avg("power")).sort("id", "bucket", "season", "daytype").cache()
    featureP.count //force to be cached
    // V feature
    var featureV = pvsdBuckets.groupBy($"id", $"bucket", $"season", $"daytype").agg(avg("volt_c")).sort("id", "bucket", "season", "daytype").cache()
    featureV.count //force to be cached
    val arrfp = featureP.collect
    val arrfv = featureV.collect
    //if (runmode == 1 || runmode == 4)
    //  featureP.select("ID").distinct.coalesce(numProcesses).write.mode("overwrite").jdbc(tgturl, tblMIDs, new java.util.Properties)
    (arrfp, arrfv)
  }

  // Upper case column name (Oracle)

//** START K-MEANS BLOCK **//

  //get the mean of a cluster of points
  def clusterMean(points: List[(Double,Double)]): (Double,Double) = {
    val cumulative = points.reduceLeft((a: (Double,Double), b: (Double,Double)) => (a._1 + b._1, a._2 + b._2))
    return (cumulative._1 / points.length, cumulative._2 / points.length)
  }
  //.foldLeft((0.0,0.0),0)((b,a) => ((b._1._1+a._1,b._1._2+a._2),b._2+1))

  //get the mean of all the clusters
  def mean(clusters: Map[Int, List[(Double,Double)]]): List[(Double,Double)] = {
    // find cluster means
    var means = List[(Double,Double)]()
    for (clusterIndex <- clusters.keys)
       means = means :+ clusterMean(clusters(clusterIndex))
       means
  }
     
  //helper function for clustering 
  def closest(p: (Double,Double), means: List[(Double,Double)]): Int = {
      val distances = means.map(center => ((p._1-center._1)*(p._1-center._1) + (p._2-center._2)*(p._2-center._2)))
      return distances.zipWithIndex.min._2
  }
  //x._1.map(center => ((p._1-center._1)*(p._1-center._1) + (p._2-center._2)*(p._2-center._2))).zipWithIndex.min._2
  //x._1.map(center => ((p._1._1-center._1)*(p._1._1-center._1) + (p._1._2-center._2)*(p._1._2-center._2))).zipWithIndex.min._2
  // val collection = means.zip(data).map(x=>x._2.map(p=>(p._2,x._1.map(center => ((p._1._1-center._1)*(p._1._1-center._1) + (p._1._2-center._2)*(p._1._2-center._2))).zipWithIndex.min._2)))

  //group points into clusters based off a set of means
  def cluster(means: List[(Double,Double)], points: List[(Double,Double)]): Map[Int, List[(Double,Double)]] = {
     // assignment step
      val newClusters =
      points.groupBy(
      p => closest(p, means))
      newClusters
  }

  // high level function that clusters and re-evaluates the means for k-means
  def iterate(means: List[(Double,Double)], points: List[(Double,Double)]): List[(Double,Double)] = {
     var clusters = cluster(means,points)
     var newMeans = mean(clusters)
     newMeans
  }

  // collect results
  def collect(means: List[(Double,Double)], data: List[((Double, Double),Int)]) = {
     val newClusters = data.map( p => (p._2, closest(p._1, means)))
     newClusters
  }


  //main k-means function, input takes in a list of pairs of points and indices, with hourindex as the Int parameter
  // the function preserves the pairing of each point to its hourindex
  def kmeans(data: List[((Double, Double),Int)]) = {

    var k = 6 // number of clusters
    var numIters = 20 //number of iterations to run
    var points = data.map(x=>x._1)
    var means = Random.shuffle(points).take(k) // random initialization of means
    
    for (i <- 0 to numIters) {
      means = iterate(means, points)
    }
    collect(means,data)
  }

  //** END K-MEANS BLOCK **//


  // Get arrays of PV feature Vector; Also populate tblMIDs
  val (arrfp, arrfv) = get24HoursPVFAll(sc, sqlContext, pvsdDF, qvsdDF)

    var arrHrPVRDD = new ArrayBuffer[RDD[(Vector, Long)]]()
    var arrKMMOpt  = new ArrayBuffer[Option[KMeansModel]]()
    var arrHGOpt = new ArrayBuffer[Option[RDD[(Int, Iterable[Long])]]]()
    //var arrHGRow = new ArrayBuffer[Row]()

    var clustersLSDOpt: Option[KMeansModel] = None
    var pvhgmRDDOpt: Option[RDD[(Int, Iterable[Long])]] = None
 
    //Read the meter id list 
    val midDF = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> "(select meterid from meter) id")) 
    //val midDF = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> "id_match")) 
     
    //val meterids = midDF.select("ID").rdd.collect 
    //val meterids = midDF.select("id").map(r => r.getDecimal(0).toString.replaceAll("""\.0+$""", "").toLong).collect
    val meterids = midDF.select("meterid").map(r => r.getLong(0)).collect
    
    var mids = meterids   // .slice(0, 50) //just pick up some meters for testing 

    // modified getFeatureVectors to pass arrfp and arrfv arrays only once
    // used so that mapping k-means is easier, also zips idx, se, and dt 
    // requires a set of meters to consider
    def getFeatureVectors(meters: Set[Long]) = {
          //gets the feature vector for a given id, se, and dt
      //this function is used in a mapping call to convert fpi and fvi arrays into a format usable by k-means
      def getFeatureVector(fpi: Array[Double], fvi: Array[Double]) = {
        // Initialize 
        var hr0vPV = (0.0,0.0); var hr1vPV = (0.0,0.0); var hr2vPV = (0.0,0.0); var hr3vPV = (0.0,0.0);
        var hr4vPV = (0.0,0.0); var hr5vPV = (0.0,0.0); var hr6vPV = (0.0,0.0); var hr7vPV = (0.0,0.0);
        var hr8vPV = (0.0,0.0); var hr9vPV = (0.0,0.0); var hr10vPV = (0.0,0.0); var hr11vPV = (0.0,0.0);
        var hr12vPV = (0.0,0.0); var hr13vPV = (0.0,0.0); var hr14vPV = (0.0,0.0); var hr15vPV = (0.0,0.0);
        var hr16vPV = (0.0,0.0); var hr17vPV = (0.0,0.0); var hr18vPV = (0.0,0.0); var hr19vPV = (0.0,0.0);
        var hr20vPV = (0.0,0.0); var hr21vPV = (0.0,0.0); var hr22vPV = (0.0,0.0); var hr23vPV = (0.0,0.0);
      
      if (fpi.size >= 12) {
        
        //var hrpvData = sc.emptyRDD[Vector]
        // Preparing feature vector of N features per hour for each hour (total 24 hours)
        // Generate dense Vector for each hour, containing feature vector of power features and voltage features    
        var volt_nominal = 220.0 
        if (fvi(1) < 154) 
          volt_nominal = 110.0    
      if (fpi.size == 96) {
          hr0vPV = (fpi(1), fvi(1)/volt_nominal)
          hr1vPV = (fpi(5), fvi(5)/volt_nominal)
          hr2vPV = (fpi(9), fvi(9)/volt_nominal)
          hr3vPV = (fpi(13), fvi(13)/volt_nominal)
          hr4vPV = (fpi(17), fvi(17)/volt_nominal)
          hr5vPV = (fpi(21), fvi(21)/volt_nominal)
          hr6vPV = (fpi(25), fvi(25)/volt_nominal)
          hr7vPV = (fpi(29), fvi(29)/volt_nominal)
          hr8vPV = (fpi(33), fvi(33)/volt_nominal)
          hr9vPV = (fpi(37), fvi(37)/volt_nominal)
          hr10vPV = (fpi(41), fvi(41)/volt_nominal)
          hr11vPV = (fpi(45), fvi(45)/volt_nominal)
          hr12vPV = (fpi(49), fvi(49)/volt_nominal)
          hr13vPV = (fpi(53), fvi(53)/volt_nominal)
          hr14vPV = (fpi(57), fvi(57)/volt_nominal)
          hr15vPV = (fpi(61), fvi(61)/volt_nominal)
          hr16vPV = (fpi(65), fvi(65)/volt_nominal)
          hr17vPV = (fpi(69), fvi(69)/volt_nominal)
          hr18vPV = (fpi(73), fvi(73)/volt_nominal)
          hr19vPV = (fpi(77), fvi(77)/volt_nominal)
          hr20vPV = (fpi(81), fvi(81)/volt_nominal)
          hr21vPV = (fpi(85), fvi(85)/volt_nominal)
          hr22vPV = (fpi(89), fvi(89)/volt_nominal)
          hr23vPV = (fpi(93), fvi(93)/volt_nominal)
        }
        else if (fpi.size == 48) {
          hr0vPV = (fpi(1), fvi(1)/volt_nominal)
          hr1vPV = (fpi(3), fvi(3)/volt_nominal)
          hr2vPV = (fpi(5), fvi(5)/volt_nominal)
          hr3vPV = (fpi(7), fvi(7)/volt_nominal)
          hr4vPV = (fpi(9), fvi(9)/volt_nominal)
          hr5vPV = (fpi(11), fvi(11)/volt_nominal)
          hr6vPV = (fpi(13), fvi(13)/volt_nominal)
          hr7vPV = (fpi(15), fvi(15)/volt_nominal)
          hr8vPV = (fpi(17), fvi(17)/volt_nominal)
          hr9vPV = (fpi(19), fvi(19)/volt_nominal)
          hr10vPV = (fpi(21), fvi(21)/volt_nominal)
          hr11vPV = (fpi(23), fvi(23)/volt_nominal)
          hr12vPV = (fpi(25), fvi(25)/volt_nominal)
          hr13vPV = (fpi(27), fvi(27)/volt_nominal)
          hr14vPV = (fpi(29), fvi(29)/volt_nominal)
          hr15vPV = (fpi(31), fvi(31)/volt_nominal)
          hr16vPV = (fpi(33), fvi(33)/volt_nominal)
          hr17vPV = (fpi(35), fvi(35)/volt_nominal)
          hr18vPV = (fpi(37), fvi(37)/volt_nominal)
          hr19vPV = (fpi(39), fvi(39)/volt_nominal)
          hr20vPV = (fpi(41), fvi(41)/volt_nominal)
          hr21vPV = (fpi(43), fvi(43)/volt_nominal)
          hr22vPV = (fpi(45), fvi(45)/volt_nominal)
          hr23vPV = (fpi(47), fvi(47)/volt_nominal)
         // log.info(s"Found datapoints 48 in : $id, $season, $daytype")
        }
        else if (fpi.size == 24) {
          hr0vPV = (fpi(0), fvi(0)/volt_nominal)
          hr1vPV = (fpi(1), fvi(1)/volt_nominal)
          hr2vPV = (fpi(2), fvi(2)/volt_nominal)
          hr3vPV = (fpi(3), fvi(3)/volt_nominal)
          hr4vPV = (fpi(4), fvi(4)/volt_nominal)
          hr5vPV = (fpi(5), fvi(5)/volt_nominal)
          hr6vPV = (fpi(6), fvi(6)/volt_nominal)
          hr7vPV = (fpi(7), fvi(7)/volt_nominal)
          hr8vPV = (fpi(8), fvi(8)/volt_nominal)
          hr9vPV = (fpi(9), fvi(9)/volt_nominal)
          hr10vPV = (fpi(10), fvi(10)/volt_nominal)
          hr11vPV = (fpi(11), fvi(11)/volt_nominal)
          hr12vPV = (fpi(12), fvi(12)/volt_nominal)
          hr13vPV = (fpi(13), fvi(13)/volt_nominal)
          hr14vPV = (fpi(14), fvi(14)/volt_nominal)
          hr15vPV = (fpi(15), fvi(15)/volt_nominal)
          hr16vPV = (fpi(16), fvi(16)/volt_nominal)
          hr17vPV = (fpi(17), fvi(17)/volt_nominal)
          hr18vPV = (fpi(18), fvi(18)/volt_nominal)
          hr19vPV = (fpi(19), fvi(19)/volt_nominal)
          hr20vPV = (fpi(20), fvi(20)/volt_nominal)
          hr21vPV = (fpi(21), fvi(21)/volt_nominal)
          hr22vPV = (fpi(22), fvi(22)/volt_nominal)
          hr23vPV = (fpi(23), fvi(23)/volt_nominal)
        }
        else { // irregular data

          //log.info(s"Found irregular data: $id, $se, $dt")
          println(s"Found irregular data")
        }
      }
        if (fpi.size > 12) {
          // Create RDD of Vector of Bin frequency for both power and voltage data for training
          var hrpvData = List(hr0vPV, hr1vPV, hr2vPV, hr3vPV, hr4vPV, hr5vPV, hr6vPV, hr7vPV, hr8vPV, hr9vPV, hr10vPV,
                         hr11vPV, hr12vPV, hr13vPV, hr14vPV, hr15vPV, hr16vPV, hr17vPV, hr18vPV, hr19vPV, hr20vPV,
                         hr21vPV, hr22vPV, hr23vPV)
          // Return RDD, and flag for non-empty
          (hrpvData.zipWithIndex, 1)
        } else {
          (List(((0.0,0.0),0)), 0) // garbage value so function return type is set
        }
      }

      var fpiAll = for {
         r <- arrfp
         if (meters.contains(r.getDecimal(0).longValue))
         } yield {((r.getDecimal(0).longValue, r.getInt(2), r.getInt(3)),r.getDecimal(4).doubleValue)}
 
      var fpiMap = fpiAll.groupBy(x=>x._1)
      
      var fviAll = for {
         r <- arrfv
         if (meters.contains(r.getDecimal(0).longValue))
         } yield {((r.getDecimal(0).longValue, r.getInt(2), r.getInt(3)),r.getDecimal(4).doubleValue)}
 
      var fviMap = fviAll.groupBy(x=>x._1)

      // combine the two by key
      var fpifvi =  for {
         k <- fpiMap.keys }
         yield{(k,fpiMap(k), fviMap(k))}
      
      // and throw away the (idx,se,dt) pairs that were used for grouping
      var argsList = fpifvi.map(x=> (x._1, x._2.map(pair => pair._2), x._3.map(pair => pair._2)))
      
      //TODO implement flag which is currently ignored
      var res = argsList.map(x=>(getFeatureVector(x._2, x._3)._1, (x._1._1, x._1._2, x._1._3)))
      res
    }

    val s = System.nanoTime // start timing

    //get argument list and make a parallel list
    val hrpvRDD = sc.parallelize(getFeatureVectors(mids.toSet).toList)
    val data = hrpvRDD.map(x=>x._1).cache
    data.count
    val params = hrpvRDD.map(x=>x._2).cache
    params.count

    var points = data.map(x=>x.map(y=>y._1)).cache
    points.count
    var means = points.map(x=>Random.shuffle(x).take(6)).cache
    means.count

    // Iterate (usually 20 is enough) to get results
    for(i <- 0 to 20)
    { 
      var zipped = means zip points
      var clusters = zipped.map(x=> x._2.groupBy(p =>x._1.map(center => ((p._1-center._1)*(p._1-center._1) + (p._2-center._2)*(p._2-center._2))).zipWithIndex.min._2 ))
      var newMeans = clusters.map(x=>x.map(y=>y._2).map(y=>y.foldLeft((0.0,0.0),0)((b,a) => ((b._1._1+a._1,b._1._2+a._2),b._2+1))).map(x=>(x._1._1/x._2,x._1._2/x._2)).toList)
      newMeans.cache.count
      means.unpersist()
      means = newMeans
    }

    val collection = means.zip(data).map(x=>x._2.map(p=>(p._2,x._1.map(center => ((p._1._1-center._1)*(p._1._1-center._1) + (p._1._2-center._2)*(p._1._2-center._2))).zipWithIndex.min._2)))

    val clustersLSDRDD = collection.zip(params).map(x=>(x._1, x._2._1, x._2._2, x._2._3))

    //next two lines fold in idx, se, and dt variables into data and flatmaps to get the correct number of rows
    var arrHGSeq = clustersLSDRDD.map(x=>(x._1, List.fill(x._1.size)((x._2,x._3,x._4)))).map(x=>x._1 zip x._2).flatMap(x => x).map(x=>(x._2._1, x._2._2, x._2._3, x._1._2, x._1._1))

    //arrHGRow is turned into a list of Rows
    var arrHGRow = arrHGSeq.map(x=>Row(x._1,x._2,x._3,x._4,x._5))

    //forcible conversion of hourIndex into Long. Keep this on its own line or the table will not work!
    var hgRowRDD = arrHGRow.map(r => Row(r.getLong(0), r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4).toLong))

    hgRowRDD.cache.count
    
    //RDD cleanup
    data.unpersist()
    params.unpersist()
    points.unpersist()
    means.unpersist()

    var t =  System.nanoTime //end timing and check 
    println(t-s) 

    var schemaHG = StructType(List(StructField("ID", LongType), StructField("Season", IntegerType), StructField("Daytype", IntegerType),
                                   StructField("hourgroup", IntegerType), StructField("hourindex", LongType)))
    var hgDF = sqlContext.createDataFrame(hgRowRDD, schemaHG).sort("ID", "Season", "Daytype", "hourgroup", "hourindex")

    hgDF.coalesce(numProcesses).write.mode("overwrite").jdbc(tgturl, "data_quality.hourgroup_test", new java.util.Properties)

    val hgarr = hgDF.rdd.collect

    // Build hgMap
    for (r <- hgarr) {
      hgMap += (r.getLong(0), r.getInt(1), r.getInt(2), r.getLong(4)) -> r.getInt(3)
    }

    // Build P-V/Q-V curve with hourgroup
    val pvsdhg = pvsdDF.withColumn("hourgroup", toHG(toLong2($"id"), $"dti", $"season", $"daytype"))
    val qvsdhg = qvsdDF.withColumn("hourgroup", toHG(toLong2($"id"), $"dti", $"season", $"daytype"))

    pvsdhg.coalesce(numProcesses*2).write.mode("overwrite").jdbc(tgturl, "data_quality.pvsdhg_test", prop)
    qvsdhg.coalesce(numProcesses*2).write.mode("overwrite").jdbc(tgturl, "data_quality.qvsdhg_test", prop)

    hgRowRDD.unpersist()

  }
}    

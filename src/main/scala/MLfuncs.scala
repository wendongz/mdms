package mdmutil 

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import collection.JavaConversions._

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

import org.apache.commons.math3.fitting.PolynomialCurveFitter
import org.apache.commons.math3.fitting.WeightedObservedPoints

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.PropertyConfigurator

/**
 * Meter Data Management System utility and Machine Learning functions
 *
 */

class MLfuncs extends Serializable {

  // Read config parameters
  val mdmHome = scala.util.Properties.envOrElse("MDM_HOME", "MDM/")
  val SparkHome = scala.util.Properties.envOrElse("SPARK_HOME", "/home/admin/programs/spark")
  val config = ConfigFactory.parseFile(new File(mdmHome + "src/main/resources/application.conf"))
  val runmode = config.getInt("mdms.runmode")
  val tgturl = config.getString("mdms.tgturl")

  val numLoadtypes = config.getInt("mdms.numLoadtypes") // Industrial, Commercial, Residential
  val numSeasons   = config.getInt("mdms.numSeasons")   // Spring, Summer, Fall, Winter
  val numDaytypes  = config.getInt("mdms.numDaytypes")  // Weekday, Weekend, Holiday

  // Number of Buckets to bin a range of active power data points
  val numBucketAP  = config.getInt("mdms.numBucketAP")
  val numBucketAP2 = config.getInt("mdms.numBucketAP2")
  val numBucketV   = config.getInt("mdms.numBucketV")
  val numProcesses = config.getInt("mdms.numProcesses")

  val volt_low  = config.getDouble("mdms.volt_low")
  val volt_high = config.getDouble("mdms.volt_high")
  val volt_nominal = config.getDouble("mdms.volt_nominal")

  val interactiveMeter = config.getString("mdms.interactive_meter")
  val meterIDs = config.getLongList("mdms.meterids.ids")

  // Configure log
//  PropertyConfigurator.configure(SparkHome + "/conf/log4j.properties")
//  val log = LogManager.getRootLogger()
//  log.setLevel(Level.INFO)
   
  // Output table names
  val pgHourGroup = "data_quality.hourgroup" 
  val pgPVHG = "data_quality.pvhg"
  val pgpvsdhg = "data_quality.pvsdhg"
  val pgpvsdhg2 = "data_quality.pvsdhg2"
  val qgpvsdhg2 = "data_quality.qvsdhg2"
  val tblMIDs = "data_quality.meterids"

  // UDF applied to DataFrame columns
  val toDouble = udf((d: java.math.BigDecimal) => d.doubleValue)

  // Initialize hourgroup Map
  var hgMap = scala.collection.mutable.Map[(Long, Int, Int, Long),Int]()

  // Initialize unregular data Map
  var urdata = new ArrayBuffer[Row @unchecked]()

  /**
   * A helper function to get hourgroup number from meter id, season, daytype and hour index
   */
  def getHG(id: Long, se: Int, dy: Int, hi: Long) = {
    hgMap.getOrElse((id, se, dy, hi), -1)
  }

  /**
   * Function of converting to hourgroup
   */
  def conv2HG(id: Long, dti: Long, se: Int, dy: Int) = {
    var hi = (dti-1) % 96L / 4
    var hgi = getHG(id, se, dy, hi)
    hgi
  }

  /**
   * UDF - User Defined Function of getting hourgroup
   */
  val toHG = udf(conv2HG(_: Long, _: Long, _: Int, _: Int))



  /**
   * Parsing category information (Commercial, Industrial, Residential)
   * based on load consumptions (total, average, max).
   */
  def categoryParsing(pwrDF: DataFrame): RDD[Vector] = {

    // Get active power data
    val apDF = pwrDF.filter("DATA_TYPE=1 and POWER is not null")

    // To classify all customers into Commercial, Industrial, Residential
    val categoryDF = apDF.groupBy("ID")
                         .agg(sum("POWER") as 'totp, avg("POWER") as 'avgp, max("POWER") as 'maxp)
                         .select("ID", "totp", "avgp", "maxp")
                         .sort("ID")

    categoryDF.map{r => val dArray = Array(r.getDecimal(1).toString.toDouble,
                                           r.getDecimal(2).toString.toDouble,
                                           r.getDecimal(3).toString.toDouble)
                        Vectors.dense(dArray) }
  }

  /**
   * Parsing active power consumption data
   */
  def powerParsing(pwrDF: DataFrame): RDD[Vector] = {

    // Get active power data
    val apDF = pwrDF.filter("DATA_TYPE=1 and POWER is not null")
 
    //val hrP = power2DF.select("P1","P5","P9","P13","P17","P21","P25","P29","P33","P37","P41","P45","P49","P53","P57","P61","P65","P69","P73","P77","P81","P85","P89","P93")
    //val hrP2 = hrP.na.drop()  // dropping rows containing any null values

    // Preparing training data
    val hrP = apDF.filter("pmod(dti, 4) = 1").select("POWER")
   
    hrP.map{r => val dArray = Array(r.getDecimal(0).toString.toDouble)
                 Vectors.dense(dArray) }

  }

  /** 
   * Calculate clustering cost
   */
  def clusteringCost(parsedData: RDD[Vector], k: Integer): Double = {

    val maxIter = 300
    val clusters = KMeans.train(parsedData, k, maxIter)

    // Calculate WSSSE
    clusters.computeCost(parsedData)
  }

  /**
   * Get load types - Industrial, Commercial, Residential
   *
   */
  def getLoadtype(sc: SparkContext, sqlContext: SQLContext, cjccDF: DataFrame, pwrDF: DataFrame) = {

    // Get active power data
    val apDF = pwrDF.filter("DATA_TYPE=1 and POWER is not null")

    // To classify all customers into Commercial, Industrial, Residential
    val categoryDF = apDF.groupBy("ID")
                         .agg(sum("POWER") as 'totp, avg("POWER") as 'avgp, max("POWER") as 'maxp)
                         .select("ID", "totp", "avgp", "maxp")
                         .sort("ID")
 
    // Get meter id and attr code
    val mcatDF = cjccDF.select("mped_id", "mp_attr_code").sort("mped_id")

    // Join with active power DataFrame on id
    val mcatcode = mcatDF.join(apDF, mcatDF("mped_id") === apDF("ID"), "inner").select("ID", "TS", "DTI", "DATA_TYPE", "POWER", "mp_attr_code")

  }

  /**
   * Get 24 hourly Feature Vectors of active power and voltage. The calculation of each hour
   * is a chain of actions: filter->select->histogram. If concurrent (Future) works, then 48 concurrent
   * running should be fast. However, Future seems only submitting job to Spark, not running simultaneously. 
   *
   *   - Return: 
   *            RDD of Feature Vector containing PV information: RDD[Vector]
   */
  def get24HoursPVF(sc: SparkContext, pvsdDF: DataFrame, id: Long, season: Int, daytype: Int) = {

    // Drop null values and abnornal voltage data from  PV curve data; also filter loadtype and seasonal daytype
    val pvsd2DF = pvsdDF.na.drop()
                        .filter(s"ID = $id and VOLT_C <= $volt_high and VOLT_C >= $volt_low and Season = $season and Daytype = $daytype")
                        .cache()

    if (!pvsd2DF.rdd.isEmpty) { // Not Empty RDD

      // Create active power DoubleRDD
      val apRDD = pvsd2DF.select("POWER").map{r => r.getDecimal(0).toString.toDouble}

      // Create voltage DoubleRDD
      val voltRDD = pvsd2DF.select("VOLT_C").map{r => r.getDecimal(0).toString.toDouble}

      // Create an array of buckets from DoubleRDDFunctions Histogram, using the min and max values from ALL active power data
      val arrBucketAP = apRDD.histogram(numBucketAP)._1.toArray

      // Create an array of buckets from DoubleRDDFunctions Histogram, using the min and max values from ALL voltage data
      val arrBucketV = voltRDD.histogram(numBucketV)._1.toArray

      // Preparing feature vector of N features per hour for each hour (total 24 hours)
      // where each feature is a bin frequency of power data 

      var hr0vP  = pvsd2DF.filter("pmod(DTI, 96) = 1").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr1vP  = pvsd2DF.filter("pmod(DTI, 96) = 5").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr2vP  = pvsd2DF.filter("pmod(DTI, 96) = 9").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr3vP  = pvsd2DF.filter("pmod(DTI, 96) = 13").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr4vP  = pvsd2DF.filter("pmod(DTI, 96) = 17").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr5vP  = pvsd2DF.filter("pmod(DTI, 96) = 21").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr6vP  = pvsd2DF.filter("pmod(DTI, 96) = 25").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr7vP  = pvsd2DF.filter("pmod(DTI, 96) = 29").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr8vP  = pvsd2DF.filter("pmod(DTI, 96) = 33").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr9vP  = pvsd2DF.filter("pmod(DTI, 96) = 37").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr10vP = pvsd2DF.filter("pmod(DTI, 96) = 41").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr11vP = pvsd2DF.filter("pmod(DTI, 96) = 45").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr12vP = pvsd2DF.filter("pmod(DTI, 96) = 49").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr13vP = pvsd2DF.filter("pmod(DTI, 96) = 53").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr14vP = pvsd2DF.filter("pmod(DTI, 96) = 57").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr15vP = pvsd2DF.filter("pmod(DTI, 96) = 61").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr16vP = pvsd2DF.filter("pmod(DTI, 96) = 65").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr17vP = pvsd2DF.filter("pmod(DTI, 96) = 69").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr18vP = pvsd2DF.filter("pmod(DTI, 96) = 73").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr19vP = pvsd2DF.filter("pmod(DTI, 96) = 77").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr20vP = pvsd2DF.filter("pmod(DTI, 96) = 81").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr21vP = pvsd2DF.filter("pmod(DTI, 96) = 85").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr22vP = pvsd2DF.filter("pmod(DTI, 96) = 89").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      var hr23vP = pvsd2DF.filter("pmod(DTI, 96) = 93").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
    
      // Preparing feature vector of N features per hour for each hour (total 24 hours)
      // where each feature is a bin frequency of voltage data 

      var hr0vV  = pvsd2DF.filter("pmod(DTI, 96) = 1").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr1vV  = pvsd2DF.filter("pmod(DTI, 96) = 5").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr2vV  = pvsd2DF.filter("pmod(DTI, 96) = 9").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr3vV  = pvsd2DF.filter("pmod(DTI, 96) = 13").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr4vV  = pvsd2DF.filter("pmod(DTI, 96) = 17").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr5vV  = pvsd2DF.filter("pmod(DTI, 96) = 21").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr6vV  = pvsd2DF.filter("pmod(DTI, 96) = 25").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr7vV  = pvsd2DF.filter("pmod(DTI, 96) = 29").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr8vV  = pvsd2DF.filter("pmod(DTI, 96) = 33").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr9vV  = pvsd2DF.filter("pmod(DTI, 96) = 37").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr10vV = pvsd2DF.filter("pmod(DTI, 96) = 41").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr11vV = pvsd2DF.filter("pmod(DTI, 96) = 45").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr12vV = pvsd2DF.filter("pmod(DTI, 96) = 49").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr13vV = pvsd2DF.filter("pmod(DTI, 96) = 53").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr14vV = pvsd2DF.filter("pmod(DTI, 96) = 57").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr15vV = pvsd2DF.filter("pmod(DTI, 96) = 61").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr16vV = pvsd2DF.filter("pmod(DTI, 96) = 65").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr17vV = pvsd2DF.filter("pmod(DTI, 96) = 69").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr18vV = pvsd2DF.filter("pmod(DTI, 96) = 73").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr19vV = pvsd2DF.filter("pmod(DTI, 96) = 77").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr20vV = pvsd2DF.filter("pmod(DTI, 96) = 81").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr21vV = pvsd2DF.filter("pmod(DTI, 96) = 85").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr22vV = pvsd2DF.filter("pmod(DTI, 96) = 89").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      var hr23vV = pvsd2DF.filter("pmod(DTI, 96) = 93").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}

      // Generate dense Vector for each hour, containing feature vector of power features and voltage features
      var hr0vPV = Vectors.dense(Array.concat(hr0vP, hr0vV))
      var hr1vPV = Vectors.dense(Array.concat(hr1vP, hr1vV))
      var hr2vPV = Vectors.dense(Array.concat(hr2vP, hr2vV))
      var hr3vPV = Vectors.dense(Array.concat(hr3vP, hr3vV))
      var hr4vPV = Vectors.dense(Array.concat(hr4vP, hr4vV))
      var hr5vPV = Vectors.dense(Array.concat(hr5vP, hr5vV))
      var hr6vPV = Vectors.dense(Array.concat(hr6vP, hr6vV))
      var hr7vPV = Vectors.dense(Array.concat(hr7vP, hr7vV))
      var hr8vPV = Vectors.dense(Array.concat(hr8vP, hr8vV))
      var hr9vPV = Vectors.dense(Array.concat(hr9vP, hr9vV))
      var hr10vPV = Vectors.dense(Array.concat(hr10vP, hr10vV))
      var hr11vPV = Vectors.dense(Array.concat(hr11vP, hr11vV))
      var hr12vPV = Vectors.dense(Array.concat(hr12vP, hr12vV))
      var hr13vPV = Vectors.dense(Array.concat(hr13vP, hr13vV))
      var hr14vPV = Vectors.dense(Array.concat(hr14vP, hr14vV))
      var hr15vPV = Vectors.dense(Array.concat(hr15vP, hr15vV))
      var hr16vPV = Vectors.dense(Array.concat(hr16vP, hr16vV))
      var hr17vPV = Vectors.dense(Array.concat(hr17vP, hr17vV))
      var hr18vPV = Vectors.dense(Array.concat(hr18vP, hr18vV))
      var hr19vPV = Vectors.dense(Array.concat(hr19vP, hr19vV))
      var hr20vPV = Vectors.dense(Array.concat(hr20vP, hr20vV))
      var hr21vPV = Vectors.dense(Array.concat(hr21vP, hr21vV))
      var hr22vPV = Vectors.dense(Array.concat(hr22vP, hr22vV))
      var hr23vPV = Vectors.dense(Array.concat(hr23vP, hr23vV))

      // Release it from cache
      pvsd2DF.unpersist()

      // Create RDD of Vector of Bin frequency for both power and voltage data for training
      sc.parallelize(Array(hr0vPV, hr1vPV, hr2vPV, hr3vPV, hr4vPV, hr5vPV, hr6vPV, hr7vPV, hr8vPV, hr9vPV, hr10vPV,
                           hr11vPV, hr12vPV, hr13vPV, hr14vPV, hr15vPV, hr16vPV, hr17vPV, hr18vPV, hr19vPV, hr20vPV,
                           hr21vPV, hr22vPV, hr23vPV))

    }
    else {// Empty RDD
      pvsd2DF.unpersist()
      sc.emptyRDD[Vector]
    }
  }

  /**
   * A helper function to extract values
   */
  def extractBuckets(xs: Seq[Row]): Seq[(Double, Double)] = { 
    xs.map(x => (x.getDouble(0), x.getDouble(1)))
  }

  /**
   * Get 24 hourly Feature Vectors of active power and voltage. 
   * This approach calculates pmod at one pass; then group by using buckets
   * and callUDF of histogram_numeric, a hive function, on aggregated data
   *
   *   - Return: 
   *            RDD of Feature Vector containing PV information: RDD[Vector]
   */
  def get24HoursPVF2(sc: SparkContext, sqlContext: SQLContext, pvsdDF: DataFrame, id: Long, season: Int, daytype: Int) = {

    import sqlContext.implicits._

    // Drop null values and abnornal voltage data from  PV curve data; also filter loadtype and seasonal daytype
    var pvsdBuckets = pvsdDF.na.drop()
                        .filter(s"ID = $id and VOLT_C <= $volt_high and VOLT_C >= $volt_low and Season = $season and Daytype = $daytype")
                        .withColumn("bucket", pmod($"DTI", lit(96)))
                        .select("ID", "TS", "VOLT_C", "POWER", "DTI", "SDTI", "Season", "Daytype", "bucket")

    var nbp = 0

    if (daytype == 1) // weekday
        nbp = numBucketAP
    else
        nbp = numBucketAP2
 
    // Histogram of P
    var histoP = pvsdBuckets.groupBy($"bucket").agg(callUDF("histogram_numeric", toDouble($"POWER"), lit(nbp))).sort("bucket")

    // Histogram of V
    var histoV = pvsdBuckets.groupBy($"bucket").agg(callUDF("histogram_numeric", toDouble($"VOLT_C"), lit(numBucketV))).sort("bucket")

    if (!pvsdBuckets.rdd.isEmpty) { // Not Empty RDD

      var histoPRDD: RDD[(Long, Seq[(Double, Double)])] = histoP.map{
                       case Row(k: Long, hs: Seq[Row @unchecked]) => (k, extractBuckets(hs)) }  

      var histoVRDD: RDD[(Long, Seq[(Double, Double)])] = histoV.map{
                       case Row(k: Long, hs: Seq[Row @unchecked]) => (k, extractBuckets(hs)) }  

      var arrhsP = histoPRDD.collect()
      var arrhsV = histoVRDD.collect()

      var minPBuckets = arrhsP.map(a => a._2.size).reduceLeft(_ min _)
      var minVBuckets = arrhsV.map(a => a._2.size).reduceLeft(_ min _)

      // Preparing feature vector of N features per hour for each hour (total 24 hours)
      // where each feature is a bin frequency of power data 
      // Generate dense Vector for each hour, containing feature vector of power features and voltage features

      var hr0vPV = Vectors.dense(Array.concat(arrhsP(1)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                              arrhsV(1)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr1vPV = Vectors.dense(Array.concat(arrhsP(5)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                              arrhsV(5)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr2vPV = Vectors.dense(Array.concat(arrhsP(9)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                              arrhsV(9)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr3vPV = Vectors.dense(Array.concat(arrhsP(13)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                              arrhsV(13)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr4vPV = Vectors.dense(Array.concat(arrhsP(17)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                              arrhsV(17)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr5vPV = Vectors.dense(Array.concat(arrhsP(21)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                              arrhsV(21)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr6vPV = Vectors.dense(Array.concat(arrhsP(25)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                              arrhsV(25)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr7vPV = Vectors.dense(Array.concat(arrhsP(29)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                              arrhsV(29)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr8vPV = Vectors.dense(Array.concat(arrhsP(33)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                              arrhsV(33)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr9vPV = Vectors.dense(Array.concat(arrhsP(37)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                              arrhsV(37)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr10vPV = Vectors.dense(Array.concat(arrhsP(41)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(41)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr11vPV = Vectors.dense(Array.concat(arrhsP(45)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(45)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr12vPV = Vectors.dense(Array.concat(arrhsP(49)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(49)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr13vPV = Vectors.dense(Array.concat(arrhsP(53)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(53)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr14vPV = Vectors.dense(Array.concat(arrhsP(57)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(57)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr15vPV = Vectors.dense(Array.concat(arrhsP(61)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(61)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr16vPV = Vectors.dense(Array.concat(arrhsP(65)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(65)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr17vPV = Vectors.dense(Array.concat(arrhsP(69)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                               arrhsV(69)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr18vPV = Vectors.dense(Array.concat(arrhsP(73)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(73)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr19vPV = Vectors.dense(Array.concat(arrhsP(77)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                               arrhsV(77)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr20vPV = Vectors.dense(Array.concat(arrhsP(81)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(81)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr21vPV = Vectors.dense(Array.concat(arrhsP(85)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten, 
                                               arrhsV(85)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr22vPV = Vectors.dense(Array.concat(arrhsP(89)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,
                                               arrhsV(89)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))
      var hr23vPV = Vectors.dense(Array.concat(arrhsP(93)._2.map(r => Array(r._1, r._2/5.0)).toArray.flatten,  
                                               arrhsV(93)._2.map(r => Array(r._1/1000.0, r._2/100.0)).toArray.flatten))

      // Create RDD of Vector of Bin frequency for both power and voltage data for training
      sc.parallelize(Array(hr0vPV, hr1vPV, hr2vPV, hr3vPV, hr4vPV, hr5vPV, hr6vPV, hr7vPV, hr8vPV, hr9vPV, hr10vPV,
                           hr11vPV, hr12vPV, hr13vPV, hr14vPV, hr15vPV, hr16vPV, hr17vPV, hr18vPV, hr19vPV, hr20vPV,
                           hr21vPV, hr22vPV, hr23vPV))
    }
    else {// Empty RDD
      sc.emptyRDD[Vector]
    }
  }

  /**
   * Get 24 hourly Feature Vectors of active power and voltage for each meter and each season/daytype.
   * This approach calculates pmod at one pass; then group by using buckets
   * and using avg on aggregated data
   *
   *   - Return:
   *            RDD of Feature Vector containing PV information: RDD[Vector]
   */

  def get24HoursPVF3(sc: SparkContext, sqlContext: SQLContext, pvsdDF: DataFrame, id: Long, season: Int, daytype: Int) = {

    import sqlContext.implicits._

    // Initialize Vector
    var hr0vPV = Vectors.dense(0); var hr1vPV = Vectors.dense(0); var hr2vPV = Vectors.dense(0); var hr3vPV = Vectors.dense(0);
    var hr4vPV = Vectors.dense(0); var hr5vPV = Vectors.dense(0); var hr6vPV = Vectors.dense(0); var hr7vPV = Vectors.dense(0);
    var hr8vPV = Vectors.dense(0); var hr9vPV = Vectors.dense(0); var hr10vPV = Vectors.dense(0); var hr11vPV = Vectors.dense(0);
    var hr12vPV = Vectors.dense(0); var hr13vPV = Vectors.dense(0); var hr14vPV = Vectors.dense(0); var hr15vPV = Vectors.dense(0);
    var hr16vPV = Vectors.dense(0); var hr17vPV = Vectors.dense(0); var hr18vPV = Vectors.dense(0); var hr19vPV = Vectors.dense(0);
    var hr20vPV = Vectors.dense(0); var hr21vPV = Vectors.dense(0); var hr22vPV = Vectors.dense(0); var hr23vPV = Vectors.dense(0);

    // Drop null values and abnornal voltage data from  PV curve data; also filter loadtype and seasonal daytype
    var pvsdBuckets = pvsdDF.na.drop()
                        .filter(s"ID = $id and VOLT_C <= $volt_high and VOLT_C >= $volt_low and Season = $season and Daytype = $daytype")
                        .withColumn("bucket", pmod($"DTI", lit(96)))
                        .select("ID", "TS", "VOLT_C", "POWER", "DTI", "SDTI", "Season", "Daytype", "bucket")
                        .cache()

    if (!(pvsdBuckets.rdd.take(1).length == 0)) { // fastest way to check Not Empty RDD
      // P feature
      var featureP = pvsdBuckets.groupBy($"bucket").agg(avg("POWER")).sort("bucket")
	
      // V feature
      var featureV = pvsdBuckets.groupBy($"bucket").agg(avg("VOLT_C")).sort("bucket")

      var arrfp = featureP.collect
      var arrfv = featureV.collect 

      // Preparing feature vector of N features per hour for each hour (total 24 hours)
      // Generate dense Vector for each hour, containing feature vector of power features and voltage features

      if (arrfp.size == 96) {
        hr0vPV = Vectors.dense(Array(arrfp(1).getDecimal(1).toString.toDouble, arrfv(1).getDecimal(1).toString.toDouble/volt_nominal))
        hr1vPV = Vectors.dense(Array(arrfp(5).getDecimal(1).toString.toDouble, arrfv(5).getDecimal(1).toString.toDouble/volt_nominal))
        hr2vPV = Vectors.dense(Array(arrfp(9).getDecimal(1).toString.toDouble, arrfv(9).getDecimal(1).toString.toDouble/volt_nominal))
        hr3vPV = Vectors.dense(Array(arrfp(13).getDecimal(1).toString.toDouble, arrfv(13).getDecimal(1).toString.toDouble/volt_nominal))
        hr4vPV = Vectors.dense(Array(arrfp(17).getDecimal(1).toString.toDouble, arrfv(17).getDecimal(1).toString.toDouble/volt_nominal))
        hr5vPV = Vectors.dense(Array(arrfp(21).getDecimal(1).toString.toDouble, arrfv(21).getDecimal(1).toString.toDouble/volt_nominal))
        hr6vPV = Vectors.dense(Array(arrfp(25).getDecimal(1).toString.toDouble, arrfv(25).getDecimal(1).toString.toDouble/volt_nominal))
        hr7vPV = Vectors.dense(Array(arrfp(29).getDecimal(1).toString.toDouble, arrfv(29).getDecimal(1).toString.toDouble/volt_nominal))
        hr8vPV = Vectors.dense(Array(arrfp(33).getDecimal(1).toString.toDouble, arrfv(33).getDecimal(1).toString.toDouble/volt_nominal))
        hr9vPV = Vectors.dense(Array(arrfp(37).getDecimal(1).toString.toDouble, arrfv(37).getDecimal(1).toString.toDouble/volt_nominal))
        hr10vPV = Vectors.dense(Array(arrfp(41).getDecimal(1).toString.toDouble, arrfv(41).getDecimal(1).toString.toDouble/volt_nominal))
        hr11vPV = Vectors.dense(Array(arrfp(45).getDecimal(1).toString.toDouble, arrfv(45).getDecimal(1).toString.toDouble/volt_nominal))
        hr12vPV = Vectors.dense(Array(arrfp(49).getDecimal(1).toString.toDouble, arrfv(49).getDecimal(1).toString.toDouble/volt_nominal))
        hr13vPV = Vectors.dense(Array(arrfp(53).getDecimal(1).toString.toDouble, arrfv(53).getDecimal(1).toString.toDouble/volt_nominal))
        hr14vPV = Vectors.dense(Array(arrfp(57).getDecimal(1).toString.toDouble, arrfv(57).getDecimal(1).toString.toDouble/volt_nominal))
        hr15vPV = Vectors.dense(Array(arrfp(61).getDecimal(1).toString.toDouble, arrfv(61).getDecimal(1).toString.toDouble/volt_nominal))
        hr16vPV = Vectors.dense(Array(arrfp(65).getDecimal(1).toString.toDouble, arrfv(65).getDecimal(1).toString.toDouble/volt_nominal))
        hr17vPV = Vectors.dense(Array(arrfp(69).getDecimal(1).toString.toDouble, arrfv(69).getDecimal(1).toString.toDouble/volt_nominal))
        hr18vPV = Vectors.dense(Array(arrfp(73).getDecimal(1).toString.toDouble, arrfv(73).getDecimal(1).toString.toDouble/volt_nominal))
        hr19vPV = Vectors.dense(Array(arrfp(77).getDecimal(1).toString.toDouble, arrfv(77).getDecimal(1).toString.toDouble/volt_nominal))
        hr20vPV = Vectors.dense(Array(arrfp(81).getDecimal(1).toString.toDouble, arrfv(81).getDecimal(1).toString.toDouble/volt_nominal))
        hr21vPV = Vectors.dense(Array(arrfp(85).getDecimal(1).toString.toDouble, arrfv(85).getDecimal(1).toString.toDouble/volt_nominal))
        hr22vPV = Vectors.dense(Array(arrfp(89).getDecimal(1).toString.toDouble, arrfv(89).getDecimal(1).toString.toDouble/volt_nominal))
        hr23vPV = Vectors.dense(Array(arrfp(93).getDecimal(1).toString.toDouble, arrfv(93).getDecimal(1).toString.toDouble/volt_nominal))
      }
      else if (arrfp.size == 48) {
        hr0vPV = Vectors.dense(Array(arrfp(1).getDecimal(1).toString.toDouble, arrfv(1).getDecimal(1).toString.toDouble/volt_nominal))
        hr1vPV = Vectors.dense(Array(arrfp(3).getDecimal(1).toString.toDouble, arrfv(3).getDecimal(1).toString.toDouble/volt_nominal))
        hr2vPV = Vectors.dense(Array(arrfp(5).getDecimal(1).toString.toDouble, arrfv(5).getDecimal(1).toString.toDouble/volt_nominal))
        hr3vPV = Vectors.dense(Array(arrfp(7).getDecimal(1).toString.toDouble, arrfv(7).getDecimal(1).toString.toDouble/volt_nominal))
        hr4vPV = Vectors.dense(Array(arrfp(9).getDecimal(1).toString.toDouble, arrfv(9).getDecimal(1).toString.toDouble/volt_nominal))
        hr5vPV = Vectors.dense(Array(arrfp(11).getDecimal(1).toString.toDouble, arrfv(11).getDecimal(1).toString.toDouble/volt_nominal))
        hr6vPV = Vectors.dense(Array(arrfp(13).getDecimal(1).toString.toDouble, arrfv(13).getDecimal(1).toString.toDouble/volt_nominal))
        hr7vPV = Vectors.dense(Array(arrfp(15).getDecimal(1).toString.toDouble, arrfv(15).getDecimal(1).toString.toDouble/volt_nominal))
        hr8vPV = Vectors.dense(Array(arrfp(17).getDecimal(1).toString.toDouble, arrfv(17).getDecimal(1).toString.toDouble/volt_nominal))
        hr9vPV = Vectors.dense(Array(arrfp(19).getDecimal(1).toString.toDouble, arrfv(19).getDecimal(1).toString.toDouble/volt_nominal))
        hr10vPV = Vectors.dense(Array(arrfp(21).getDecimal(1).toString.toDouble, arrfv(21).getDecimal(1).toString.toDouble/volt_nominal))
        hr11vPV = Vectors.dense(Array(arrfp(23).getDecimal(1).toString.toDouble, arrfv(23).getDecimal(1).toString.toDouble/volt_nominal))
        hr12vPV = Vectors.dense(Array(arrfp(25).getDecimal(1).toString.toDouble, arrfv(25).getDecimal(1).toString.toDouble/volt_nominal))
        hr13vPV = Vectors.dense(Array(arrfp(27).getDecimal(1).toString.toDouble, arrfv(27).getDecimal(1).toString.toDouble/volt_nominal))
        hr14vPV = Vectors.dense(Array(arrfp(29).getDecimal(1).toString.toDouble, arrfv(29).getDecimal(1).toString.toDouble/volt_nominal))
        hr15vPV = Vectors.dense(Array(arrfp(31).getDecimal(1).toString.toDouble, arrfv(31).getDecimal(1).toString.toDouble/volt_nominal))
        hr16vPV = Vectors.dense(Array(arrfp(33).getDecimal(1).toString.toDouble, arrfv(33).getDecimal(1).toString.toDouble/volt_nominal))
        hr17vPV = Vectors.dense(Array(arrfp(35).getDecimal(1).toString.toDouble, arrfv(35).getDecimal(1).toString.toDouble/volt_nominal))
        hr18vPV = Vectors.dense(Array(arrfp(37).getDecimal(1).toString.toDouble, arrfv(37).getDecimal(1).toString.toDouble/volt_nominal))
        hr19vPV = Vectors.dense(Array(arrfp(39).getDecimal(1).toString.toDouble, arrfv(39).getDecimal(1).toString.toDouble/volt_nominal))
        hr20vPV = Vectors.dense(Array(arrfp(41).getDecimal(1).toString.toDouble, arrfv(41).getDecimal(1).toString.toDouble/volt_nominal))
        hr21vPV = Vectors.dense(Array(arrfp(43).getDecimal(1).toString.toDouble, arrfv(43).getDecimal(1).toString.toDouble/volt_nominal))
        hr22vPV = Vectors.dense(Array(arrfp(45).getDecimal(1).toString.toDouble, arrfv(45).getDecimal(1).toString.toDouble/volt_nominal))
        hr23vPV = Vectors.dense(Array(arrfp(47).getDecimal(1).toString.toDouble, arrfv(47).getDecimal(1).toString.toDouble/volt_nominal))

       // log.info(s"Found datapoints 48 in : $id, $season, $daytype")
      }
      else if (arrfp.size == 24) {
        hr0vPV = Vectors.dense(Array(arrfp(0).getDecimal(1).toString.toDouble, arrfv(0).getDecimal(1).toString.toDouble/volt_nominal))
        hr1vPV = Vectors.dense(Array(arrfp(1).getDecimal(1).toString.toDouble, arrfv(1).getDecimal(1).toString.toDouble/volt_nominal))
        hr2vPV = Vectors.dense(Array(arrfp(2).getDecimal(1).toString.toDouble, arrfv(2).getDecimal(1).toString.toDouble/volt_nominal))
        hr3vPV = Vectors.dense(Array(arrfp(3).getDecimal(1).toString.toDouble, arrfv(3).getDecimal(1).toString.toDouble/volt_nominal))
        hr4vPV = Vectors.dense(Array(arrfp(4).getDecimal(1).toString.toDouble, arrfv(4).getDecimal(1).toString.toDouble/volt_nominal))
        hr5vPV = Vectors.dense(Array(arrfp(5).getDecimal(1).toString.toDouble, arrfv(5).getDecimal(1).toString.toDouble/volt_nominal))
        hr6vPV = Vectors.dense(Array(arrfp(6).getDecimal(1).toString.toDouble, arrfv(6).getDecimal(1).toString.toDouble/volt_nominal))
        hr7vPV = Vectors.dense(Array(arrfp(7).getDecimal(1).toString.toDouble, arrfv(7).getDecimal(1).toString.toDouble/volt_nominal))
        hr8vPV = Vectors.dense(Array(arrfp(8).getDecimal(1).toString.toDouble, arrfv(8).getDecimal(1).toString.toDouble/volt_nominal))
        hr9vPV = Vectors.dense(Array(arrfp(9).getDecimal(1).toString.toDouble, arrfv(9).getDecimal(1).toString.toDouble/volt_nominal))
        hr10vPV = Vectors.dense(Array(arrfp(10).getDecimal(1).toString.toDouble, arrfv(10).getDecimal(1).toString.toDouble/volt_nominal))
        hr11vPV = Vectors.dense(Array(arrfp(11).getDecimal(1).toString.toDouble, arrfv(11).getDecimal(1).toString.toDouble/volt_nominal))
        hr12vPV = Vectors.dense(Array(arrfp(12).getDecimal(1).toString.toDouble, arrfv(12).getDecimal(1).toString.toDouble/volt_nominal))
        hr13vPV = Vectors.dense(Array(arrfp(13).getDecimal(1).toString.toDouble, arrfv(13).getDecimal(1).toString.toDouble/volt_nominal))
        hr14vPV = Vectors.dense(Array(arrfp(14).getDecimal(1).toString.toDouble, arrfv(14).getDecimal(1).toString.toDouble/volt_nominal))
        hr15vPV = Vectors.dense(Array(arrfp(15).getDecimal(1).toString.toDouble, arrfv(15).getDecimal(1).toString.toDouble/volt_nominal))
        hr16vPV = Vectors.dense(Array(arrfp(16).getDecimal(1).toString.toDouble, arrfv(16).getDecimal(1).toString.toDouble/volt_nominal))
        hr17vPV = Vectors.dense(Array(arrfp(17).getDecimal(1).toString.toDouble, arrfv(17).getDecimal(1).toString.toDouble/volt_nominal))
        hr18vPV = Vectors.dense(Array(arrfp(18).getDecimal(1).toString.toDouble, arrfv(18).getDecimal(1).toString.toDouble/volt_nominal))
        hr19vPV = Vectors.dense(Array(arrfp(19).getDecimal(1).toString.toDouble, arrfv(19).getDecimal(1).toString.toDouble/volt_nominal))
        hr20vPV = Vectors.dense(Array(arrfp(20).getDecimal(1).toString.toDouble, arrfv(20).getDecimal(1).toString.toDouble/volt_nominal))
        hr21vPV = Vectors.dense(Array(arrfp(21).getDecimal(1).toString.toDouble, arrfv(21).getDecimal(1).toString.toDouble/volt_nominal))
        hr22vPV = Vectors.dense(Array(arrfp(22).getDecimal(1).toString.toDouble, arrfv(22).getDecimal(1).toString.toDouble/volt_nominal))
        hr23vPV = Vectors.dense(Array(arrfp(23).getDecimal(1).toString.toDouble, arrfv(23).getDecimal(1).toString.toDouble/volt_nominal))
      }
      else { // irregular data 
        urdata += Row(id, season, daytype) 
        //log.info(s"Found irregular data: $id, $season, $daytype")
      }

      // Create RDD of Vector of Bin frequency for both power and voltage data for training
      var hrpvData = sc.parallelize(Array(hr0vPV, hr1vPV, hr2vPV, hr3vPV, hr4vPV, hr5vPV, hr6vPV, hr7vPV, hr8vPV, hr9vPV, hr10vPV,
                           hr11vPV, hr12vPV, hr13vPV, hr14vPV, hr15vPV, hr16vPV, hr17vPV, hr18vPV, hr19vPV, hr20vPV,
                           hr21vPV, hr22vPV, hr23vPV))
   
      // Release cache
      pvsdBuckets.unpersist()

      // Return RDD, and flag for non-empty
      (hrpvData, 1)
    }
    else {// Empty RDD
      pvsdBuckets.unpersist()
      (sc.emptyRDD[Vector], 0)
    }
  }


  /**
   * Get 24 hourly Feature Vectors of active power and voltage for all meter, season/daytype.
   * This approach calculates pmod at one pass; then group by using ID, buckets, season, daytype
   * and using avg on aggregated data
   *
   * Note: this method is currently in use due to the speed.
   *
   *   - Return:
   *            Arrays of Feature Vector containing PV information
   */
  def get24HoursPVFAll(sc: SparkContext, sqlContext: SQLContext, pvsdDF: DataFrame, qvsdDF: DataFrame) = {

    import sqlContext.implicits._

    var pvsdBuckets = pvsdDF.na.drop()
                        .filter(s"VOLT_C <= $volt_high and VOLT_C >= $volt_low")
                        .withColumn("bucket", pmod($"DTI", lit(96)))
                        .select("ID", "TS", "VOLT_C", "POWER", "DTI", "SDTI", "Season", "Daytype", "bucket") 

    // P feature
    var featureP = pvsdBuckets.groupBy($"ID", $"bucket", $"Season", $"Daytype").agg(avg("POWER")).sort("ID", "bucket", "Season", "Daytype").cache()
    featureP.count //force to be cached

    // V feature
    var featureV = pvsdBuckets.groupBy($"ID", $"bucket", $"Season", $"Daytype").agg(avg("VOLT_C")).sort("ID", "bucket", "Season", "Daytype").cache()
    featureV.count //force to be cached

    val arrfp = featureP.collect
    val arrfv = featureV.collect

    if (runmode == 1 || runmode == 4) 
      featureP.select("ID").distinct.coalesce(numProcesses).write.mode("overwrite").jdbc(tgturl, tblMIDs, new java.util.Properties)

    (arrfp, arrfv)
  }

 /**
   * Get 24 hourly Feature Vectors of active power and voltage for one meter at specific season/daytype.
   *
   * Note:  this method is currently in use due to the speed. 
   *
   *   - Return:
   *            Tuple2 - (RDD of Feature Vector containing PV information: RDD[Vector], nonEmptyFlag)
   */
  def getFeatureVector(sc: SparkContext, arrfp: Array[Row], arrfv: Array[Row], id: Long, se: Int, dt: Int) = {

    // Retrieve readings from array of Row given id, se, dt
    var fpi = for {
      r <- arrfp 
      if (r.getDecimal(0).longValue == id && r(2) == se && r(3) == dt ) 
    } yield {r.getDecimal(4).doubleValue}

    var fvi = for {
      r <- arrfv 
      if (r.getDecimal(0).longValue == id && r(2) == se && r(3) == dt ) 
    } yield {r.getDecimal(4).doubleValue}

    // Initialize Vector
    var hr0vPV = Vectors.dense(0); var hr1vPV = Vectors.dense(0); var hr2vPV = Vectors.dense(0); var hr3vPV = Vectors.dense(0);
    var hr4vPV = Vectors.dense(0); var hr5vPV = Vectors.dense(0); var hr6vPV = Vectors.dense(0); var hr7vPV = Vectors.dense(0);
    var hr8vPV = Vectors.dense(0); var hr9vPV = Vectors.dense(0); var hr10vPV = Vectors.dense(0); var hr11vPV = Vectors.dense(0);
    var hr12vPV = Vectors.dense(0); var hr13vPV = Vectors.dense(0); var hr14vPV = Vectors.dense(0); var hr15vPV = Vectors.dense(0);
    var hr16vPV = Vectors.dense(0); var hr17vPV = Vectors.dense(0); var hr18vPV = Vectors.dense(0); var hr19vPV = Vectors.dense(0);
    var hr20vPV = Vectors.dense(0); var hr21vPV = Vectors.dense(0); var hr22vPV = Vectors.dense(0); var hr23vPV = Vectors.dense(0);

    // Preparing feature vector of N features per hour for each hour (total 24 hours)
    // Generate dense Vector for each hour, containing feature vector of power features and voltage features

    if (fpi.size == 96) {
      hr0vPV = Vectors.dense(Array(fpi(1), fvi(1)/volt_nominal))
      hr1vPV = Vectors.dense(Array(fpi(5), fvi(5)/volt_nominal))
      hr2vPV = Vectors.dense(Array(fpi(9), fvi(9)/volt_nominal))
      hr3vPV = Vectors.dense(Array(fpi(13), fvi(13)/volt_nominal))
      hr4vPV = Vectors.dense(Array(fpi(17), fvi(17)/volt_nominal))
      hr5vPV = Vectors.dense(Array(fpi(21), fvi(21)/volt_nominal))
      hr6vPV = Vectors.dense(Array(fpi(25), fvi(25)/volt_nominal))
      hr7vPV = Vectors.dense(Array(fpi(29), fvi(29)/volt_nominal))
      hr8vPV = Vectors.dense(Array(fpi(33), fvi(33)/volt_nominal))
      hr9vPV = Vectors.dense(Array(fpi(37), fvi(37)/volt_nominal))
      hr10vPV = Vectors.dense(Array(fpi(41), fvi(41)/volt_nominal))
      hr11vPV = Vectors.dense(Array(fpi(45), fvi(45)/volt_nominal))
      hr12vPV = Vectors.dense(Array(fpi(49), fvi(49)/volt_nominal))
      hr13vPV = Vectors.dense(Array(fpi(53), fvi(53)/volt_nominal))
      hr14vPV = Vectors.dense(Array(fpi(57), fvi(57)/volt_nominal))
      hr15vPV = Vectors.dense(Array(fpi(61), fvi(61)/volt_nominal))
      hr16vPV = Vectors.dense(Array(fpi(65), fvi(65)/volt_nominal))
      hr17vPV = Vectors.dense(Array(fpi(69), fvi(69)/volt_nominal))
      hr18vPV = Vectors.dense(Array(fpi(73), fvi(73)/volt_nominal))
      hr19vPV = Vectors.dense(Array(fpi(77), fvi(77)/volt_nominal))
      hr20vPV = Vectors.dense(Array(fpi(81), fvi(81)/volt_nominal))
      hr21vPV = Vectors.dense(Array(fpi(85), fvi(85)/volt_nominal))
      hr22vPV = Vectors.dense(Array(fpi(89), fvi(89)/volt_nominal))
      hr23vPV = Vectors.dense(Array(fpi(93), fvi(93)/volt_nominal))
    }
    else if (fpi.size == 48) {
      hr0vPV = Vectors.dense(Array(fpi(1), fvi(1)/volt_nominal))
      hr1vPV = Vectors.dense(Array(fpi(3), fvi(3)/volt_nominal))
      hr2vPV = Vectors.dense(Array(fpi(5), fvi(5)/volt_nominal))
      hr3vPV = Vectors.dense(Array(fpi(7), fvi(7)/volt_nominal))
      hr4vPV = Vectors.dense(Array(fpi(9), fvi(9)/volt_nominal))
      hr5vPV = Vectors.dense(Array(fpi(11), fvi(11)/volt_nominal))
      hr6vPV = Vectors.dense(Array(fpi(13), fvi(13)/volt_nominal))
      hr7vPV = Vectors.dense(Array(fpi(15), fvi(15)/volt_nominal))
      hr8vPV = Vectors.dense(Array(fpi(17), fvi(17)/volt_nominal))
      hr9vPV = Vectors.dense(Array(fpi(19), fvi(19)/volt_nominal))
      hr10vPV = Vectors.dense(Array(fpi(21), fvi(21)/volt_nominal))
      hr11vPV = Vectors.dense(Array(fpi(23), fvi(23)/volt_nominal))
      hr12vPV = Vectors.dense(Array(fpi(25), fvi(25)/volt_nominal))
      hr13vPV = Vectors.dense(Array(fpi(27), fvi(27)/volt_nominal))
      hr14vPV = Vectors.dense(Array(fpi(29), fvi(29)/volt_nominal))
      hr15vPV = Vectors.dense(Array(fpi(31), fvi(31)/volt_nominal))
      hr16vPV = Vectors.dense(Array(fpi(33), fvi(33)/volt_nominal))
      hr17vPV = Vectors.dense(Array(fpi(35), fvi(35)/volt_nominal))
      hr18vPV = Vectors.dense(Array(fpi(37), fvi(37)/volt_nominal))
      hr19vPV = Vectors.dense(Array(fpi(39), fvi(39)/volt_nominal))
      hr20vPV = Vectors.dense(Array(fpi(41), fvi(41)/volt_nominal))
      hr21vPV = Vectors.dense(Array(fpi(43), fvi(43)/volt_nominal))
      hr22vPV = Vectors.dense(Array(fpi(45), fvi(45)/volt_nominal))
      hr23vPV = Vectors.dense(Array(fpi(47), fvi(47)/volt_nominal))

      // log.info(s"Found datapoints 48 in : $id, $se, $dt")
      println(s"Found  datapoints 48 in: $id, $se, $dt")
    }
    else if (fpi.size == 24) {
      hr0vPV = Vectors.dense(Array(fpi(0), fvi(0)/volt_nominal))
      hr1vPV = Vectors.dense(Array(fpi(1), fvi(1)/volt_nominal))
      hr2vPV = Vectors.dense(Array(fpi(2), fvi(2)/volt_nominal))
      hr3vPV = Vectors.dense(Array(fpi(3), fvi(3)/volt_nominal))
      hr4vPV = Vectors.dense(Array(fpi(4), fvi(4)/volt_nominal))
      hr5vPV = Vectors.dense(Array(fpi(5), fvi(5)/volt_nominal))
      hr6vPV = Vectors.dense(Array(fpi(6), fvi(6)/volt_nominal))
      hr7vPV = Vectors.dense(Array(fpi(7), fvi(7)/volt_nominal))
      hr8vPV = Vectors.dense(Array(fpi(8), fvi(8)/volt_nominal))
      hr9vPV = Vectors.dense(Array(fpi(9), fvi(9)/volt_nominal))
      hr10vPV = Vectors.dense(Array(fpi(10), fvi(10)/volt_nominal))
      hr11vPV = Vectors.dense(Array(fpi(11), fvi(11)/volt_nominal))
      hr12vPV = Vectors.dense(Array(fpi(12), fvi(12)/volt_nominal))
      hr13vPV = Vectors.dense(Array(fpi(13), fvi(13)/volt_nominal))
      hr14vPV = Vectors.dense(Array(fpi(14), fvi(14)/volt_nominal))
      hr15vPV = Vectors.dense(Array(fpi(15), fvi(15)/volt_nominal))
      hr16vPV = Vectors.dense(Array(fpi(16), fvi(16)/volt_nominal))
      hr17vPV = Vectors.dense(Array(fpi(17), fvi(17)/volt_nominal))
      hr18vPV = Vectors.dense(Array(fpi(18), fvi(18)/volt_nominal))
      hr19vPV = Vectors.dense(Array(fpi(19), fvi(19)/volt_nominal))
      hr20vPV = Vectors.dense(Array(fpi(20), fvi(20)/volt_nominal))
      hr21vPV = Vectors.dense(Array(fpi(21), fvi(21)/volt_nominal))
      hr22vPV = Vectors.dense(Array(fpi(22), fvi(22)/volt_nominal))
      hr23vPV = Vectors.dense(Array(fpi(23), fvi(23)/volt_nominal))
    }
    else { // irregular data 
      urdata += Row(id, se, dt) 
      //log.info(s"Found irregular data: $id, $se, $dt")
      println(s"Found irregular data: $id, $se, $dt")
    }

    if (fpi.size > 0) {
      // Create RDD of Vector of Bin frequency for both power and voltage data for training
      var hrpvData = sc.parallelize(Array(hr0vPV, hr1vPV, hr2vPV, hr3vPV, hr4vPV, hr5vPV, hr6vPV, hr7vPV, hr8vPV, hr9vPV, hr10vPV,
                         hr11vPV, hr12vPV, hr13vPV, hr14vPV, hr15vPV, hr16vPV, hr17vPV, hr18vPV, hr19vPV, hr20vPV,
                         hr21vPV, hr22vPV, hr23vPV))

      // Return RDD, and flag for non-empty
      (hrpvData, 1)
    }
    else {
      (sc.emptyRDD[Vector], 0)
    }
  }

  /**
   * k-Means clustering to compute Hour Groups 
   *
   *
   *   - Return:
   *             DataFrame of Hour Groups
   */
  def kmclust(sc: SparkContext, sqlContext: SQLContext, 
              pwrDF: DataFrame, pvsdDF: DataFrame, qvsdDF: DataFrame, 
              numClusters: Integer, numIters: Integer, numRuns: Integer) = {
   
    /*
     * 3 cluster results for one-month data (1000 meters) - seems not fit the real classifications.
     * therefore, we use the "mp_attr_code" column for load types
     */
    /*
     * 0:  [123.8857296678122,0.118494746884307,0.34315819014891186], 873
     * 1:  [912.2335701754388,0.5470634064912281,1.2927517543859652], 114
     * 2:  [3036.5732923076926,1.3335420715384614,3.026184615384616], 13
     */

    // Array of 24-Hours PV Feature for each loadtype, season, daytype
    // Access its value using getOrElse("0").asInstanceOf[Int]
    //var arrHrPVRDD = new ArrayBuffer[RDD[(Vector, Long)]]()

    // Array of k-means clustering results for each loadtype, season, daytype
    //var arrKMM     = new ArrayBuffer[KMeansModel]()
    //var arrKMMOpt  = new ArrayBuffer[Option[KMeansModel]]()

    // Array of hourly group info
    var arrHGOpt = new ArrayBuffer[Option[RDD[(Int, Iterable[Long])]]]()

    // Array of Row for Hour Group data
    val arrHGRow = new ArrayBuffer[Row]()

    // To Evaluate different k
    //(2 to 20).map(k => (k, clusteringCost(hrpvData, k))).foreach(println)

    /*
     * For each loadtypes, seasons and daytypes, run k-means clustering to get hourly groups
     */
    var clustersLSDOpt: Option[KMeansModel] = None
    var pvhgmRDDOpt: Option[RDD[(Int, Iterable[Long])]] = None
    var numHourGroup: Int = 6
    var ids = Array(0L)
   
    val schemaMid = StructType(StructField("id", LongType) ::
                               StructField("avgpwr", DoubleType) :: Nil)

    var midDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaMid)

    var mids = Array(0L)

    // Get arrays of PV feature Vector; Also populate tblMIDs 
    val (arrfp, arrfv) = get24HoursPVFAll(sc, sqlContext, pvsdDF, qvsdDF)

    // Now we get a list of meters either from config file, or all meters which will take quite long time
    if (interactiveMeter == "true") { // get meter ids from config file
      mids = meterIDs.toList.map(_.toString.toLong).toArray
    }
    else { // otherwise, get all meter ids or from table
      midDF = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> tblMIDs))
      var meterids = midDF.select("ID").map(r => r.getDecimal(0).toString.toLong).collect
      mids = meterids //.slice(1, 100)
    }
 
    // Loop for each meter/season/daytype
    for (i <- 0 until mids.size) {
      var id = mids(i)

      for (se <- 1 to numSeasons) {
        for (dt <- 1 to numDaytypes) {

          //log.info(s"Beginning processing: $id, $se, $dt")
          println(s"Beginning processing: $id, $se, $dt")

          // Retrieve feature vector given meter id, season, daytype
          var (hrpvData2, nonEmptyFlag) = getFeatureVector(sc, arrfp, arrfv, id, se, dt) 

          // Cache the feature vector
          var hrpvData = hrpvData2.cache()

          // Attache index to each hour's feature data
          var hrpvDataZI = hrpvData.zipWithIndex

          // Based on preliminary analysis, 24 hours data most likely to be grouped into 6 or 7 hourly groups 
          if (nonEmptyFlag == 1) {

            // Clusters info (in KMeansModel) - 
            // Run k-Means clustering on each meter, with each season and daytype 
            var clustersLSD = kmclustAlg(hrpvData, numHourGroup, 20, numRuns)

            // For each data point/hour-feature Vector, compute its cluster number
            var clusterPVPred = hrpvData.map(x => clustersLSD.predict(x))
      
            // Associate each hour-feature Vector with index to its cluster number
            var clusterPVMap = hrpvDataZI.zip(clusterPVPred)

            // Get the Hourly Group and its members: RDD[(Int, Iterable[Long])]
            // in the format of (1,CompactBuffer(17, 18, 19)), (5,CompactBuffer(0, 1, 2, 3, 4, 5, 6)), ...
            var pvhgmRDD = clusterPVMap.map{r => (r._2, r._1._2)}.groupByKey

            var arrHGinfo = pvhgmRDD.collect  // it's not large , so we can use collect()

            // Populate Hour Group data 
            for(hg <- 0 until arrHGinfo.size) {

              var arrhi = arrHGinfo(hg)._2.toArray // hour index array

              for(m <- 0 until arrhi.size) {
                arrHGRow += Row(id, se, dt, hg, arrhi(m))
                hgMap += (id, se, dt, arrhi(m)) -> hg
              }
            }
          }
          else {
            clustersLSDOpt = None 
            pvhgmRDDOpt = None
          }

          // Release it from cache
          hrpvData.unpersist()

          //log.info(s"Finished processing: $id, $se, $dt")
          println(s"Finished processing: $id, $se, $dt")

        }
      }
    } 

    val hgRowRDD = sc.parallelize(arrHGRow) 

    val urdataRDD = sc.parallelize(urdata)

    val schemaHG = StructType(List(StructField("ID", LongType), StructField("Season", IntegerType), StructField("Daytype", IntegerType), 
                                   StructField("hourgroup", IntegerType), StructField("hourindex", LongType)))

    val schemaurdata = StructType(List(StructField("ID", LongType), StructField("Season", IntegerType), StructField("Daytype", IntegerType))) 

    val hgDF = sqlContext.createDataFrame(hgRowRDD, schemaHG).sort("ID", "Season", "Daytype", "hourgroup", "hourindex")

    val urdataDF = sqlContext.createDataFrame(urdataRDD, schemaurdata) 

    if (runmode == 1) { // machine learning mode 
      hgDF.coalesce(numProcesses).write.mode("append").jdbc(tgturl, pgHourGroup, new java.util.Properties)
      urdataDF.coalesce(numProcesses).write.mode("append").jdbc(tgturl, "data_quality.urdata", new java.util.Properties)
      println("Finished writing to hourgroup...")   
    }

    hgDF  // Return 
  }

  /**
   * Run k-Means clustering algorithm one time 
   *
   *   - Return:
   *             clusters - KMeansModel 
   */
  def kmclustAlg(parsedData: RDD[Vector], numClusters: Integer, numIters: Integer, numRuns: Integer) = {

    // Run k-means clustering algorithm once
    KMeans.train(parsedData, numClusters, numIters, numRuns)
  }

  /**
   * Scan all PV data and mark HourGroup information using UDF toHG(). 
   * In this way, it will be faster than re-calculate for each meter 
   */
  def meterHGAll(pvsdDF: DataFrame, qvsdDF: DataFrame) = {

    val pvsdhg = pvsdDF.withColumn("hourgroup", toHG(pvsdDF("ID"), pvsdDF("DTI"), pvsdDF("Season"), pvsdDF("Daytype"))) 

    val numProcesses2 = numProcesses * 2
    
    if (runmode == 4) // machine learning mode
      pvsdhg.coalesce(numProcesses2).write.mode("append").jdbc(tgturl, pgpvsdhg, new java.util.Properties)
  }

  /**
   * Select PV data and mark HourGroup information using UDF toHG().
   * In this way, it will be faster than re-calculate for each meter
   */
  def meterHG(sqlContext: SQLContext, pvsdDF: DataFrame, qvsdDF: DataFrame) = {

    val hgdf = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> pgHourGroup))

    val hgarr = hgdf.rdd.collect

    var hgMap = scala.collection.mutable.Map[(Long, Int, Int, Long), Int]()

    for (r <- hgarr) {
      hgMap += (r.getLong(0), r.getInt(1), r.getInt(2), r.getLong(4)) -> r.getInt(3)
    }

    val hgids = hgdf.select("id").distinct.map(r => r.getLong(0)).collect

    val strFilter = hgids.mkString(",")

    val pvsdhg = pvsdDF.filter(s"ID In ($strFilter)").withColumn("hourgroup", toHG(pvsdDF("ID"), pvsdDF("DTI"), pvsdDF("Season"), pvsdDF("Daytype")))

    val qvsdhg = qvsdDF.filter(s"ID In ($strFilter)").withColumn("hourgroup", toHG(qvsdDF("ID"), qvsdDF("DTI"), qvsdDF("Season"), qvsdDF("Daytype")))

    val numProcesses2 = numProcesses * 2

    if (runmode == 1) { // machine learning mode
      pvsdhg.coalesce(numProcesses2).write.mode("overwrite").jdbc(tgturl, pgpvsdhg2, new java.util.Properties)
      qvsdhg.coalesce(numProcesses2).write.mode("overwrite").jdbc(tgturl, qgpvsdhg2, new java.util.Properties)
      println("Finished writing to pvsdhg2...")
    }
  }

  /**
   * Collect Hour Group of PV data
   * 
   * Note: It is slow to do unionAll on many DataFrames; 
   *       therefore currently not in use.
   *
   */
  def hourGroupPQV(sc: SparkContext, sqlContext: SQLContext,
                pvsdDF: DataFrame, qvsdDF: DataFrame, hgDF: DataFrame, arrHGOpt: ArrayBuffer[Option[RDD[(Int, Iterable[Long])]]]) = {

    // Initialize the array of meter ids
    var ids = Array(0L)

    // Now we get a list of meters either from config file, or all meters which will take quite long time
    if (interactiveMeter == "true") { // get meter ids from config file
      ids = meterIDs.toList.map(_.toString.toLong).toArray
    }
    else { // otherwise, get all meter ids
      ids = pvsdDF.select("ID").distinct.rdd.map{r => r.getLong(0)}.collect
    }

    val schemaPVHG = StructType(StructField("ID", LongType) ::
                                StructField("TS", TimestampType) ::
                                StructField("Season", IntegerType) ::
                                StructField("Daytype", IntegerType) ::
                                StructField("hourgroup", IntegerType) ::
                                StructField("POWER", DoubleType) ::
                                StructField("VOLT_C", DoubleType) :: Nil)

    var pvhg2DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaPVHG)
    var ai: Int = 0

    for (i <- 0 until ids.size) {
      var id = ids(i)

      for (se <- 1 to numSeasons) {
        for (dt <- 1 to numDaytypes) {

          var pvdataDF = pvsdDF.na.drop()
                               .filter(s"ID = $id and VOLT_C <= $volt_high and VOLT_C >= $volt_low and Season = $se and Daytype = $dt")
                               .cache()

          //get PV hour group Map info: pvhgmRDD.collect()
          if (!arrHGOpt(ai).isEmpty) {
            var arrHGinfo = arrHGOpt(ai).get.collect 

            for(hg <- 0 until arrHGinfo.size) {
 
              var filterStr: String = ""
              var filterStr2: String = ""

              var arrhi = arrHGinfo(hg)._2.toArray // hour index array

              for(m <- 0 until arrhi.size) {
                // from hour index, get DTI --> if hour idx = 1, then DTI = 5
                var dti = arrhi(m)*4 + 1  
                filterStr += s"pmod(DTI, 96) = $dti or pmod(DTI, 96) = $dti + 1 or pmod(DTI, 96) = $dti + 2 or pmod(DTI, 96) = $dti + 3 or "
              }

              filterStr2 = filterStr.dropRight(3) // remove last 3 chars
 
              pvhg2DF = pvhg2DF.unionAll(pvdataDF.filter(s"$filterStr2")
                               .withColumn("Season", lit(se))
                               .withColumn("Daytype", lit(dt))
                               .withColumn("hourgroup", lit(hg))
                               .select("ID", "TS", "Season", "Daytype", "hourgroup", "POWER", "VOLT_C"))
            }
          }
          ai += 1
        }
      }
    } 

    if (runmode == 4) // machine learning mode
      pvhg2DF.coalesce(numProcesses).write.mode("append").jdbc(tgturl, pgPVHG, new java.util.Properties) 
  }

  /**
   * Polynomial curve fitting
   *   y = a + b*x1 + c*x2
   */
  def polyCurveFitting(x: Array[Double], y: Array[Double], n: Int) = {

    val result = ArrayBuffer[Double]()
  
    val obs = new WeightedObservedPoints()
  
    for (i <- 0 until x.size) {
      obs.add(x(i), y(i))
    }
  
    // Instantiate a second-degree polynomial fitter.
    val fitter = PolynomialCurveFitter.create(n)

    // Retrieve fitted parameters (coefficients of the polynomial function).
    val coeff = fitter.fit(obs.toList())

    coeff
  }

}


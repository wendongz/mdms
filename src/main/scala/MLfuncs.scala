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

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Meter Data Management System utility and Machine Learning functions
 *
 */

object MLfuncs {

  // Read config parameters
  val mdmHome = scala.util.Properties.envOrElse("MDM_HOME", "MDM/")
  val config = ConfigFactory.parseFile(new File(mdmHome + "src/main/resources/application.conf"))
  val runmode = config.getInt("mdms.runmode")
  val tgturl = config.getString("mdms.tgturl")

  val numLoadtypes = config.getInt("mdms.numLoadtypes") // Industrial, Commercial, Residential
  val numSeasons   = config.getInt("mdms.numSeasons")   // Spring, Summer, Fall, Winter
  val numDaytypes  = config.getInt("mdms.numDaytypes")  // Weekday, Weekend, Holiday

  // Number of Buckets to bin a range of active power data points
  val numBucketAP  = config.getInt("mdms.numBucketAP")
  val numBucketV   = config.getInt("mdms.numBucketV")

  val volt_low  = config.getDouble("mdms.volt_low")
  val volt_high = config.getDouble("mdms.volt_high")

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
   * Get 24 hourly Feature Vectors of active power and voltage
   *
   *   - Return: 
   *            RDD of Feature Vector containing PV information: RDD[Vector]
   */
  def get24HoursPVF(sc: SparkContext, pvsdDF: DataFrame, loadtype: Int, season: Int, daytype: Int) = {

    // Drop null values and abnornal voltage data from  PV curve data; also filter loadtype and seasonal daytype
    val pvsd2DF = pvsdDF.na.drop()
                        .filter(s"VOLT_C <= $volt_high and VOLT_C >= $volt_low and Loadtype = $loadtype and Season = $season and Daytype = $daytype")
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

      val hr0vP  = pvsd2DF.filter("pmod(DTI, 96) = 1").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr1vP  = pvsd2DF.filter("pmod(DTI, 96) = 5").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr2vP  = pvsd2DF.filter("pmod(DTI, 96) = 9").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr3vP  = pvsd2DF.filter("pmod(DTI, 96) = 13").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr4vP  = pvsd2DF.filter("pmod(DTI, 96) = 17").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr5vP  = pvsd2DF.filter("pmod(DTI, 96) = 21").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr6vP  = pvsd2DF.filter("pmod(DTI, 96) = 25").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr7vP  = pvsd2DF.filter("pmod(DTI, 96) = 29").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr8vP  = pvsd2DF.filter("pmod(DTI, 96) = 33").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr9vP  = pvsd2DF.filter("pmod(DTI, 96) = 37").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr10vP = pvsd2DF.filter("pmod(DTI, 96) = 41").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr11vP = pvsd2DF.filter("pmod(DTI, 96) = 45").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr12vP = pvsd2DF.filter("pmod(DTI, 96) = 49").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr13vP = pvsd2DF.filter("pmod(DTI, 96) = 53").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr14vP = pvsd2DF.filter("pmod(DTI, 96) = 57").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr15vP = pvsd2DF.filter("pmod(DTI, 96) = 61").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr16vP = pvsd2DF.filter("pmod(DTI, 96) = 65").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr17vP = pvsd2DF.filter("pmod(DTI, 96) = 69").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr18vP = pvsd2DF.filter("pmod(DTI, 96) = 73").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr19vP = pvsd2DF.filter("pmod(DTI, 96) = 77").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr20vP = pvsd2DF.filter("pmod(DTI, 96) = 81").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr21vP = pvsd2DF.filter("pmod(DTI, 96) = 85").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr22vP = pvsd2DF.filter("pmod(DTI, 96) = 89").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
      val hr23vP = pvsd2DF.filter("pmod(DTI, 96) = 93").select("POWER")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketAP)
                          .map{r => r.toDouble}
    
      // Preparing feature vector of N features per hour for each hour (total 24 hours)
      // where each feature is a bin frequency of voltage data 

      val hr0vV  = pvsd2DF.filter("pmod(DTI, 96) = 1").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr1vV  = pvsd2DF.filter("pmod(DTI, 96) = 5").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr2vV  = pvsd2DF.filter("pmod(DTI, 96) = 9").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr3vV  = pvsd2DF.filter("pmod(DTI, 96) = 13").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr4vV  = pvsd2DF.filter("pmod(DTI, 96) = 17").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr5vV  = pvsd2DF.filter("pmod(DTI, 96) = 21").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr6vV  = pvsd2DF.filter("pmod(DTI, 96) = 25").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr7vV  = pvsd2DF.filter("pmod(DTI, 96) = 29").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr8vV  = pvsd2DF.filter("pmod(DTI, 96) = 33").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr9vV  = pvsd2DF.filter("pmod(DTI, 96) = 37").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr10vV = pvsd2DF.filter("pmod(DTI, 96) = 41").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr11vV = pvsd2DF.filter("pmod(DTI, 96) = 45").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr12vV = pvsd2DF.filter("pmod(DTI, 96) = 49").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr13vV = pvsd2DF.filter("pmod(DTI, 96) = 53").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr14vV = pvsd2DF.filter("pmod(DTI, 96) = 57").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr15vV = pvsd2DF.filter("pmod(DTI, 96) = 61").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr16vV = pvsd2DF.filter("pmod(DTI, 96) = 65").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr17vV = pvsd2DF.filter("pmod(DTI, 96) = 69").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr18vV = pvsd2DF.filter("pmod(DTI, 96) = 73").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr19vV = pvsd2DF.filter("pmod(DTI, 96) = 77").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr20vV = pvsd2DF.filter("pmod(DTI, 96) = 81").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr21vV = pvsd2DF.filter("pmod(DTI, 96) = 85").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr22vV = pvsd2DF.filter("pmod(DTI, 96) = 89").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}
      val hr23vV = pvsd2DF.filter("pmod(DTI, 96) = 93").select("VOLT_C")
                          .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                          .map{r => r.toDouble}

      // Generate dense Vector for each hour, containing feature vector of power features and voltage features
      val hr0vPV = Vectors.dense(Array.concat(hr0vP, hr0vV))
      val hr1vPV = Vectors.dense(Array.concat(hr1vP, hr1vV))
      val hr2vPV = Vectors.dense(Array.concat(hr2vP, hr2vV))
      val hr3vPV = Vectors.dense(Array.concat(hr3vP, hr3vV))
      val hr4vPV = Vectors.dense(Array.concat(hr4vP, hr4vV))
      val hr5vPV = Vectors.dense(Array.concat(hr5vP, hr5vV))
      val hr6vPV = Vectors.dense(Array.concat(hr6vP, hr6vV))
      val hr7vPV = Vectors.dense(Array.concat(hr7vP, hr7vV))
      val hr8vPV = Vectors.dense(Array.concat(hr8vP, hr8vV))
      val hr9vPV = Vectors.dense(Array.concat(hr9vP, hr9vV))
      val hr10vPV = Vectors.dense(Array.concat(hr10vP, hr10vV))
      val hr11vPV = Vectors.dense(Array.concat(hr11vP, hr11vV))
      val hr12vPV = Vectors.dense(Array.concat(hr12vP, hr12vV))
      val hr13vPV = Vectors.dense(Array.concat(hr13vP, hr13vV))
      val hr14vPV = Vectors.dense(Array.concat(hr14vP, hr14vV))
      val hr15vPV = Vectors.dense(Array.concat(hr15vP, hr15vV))
      val hr16vPV = Vectors.dense(Array.concat(hr16vP, hr16vV))
      val hr17vPV = Vectors.dense(Array.concat(hr17vP, hr17vV))
      val hr18vPV = Vectors.dense(Array.concat(hr18vP, hr18vV))
      val hr19vPV = Vectors.dense(Array.concat(hr19vP, hr19vV))
      val hr20vPV = Vectors.dense(Array.concat(hr20vP, hr20vV))
      val hr21vPV = Vectors.dense(Array.concat(hr21vP, hr21vV))
      val hr22vPV = Vectors.dense(Array.concat(hr22vP, hr22vV))
      val hr23vPV = Vectors.dense(Array.concat(hr23vP, hr23vV))

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
   * k-Means clustering main program 
   *
   *   - numRuns: Number of times to run k-mean algorithm to find a globally optimal solution
   *
   *   - Return:
   *             Tuple4
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
    var arrHrPVRDD = new ArrayBuffer[RDD[(Vector, Long)]]()

    // Array of k-means clustering results for each loadtype, season, daytype
    //var arrKMM     = new ArrayBuffer[KMeansModel]()
    var arrKMMOpt  = new ArrayBuffer[Option[KMeansModel]]()

    // Array of hourly group info
    //var arrHG = new ArrayBuffer[RDD[(Int, Iterable[Long])]]()
    var arrHGOpt = new ArrayBuffer[Option[RDD[(Int, Iterable[Long])]]]()

    // Array of Row for Hour Group data
    val arrHGRow = new ArrayBuffer[Row]()

/*
    // Get parsed data of PV feature Vector and cache
    val hrvPV = get24HoursPVF(sc, pvsdDF, 1, 1, 1).cache()

    // Get each record's index also
    val hrvPVZI = hrvPV.zipWithIndex

    // Run k-means clustering Algorithm once
    val clustersPV = kmclustAlg(hrvPV, numClusters, numIters)

    // Print centers
    for (c <- clustersPV.clusterCenters) {
      println("  " + c.toString) 
    }

    var clusterPVPred = hrvPV.map(x => clustersPV.predict(x))

    var clusterPVMap = hrvPVZI.zip(clusterPVPred)

    // Get the Hourly Group and its members 
    // in the format of (4,CompactBuffer(17, 18, 19, 22)), (5,CompactBuffer(0, 1, 2, 3, 4, 5, 6, 23)), ...
    val pvhgmRDD = clusterPVMap.map{r => (r._2, r._1._2)}.groupByKey

    // get the cluster number assigned to each point, then count the number of points in each cluster
    // such as: Map(0 -> 5, 5 -> 8, 1 -> 2, 2 -> 2, 3 -> 3, 4 -> 4)
    val pvcMap = clusterPVMap.map{r => r._2}.countByValue

    // Evaluate different k
    //(3 to 20).map(k => (k, clusteringCost(hrvPV, k))).foreach(println)
*/

    /*
     * For each loadtypes, seasons and daytypes, run k-means clustering to get hourly groups
     */
    var clustersLSDOpt: Option[KMeansModel] = None
    var pvhgmRDDOpt: Option[RDD[(Int, Iterable[Long])]] = None
    var numHourGroup: Int = 0
   
    for (lt <- 1 to numLoadtypes) {
      for (se <- 1 to numSeasons) {
        for (dt <- 1 to numDaytypes) {
          // Get parsed data of PV feature Vector and cache
          var hrpvData = get24HoursPVF(sc, pvsdDF, lt, se, dt).cache()

          // Attache index to each hour's feature data
          var hrpvDataZI = hrpvData.zipWithIndex

          arrHrPVRDD += hrpvDataZI // may contain empty RDD

          // Here we use 6/9 or 7 hourly-group to clustering 24 hours data
          if (!hrpvData.isEmpty) {

            if (lt == 3 && se == 1 && dt == 2) // Residential, Spring, Weekend 
              numHourGroup = 7
            else
              numHourGroup = 6
 
            // Clusters info (in KMeansModel)
            var clustersLSD = kmclustAlg(hrpvData, numHourGroup, 20, numRuns)

            clustersLSDOpt = Some(clustersLSD)

            // For each data point/hour-feature Vector, compute its cluster number
            var clusterPVPred = hrpvData.map(x => clustersLSD.predict(x))
      
            // Associate each hour-feature Vector with index to its cluster number
            var clusterPVMap = hrpvDataZI.zip(clusterPVPred)

            // Get the Hourly Group and its members: RDD[(Int, Iterable[Long])]
            // in the format of (1,CompactBuffer(17, 18, 19)), (5,CompactBuffer(0, 1, 2, 3, 4, 5, 6)), ...
            var pvhgmRDD = clusterPVMap.map{r => (r._2, r._1._2)}.groupByKey

            pvhgmRDDOpt = Some(pvhgmRDD)

            var arrHGinfo = pvhgmRDD.collect  // it's not large , so we can use collect()

            // Populate Hour Group data 
            for(hg <- 0 until arrHGinfo.size) {

              var arrhi = arrHGinfo(hg)._2.toArray // hour index array

              for(m <- 0 until arrhi.size) {
                arrHGRow += Row(lt, se, dt, hg, arrhi(m))
              }
            }
          }
          else {
            clustersLSDOpt = None 
            pvhgmRDDOpt = None
          }

          //arrKMM += clustersLSDOpt
          arrKMMOpt += clustersLSDOpt

          //arrHG += pvhgmRDD
          arrHGOpt += pvhgmRDDOpt

        }
      }
    } 

    val hgRowRDD = sc.parallelize(arrHGRow) 

    val schemaHG = StructType(List(StructField("Loadtype", IntegerType), StructField("Season", IntegerType), StructField("Daytype", IntegerType), 
                                   StructField("hourgroup", IntegerType), StructField("hourindex", LongType)))

    val hgDF = sqlContext.createDataFrame(hgRowRDD, schemaHG).sort("Loadtype", "Season", "Daytype", "hourgroup", "hourindex")

    if (runmode == 4) // machine learning mode 
      hgDF.write.mode("append").jdbc(tgturl, "data_quality.hourgroup", new java.util.Properties)

    (arrHrPVRDD, arrKMMOpt, arrHGOpt, hgDF)  // Return the Tuple
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
   * Collect Hour Group data
   *
   */
  def hourGroup(sc: SparkContext, sqlContext: SQLContext,
                pvsdDF: DataFrame, qvsdDF: DataFrame, hgDF: DataFrame, arrHGOpt: ArrayBuffer[Option[RDD[(Int, Iterable[Long])]]]) = {

     var loadtype = 1
     var season   = 1
     var daytype  = 1

     val arrpvhgDF = new ArrayBuffer[DataFrame]() 

     val schemaPVHG = StructType(StructField("Loadtype", IntegerType) ::
                                 StructField("Season", IntegerType) ::
                                 StructField("Daytype", IntegerType) ::
                                 StructField("hourgroup", IntegerType) ::
                                 StructField("POWER", DoubleType) ::
                                 StructField("VOLT_C", DoubleType) :: Nil)


     val pvdataDF = pvsdDF.na.drop()
                          .filter(s"VOLT_C <= $volt_high and VOLT_C >= $volt_low and Loadtype = $loadtype and Season = $season and Daytype = $daytype")
                          .cache()

     //val hghi = hgDF.filter(s"Loadtype = $loadtype and Season = $season and Daytype = $daytype")
     //               .select("hourgroup", "hourindex")
     //               .sort("hourgroup", "hourindex") 

     var pvhg2DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaPVHG)

     var arrHGinfo = arrHGOpt(0).get.collect // = pvhgmRDD.collect()

     //var pvhgDF: DataFrame = _

     for(hg <- 0 until arrHGinfo.size) {

       var filterStr: String = ""
       var filterStr2: String = ""

       var arrhi = arrHGinfo(hg)._2.toArray // hour index array

       for(m <- 0 until arrhi.size) {
         var hridx = arrhi(m) + 1 
         filterStr += s"pmod(DTI, 96) = $hridx or "
       }

       filterStr2 = filterStr.dropRight(3) // remove last 3 chars
 
       pvhg2DF = pvhg2DF.unionAll(pvdataDF.filter(s"$filterStr2")
                            .withColumn("Loadtype", lit(loadtype))
                            .withColumn("Season", lit(season))
                            .withColumn("Daytype", lit(daytype))
                            .withColumn("hourgroup", lit(hg))
                            .select("Loadtype", "Season", "Daytype", "hourgroup", "POWER", "VOLT_C"))

       //arrpvhgDF += pvhgDF 
       //pvhg2DF = pvhg2DF.unionAll(pvhgDF)
       
     }

     if (runmode == 4) // machine learning mode
       pvhg2DF.write.mode("append").jdbc(tgturl, "data_quality.pvhg", new java.util.Properties) 
  }

}


package mdmutil 

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Meter Data Management System utility and Machine Learning functions
 *
 */

object MLfuncs {

  val volt_low  = 150
  val volt_high = 300

  /**
   * Parsing category information (Commercial, Industrial, Residential)
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
   * Get hourly Feature Vectors of active power and voltage
   */
  def getHourlyPVF(sc: SparkContext, pvsdDF: DataFrame) = {

    // Number of Buckets to bin a range of active power data points
    val numBucketAP = 60
    val numBucketV = numBucketAP

    // Drop null values and abnornal voltage data from  PV curve data
    val pvsd2DF = pvsdDF.na.drop().filter(s"VOLT_C <= $volt_high and VOLT_C >= $volt_low")

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
    val hr10vV  = pvsd2DF.filter("pmod(DTI, 96) = 41").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr11vV  = pvsd2DF.filter("pmod(DTI, 96) = 45").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr12vV  = pvsd2DF.filter("pmod(DTI, 96) = 49").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr13vV  = pvsd2DF.filter("pmod(DTI, 96) = 53").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr14vV  = pvsd2DF.filter("pmod(DTI, 96) = 57").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr15vV  = pvsd2DF.filter("pmod(DTI, 96) = 61").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr16vV  = pvsd2DF.filter("pmod(DTI, 96) = 65").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr17vV  = pvsd2DF.filter("pmod(DTI, 96) = 69").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr18vV  = pvsd2DF.filter("pmod(DTI, 96) = 73").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr19vV  = pvsd2DF.filter("pmod(DTI, 96) = 77").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr20vV  = pvsd2DF.filter("pmod(DTI, 96) = 81").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr21vV  = pvsd2DF.filter("pmod(DTI, 96) = 85").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr22vV  = pvsd2DF.filter("pmod(DTI, 96) = 89").select("VOLT_C")
                      .map{r => r.getDecimal(0).toString.toDouble}.histogram(arrBucketV)
                      .map{r => r.toDouble}
    val hr23vV  = pvsd2DF.filter("pmod(DTI, 96) = 93").select("VOLT_C")
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

  /**
   * k-Means clustering main program 
   *
   */
  def kmclust(sc: SparkContext, pwrDF: DataFrame, pvsdDF: DataFrame, qvsdDF: DataFrame, numClusters: Integer, numIter: Integer) {
   
/*
    // Classify load type first - commercial, industrial, residential
    val categoryData = categoryParsing(pwrDF).cache()

    val numCategory = 3
    val clustersC = KMeans.train(categoryData, numCategory, numIter)

    var clusterCPred = categoryData.map(x=>clustersC.predict(x))
    var clusterCMap = categoryData.zip(clusterCPred)

    // get the cluster number assigned to each point, then count the number of points in each cluster 
    clusterCMap.map{r => r._2}.countByValue 
*/
    /*
     * 3 cluster results for one-month data (1000 meters) - seems not fit the real classifications.
     * therefore, we use the "mp_attr_code" column for load types
     */
    /*
       0:  [123.8857296678122,0.118494746884307,0.34315819014891186], 873
       1:  [912.2335701754388,0.5470634064912281,1.2927517543859652], 114
       2:  [3036.5732923076926,1.3335420715384614,3.026184615384616], 13
    */

    // Get parsed data of PV feature Vector and cache
    val hrvPV = getHourlyPVF(sc, pvsdDF).cache() 

    // Run k-means
    val clustersPV = KMeans.train(hrvPV, numClusters, numIter)

    // Print centers
    for (c <- clustersPV.clusterCenters) {
      println("  " + c.toString) 
    }

    var clusterPVPred = hrvPV.map(x=>clustersPV.predict(x))

    var clusterPVMap = hrvPV.zip(clusterPVPred)

    // Evaluate different k
    (3 to 20).map(k => (k, clusteringCost(hrvPV, k))).foreach(println)

    //getLoadtype()

/*
    // Clustering power consumptions
    val parsedData = powerParsing(pwrDF)
    parsedData.cache()

    val clusters = KMeans.train(parsedData, numClusters, numIter)

    val WSSSE = clusters.computeCost(parsedData)

    println("Now running k-means clustering - ................")
    println("Within Set Sum of Squared Errors = " + WSSSE)

    println("Now evaluating different k...... ")

    //(60 to 250 by 10).map(k => (k, clusteringCost(parsedData, k))).foreach(println)

    //clusters.save(sc, "/home/admin/apps/MDM/src/test/resources/Ptest-10-clusters")
*/
  }

}


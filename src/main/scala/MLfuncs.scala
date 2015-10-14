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
 * Meter Data Management System utility and ML functions
 *
 */

object MLfuncs {

  def powerParsing(powerDF: DataFrame): RDD[Vector] = {

    // Get active power data
    val power2DF = powerDF.filter("DATA_TYPE=1")
 
    val hrP = power2DF.select("P1","P5","P9","P13","P17","P21","P25","P29","P33","P37","P41","P45","P49","P53","P57","P61","P65","P69","P73","P77","P81","P85","P89","P93")

    val hrP2 = hrP.na.drop()  // dropping rows containing any null values

    // Preparing training data
    hrP2.map{r =>
      val dArray = Array(r.getDecimal(0).toString.toDouble, r.getDecimal(1).toString.toDouble, r.getDecimal(2).toString.toDouble,
                         r.getDecimal(3).toString.toDouble, r.getDecimal(4).toString.toDouble, r.getDecimal(5).toString.toDouble,
                         r.getDecimal(6).toString.toDouble, r.getDecimal(7).toString.toDouble, r.getDecimal(8).toString.toDouble,
                         r.getDecimal(9).toString.toDouble, r.getDecimal(10).toString.toDouble, r.getDecimal(11).toString.toDouble,
                         r.getDecimal(12).toString.toDouble, r.getDecimal(13).toString.toDouble, r.getDecimal(14).toString.toDouble,
                         r.getDecimal(15).toString.toDouble, r.getDecimal(16).toString.toDouble, r.getDecimal(17).toString.toDouble,
                         r.getDecimal(18).toString.toDouble, r.getDecimal(19).toString.toDouble, r.getDecimal(20).toString.toDouble,
                         r.getDecimal(21).toString.toDouble, r.getDecimal(22).toString.toDouble, r.getDecimal(23).toString.toDouble)
      Vectors.dense(dArray)
    }
  }

  def clusteringCost(parsedData: RDD[Vector], k: Integer): Double = {

    val maxIter = 300
    val clusters = KMeans.train(parsedData, k, maxIter)

    // Calculate WSSSE
    clusters.computeCost(parsedData)
  }

  def kmclust(sc: SparkContext, powerDF: DataFrame, numClusters: Integer, numIter: Integer) {
   
    val numClusters = 10 

    val parsedData = powerParsing(powerDF)
    parsedData.cache()

    val clusters = KMeans.train(parsedData, numClusters, numIter)

    val WSSSE = clusters.computeCost(parsedData)

    println("Now running k-means clustering - ................")
    println("Within Set Sum of Squared Errors = " + WSSSE)

    println("Now evaluating different k...... ")

    (60 to 250 by 10).map(k => (k, clusteringCost(parsedData, k))).foreach(println)

    //clusters.save(sc, "/home/admin/apps/MDM/src/test/resources/Ptest-10-clusters")

  }

}


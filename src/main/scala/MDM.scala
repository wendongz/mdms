/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//package org.apache.spark.examples.streaming

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

import com.databricks.spark.csv
import com.databricks.spark.csv._

import dataprocessing._
import mdmutil._ 

/**
 * Meter Data Management System
 *
 * This is a simple exmample of reading tables from SGDM using JDBC.
 * Then do operations such join, filter, select, etc.
 * Finally write data into tables in PostgreSQL using JDBC, assuming
 * there is a schema called data_quality. Tables to write will be
 * automatically created during writing. SGDM tables are in public schema
 *   in PostgreSQL.
 *
 * To run this on your local machine:
 *
 *    ./bin/spark-submit --class "MDM" --master local[*] \
 *         /home/admin/apps/MDM/target/scala-2.11/mdm_2.11-1.0.jar
 */

object MDM {

  /*
   * Read configuration parameters
   */
  val mdmHome = scala.util.Properties.envOrElse("MDM_HOME", "MDM/")
  val config = ConfigFactory.parseFile(new File(mdmHome + "src/main/resources/application.conf"))
  val runmode = config.getInt("mdms.runmode")
  val srcurl = config.getString("mdms.srcurl")
  val tgturl = config.getString("mdms.tgturl")

  val pgdti = config.getString("mdms.pgdti")
 
  val numClusters = config.getInt("mdms.numClusters")
  val numIters = config.getInt("mdms.numIters")
  val numRuns = config.getInt("mdms.numRuns")


  /**
   *  Main program of MDMS
   *
   */
  def main(args: Array[String]) {

    val pgmrdprob = "data_quality.mrdprob3"
    val pgvoltoutsum = "data_quality.voltoutsum"

    val sparkConf = new SparkConf().setAppName("MDM")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val numDays = 3258 // from historical real meter system

    /*
     * Reading data from Oracle (or PostgreSQL, Parquet, CSV, etc.)
     */
    
    // Load Meter Data from MDMS Source (Oracle) database tables into Spark DataFrame
    // the step can use key-value pairs of partitionColumn, lowerBound, upperBound, numPartitions 
    val power2DF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "YZ_E_MP_POWER_CURVE"))

    val voltDF  = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "YZ_E_MP_VOL_CURVE"))
    val curDF   = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "YZ_E_MP_CUR_CURVE"))
    val pfDF    = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "YZ_E_MP_FACTOR_CURVE"))
    val readDF  = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "YZ_E_MP_READ_CURVE"))
    val cjccDF  = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "YZ_V_CJ_CC"))

    // Load static data from SGDM PostgreSQL tables
    val rdtyDF = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> "readingtype"))
    val mskDF = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> "MEASUREMENTKIND"))
    val phaseDF = sqlContext.load("jdbc", Map("url" -> tgturl, "dbtable" -> "phase"))


    // Populate SGDM datetimeinterval table (3258 days)
    val dtiDF = DataProcessing.popDTItv(sc, sqlContext, numDays)

    if (runmode == 2) // Populate SGDM tables
      dtiDF.write.mode("append").jdbc(tgturl, pgdti, new java.util.Properties)

    // Generate DataFrame from readingtype-measurementkind-phase, and return a Map
    val rdtyMap = DataProcessing.getReadingType(sqlContext, rdtyDF, mskDF, phaseDF)

    // Processing Voltage data
    val vDF = DataProcessing.voltProcessing(sc, sqlContext, voltDF, rdtyMap)

    // Processing Active and Reactive Power data
    val pwrDF = DataProcessing.powerProcessing(sc, sqlContext, power2DF, rdtyMap)

    // Processing Current data
    DataProcessing.curProcessing(sc, sqlContext, curDF, rdtyMap)

    // Processing Power Factor data
    DataProcessing.pfProcessing(sc, sqlContext, pfDF, rdtyMap)

    // Processing Accumulated Energy data
    DataProcessing.enerProcessing(sc, sqlContext, readDF, rdtyMap)

    // Populate additional SGDM tables in PostgreSQL (identifiedobject, enddevice, meter, etc.)
    if (runmode == 2) // Populate SGDM tables
      DataProcessing.popSgdmTables(sc, sqlContext, vDF)

    // Convert Voltage Phase A,B,C rows into Column Phase_A, Phase_B, Phase_C
    val vfDF = DataProcessing.convPhaseRow2Col(sqlContext, vDF).sort("ID", "DTI")

    // Compute statistics
    val vfsumDF = DataProcessing.voltQuality(sc, sqlContext, vfDF).sort("ID", "TS")

    if (runmode == 2)  // Populate SGDM tables
      vfsumDF.write.mode("append").jdbc(tgturl, pgvoltoutsum, new java.util.Properties)

    // Compute PQ/PV curves
    val (pvsdDF, qvsdDF) = DataProcessing.PQVcurves(sc, sqlContext, vfDF, pwrDF, cjccDF)

    // Run k-means clustering on PV data
    val (arrHrPVRDD, arrKMMOpt, arrHGOpt, hgDF) = MLfuncs.kmclust(sc, sqlContext, pwrDF, pvsdDF, qvsdDF, numClusters, numIters, numRuns) 

    // Clustering all meter data using Seasonal and Daytype into Hour Groups
    MLfuncs.hourGroupPQV(sc, sqlContext, pvsdDF, qvsdDF, hgDF, arrHGOpt)

  }
}

/** Lazily instantiated singleton instance of SQLContext */

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

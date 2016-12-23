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
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

import com.databricks.spark.csv
import com.databricks.spark.csv._

import dataprocessing._
import dataprocessing.DataProcessing
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
  val numProcesses = config.getInt("mdms.numProcesses")


  /**
   *  Main program of MDMS
   *
   */
  def main(args: Array[String]) {

    val pgmrdprob = "data_quality.mrdprob3"
    val pgvoltoutsum = "data_quality.voltoutsum"

    val sparkConf = new SparkConf().setAppName("MDM")
    //sparkConf.registerKryoClasses(Array(classOf[MDM], classOf[MLfuncs], classOf[DataProcessing]))
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)

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


    val DataProcessing = new DataProcessing()

    val MLfuncs = new MLfuncs()

    // Populate SGDM datetimeinterval table (3258 days)
    val dtiDF = DataProcessing.popDTItv(sc, sqlContext, numDays)

    if (runmode == 2) { // Populate SGDM tables 
      dtiDF.coalesce(numProcesses).write.mode("append").jdbc(tgturl, pgdti, new java.util.Properties)
    }

    // Generate DataFrame from readingtype-measurementkind-phase, and return a Map
    val rdtyMap = DataProcessing.getReadingType(sqlContext, rdtyDF, mskDF, phaseDF)

    // Processing Voltage data
    val vDF = DataProcessing.voltProcessing(sc, sqlContext, voltDF, rdtyMap)
    println("Finished voltProcessing.")

    // Processing Active and Reactive Power data
    val pwrDF = DataProcessing.powerProcessing(sc, sqlContext, power2DF, rdtyMap)
    println("Finished powerProcessing.")

    // Processing Current data
    val cDF = DataProcessing.curProcessing(sc, sqlContext, curDF, rdtyMap)
    println("Finished curProcessing.")

    // Processing Power Factor data
    val factorDF = DataProcessing.pfProcessing(sc, sqlContext, pfDF, rdtyMap)
    println("Finished pfProcessing.")

    // Processing Accumulated Energy data
    val enerDF = DataProcessing.enerProcessing(sc, sqlContext, readDF, rdtyMap)
    println("Finished enerProcessing.")

    // Populate additional SGDM tables in PostgreSQL (identifiedobject, enddevice, meter, etc.)
    if (runmode == 2) { // Populate SGDM tables
      DataProcessing.popSgdmTables(sc, sqlContext, vDF)
      println("Finished popSgdmTables.")
    }

    // Convert Voltage Phase A,B,C rows into Column Phase_A, Phase_B, Phase_C
    val vfDF = DataProcessing.convPhaseRow2Col(sqlContext, vDF).sort("ID", "DTI")
    println("Finished convPhaseRow2Col.")

    // Compute statistics
    val vfsumDF = DataProcessing.voltQuality(sc, sqlContext, vfDF).sort("ID", "TS")
    println("Finished voltQuality.")

    if (runmode == 2)  // Populate SGDM tables
      vfsumDF.coalesce(numProcesses).write.mode("append").jdbc(tgturl, pgvoltoutsum, new java.util.Properties)

    // Compute PQ/PV curves
    val (pvsdDF, qvsdDF) = DataProcessing.PQVcurves(sc, sqlContext, vfDF, pwrDF, cjccDF)
    println("Finished PQVcurves.")

    // Build dataframe of PQVCF for data cross-validation
    DataProcessing.PQVCF(sc, sqlContext, vfDF, pwrDF, cDF, factorDF, enerDF)
    println("Finished PQVCF.")

    // Load modeling using k-means moved into a seperate program
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

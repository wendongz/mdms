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
  def main(args: Array[String]) {

    val oraurl = "jdbc:oracle:thin:sgdm/sgdm@//192.168.5.21:1521/orcl"
    //val pgurl  = "jdbc:postgresql://data1:5432/sgdm?user=wendong&password=wendong"
    val pgurl  = "jdbc:postgresql://192.168.5.2:5433/sgdm_for_etl?user=wendong&password=wendong"
    val pgdti = "datetimeinterval"
    val pgmrdprob = "data_quality.mrdprob3"
    val pgvoltoutsum = "data_quality.voltoutsum"

    val sparkConf = new SparkConf().setAppName("MDM")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val appDir = "./"
    val numDays = 3258 // from historical real meter system

    /*
     * Reading data from Oracle (or PostgreSQL, Parquet, CSV, etc.)
     */
    // .............
    
    // Load Meter Data from MDMS Oracle database tables into Spark DataFrame
    val powerDF = sqlContext.load("jdbc", Map("url" -> oraurl, "dbtable" -> "YZ_E_MP_POWER_CURVE"))
    val voltDF  = sqlContext.load("jdbc", Map("url" -> oraurl, "dbtable" -> "YZ_E_MP_VOL_CURVE"))
    val curDF   = sqlContext.load("jdbc", Map("url" -> oraurl, "dbtable" -> "YZ_E_MP_CUR_CURVE"))
    val pfDF    = sqlContext.load("jdbc", Map("url" -> oraurl, "dbtable" -> "YZ_E_MP_FACTOR_CURVE"))
    val readDF  = sqlContext.load("jdbc", Map("url" -> oraurl, "dbtable" -> "YZ_E_MP_READ_CURVE"))
    val cjccDF  = sqlContext.load("jdbc", Map("url" -> oraurl, "dbtable" -> "YZ_V_CJ_CC"))

    // Load static data from SGDM PostgreSQL tables
    val rdtyDF = sqlContext.load("jdbc", Map("url" -> pgurl, "dbtable" -> "readingtype"))
    val mskDF = sqlContext.load("jdbc", Map("url" -> pgurl, "dbtable" -> "MEASUREMENTKIND"))
    val phaseDF = sqlContext.load("jdbc", Map("url" -> pgurl, "dbtable" -> "phase"))


     // Populate SGDM datetimeinterval table (3258 days)
    val dtiDF = DataProcessing.popDTItv(sc, sqlContext, numDays)

    dtiDF.write.mode("append").jdbc(pgurl, pgdti, new java.util.Properties)

    // Generate DataFrame from readingtype-measurementkind-phase, and return a Map
    val rdtyMap = DataProcessing.getReadingType(sqlContext, rdtyDF, mskDF, phaseDF)

    // Processing Voltage data
    val vDF = DataProcessing.voltProcessing(sc, sqlContext, voltDF, rdtyMap)

    // Processing Active and Reactive Power data
    val pwrDF = DataProcessing.powerProcessing(sc, sqlContext, powerDF, rdtyMap)

    // Processing Current data
    DataProcessing.curProcessing(sc, sqlContext, curDF, rdtyMap)

    // Processing Power Factor data
    DataProcessing.pfProcessing(sc, sqlContext, pfDF, rdtyMap)

    // Processing Accumulated Energy data
    DataProcessing.enerProcessing(sc, sqlContext, readDF, rdtyMap)

    // Populate additional SGDM tables in PostgreSQL (identifiedobject, enddevice, meter, etc.)
    DataProcessing.popSgdmTables(sc, sqlContext, vDF)

    // Convert Voltage Phase A,B,C rows into Column Phase_A, Phase_B, Phase_C
    val vfDF = DataProcessing.convPhaseRow2Col(sqlContext, vDF).sort("ID", "DTI")

    // Compute statistics
    val vfsumDF = DataProcessing.voltQuality(sc, sqlContext, vfDF).sort("ID", "TS")

    vfsumDF.write.mode("append").jdbc(pgurl, pgvoltoutsum, new java.util.Properties)

    // Compute PQ/PV curves
    val (pvsdDF, qvsdDF) = DataProcessing.PQVcurves(sc, sqlContext, vfDF, pwrDF, cjccDF)

    // Run k-means clustering on PV data
    val numClusters = 9
    val numIter = 30

    MLfuncs.kmclust(sc, pwrDF, pvsdDF, qvsdDF, numClusters, numIter) 


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

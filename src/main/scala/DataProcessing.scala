package dataprocessing 

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp
import java.math._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.Map

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

/**
 * MDM data processing package and related functions
 *
 */

object DataProcessing {

  /* 
   * Totla number of days
   * In BJ data, starting date is 2006-01-06; ending date is 2014-12-07;
   * total 3258 days or (465 weeks and 3 days)
   */
  val numDays = 3258

  // PostgreSQL jdbc URL
  val pgurl  = "jdbc:postgresql://192.168.5.2:5433/sgdm_for_etl?user=wendong&password=wendong"

  // Defined different voltage levels
  val voltLow1 = 10.0
  val voltLow2 = 90.0
  val voltLow3 = 110.0
  val voltLow4 = 188.0
  val voltHigh = 236.0
  val voltHigh2 = 253.0
  val voltDefault = 220.0

  // table to write
  val pgtestv = "data_quality.volt"
  val pgtestvl = "data_quality.voltagelow"
  val pgtestvh = "data_quality.voltagehigh"
  val pgtestvo = "data_quality.voltageout"
  val pgtestvop = "data_quality.voltageout_phc"
  val pgdti = "data_quality.datetimeinterval"
  val pgbasereading = "basereading"

  // UDF applied to DataFrame columns
  val toIntg = udf((d: java.math.BigDecimal) => d.toString.toInt)
  val toIntg2 = udf((d: java.math.BigDecimal) => d.toString.replaceAll("""\.0+$""", "").toInt)
  val toLong = udf((d: java.math.BigDecimal) => d.toString.toLong)
  val toLong2 = udf((d: java.math.BigDecimal) => d.toString.replaceAll("""\.0+$""", "").toLong)
  val toDouble = udf((d: java.math.BigDecimal) => d.toString.toDouble)
  val toStr = udf((d: java.sql.Timestamp) => d.toString.replaceAll("""\.0$""", ""))
  val toTimestamp = udf((d: String) => Timestamp.valueOf(d))
  val toVolLow = udf((d: String) => "Voltage-Low")
  val toVolHigh = udf((d: String) => "Voltage-High")
  val toVolMissing = udf((d: String) => "Voltage-Missing")
  val toPowerLow = udf((d: String) => "Power-Low")
  val toPowerHigh = udf((d: String) => "Power-High")
  val toPowerMissing = udf((d: String) => "Power-Missing")

  // Convert columns U[1-96] to TIMEIDX (1-96) to indicate time of day
  val toTI1 = udf((d: Double) =>  1); val toTI2 = udf((d: Double) =>  2); val toTI3 = udf((d: Double) =>  3); val toTI4 = udf((d: Double) =>  4);
  val toTI5 = udf((d: Double) =>  5); val toTI6 = udf((d: Double) =>  6); val toTI7 = udf((d: Double) =>  7); val toTI8 = udf((d: Double) =>  8);
  val toTI9 = udf((d: Double) =>  9); val toTI10= udf((d: Double) => 10); val toTI11= udf((d: Double) => 11); val toTI12= udf((d: Double) => 12);
  val toTI13= udf((d: Double) => 13); val toTI14= udf((d: Double) => 14); val toTI15= udf((d: Double) => 15); val toTI16= udf((d: Double) => 16);
  val toTI17= udf((d: Double) => 17); val toTI18= udf((d: Double) => 18); val toTI19= udf((d: Double) => 19); val toTI20= udf((d: Double) => 20);
  val toTI21= udf((d: Double) => 21); val toTI22= udf((d: Double) => 22); val toTI23= udf((d: Double) => 23); val toTI24= udf((d: Double) => 24);
  val toTI25= udf((d: Double) => 25); val toTI26= udf((d: Double) => 26); val toTI27= udf((d: Double) => 27); val toTI28= udf((d: Double) => 28);
  val toTI29= udf((d: Double) => 29); val toTI30= udf((d: Double) => 30); val toTI31= udf((d: Double) => 31); val toTI32= udf((d: Double) => 32);
  val toTI33= udf((d: Double) => 33); val toTI34= udf((d: Double) => 34); val toTI35= udf((d: Double) => 35); val toTI36= udf((d: Double) => 36);
  val toTI37= udf((d: Double) => 37); val toTI38= udf((d: Double) => 38); val toTI39= udf((d: Double) => 39); val toTI40= udf((d: Double) => 40);
  val toTI41= udf((d: Double) => 41); val toTI42= udf((d: Double) => 42); val toTI43= udf((d: Double) => 43); val toTI44= udf((d: Double) => 44);
  val toTI45= udf((d: Double) => 45); val toTI46= udf((d: Double) => 46); val toTI47= udf((d: Double) => 47); val toTI48= udf((d: Double) => 48);
  val toTI49= udf((d: Double) => 49); val toTI50= udf((d: Double) => 50); val toTI51= udf((d: Double) => 51); val toTI52= udf((d: Double) => 52);
  val toTI53= udf((d: Double) => 53); val toTI54= udf((d: Double) => 54); val toTI55= udf((d: Double) => 55); val toTI56= udf((d: Double) => 56);
  val toTI57= udf((d: Double) => 57); val toTI58= udf((d: Double) => 58); val toTI59= udf((d: Double) => 59); val toTI60= udf((d: Double) => 60);
  val toTI61= udf((d: Double) => 61); val toTI62= udf((d: Double) => 62); val toTI63= udf((d: Double) => 63); val toTI64= udf((d: Double) => 64);
  val toTI65= udf((d: Double) => 65); val toTI66= udf((d: Double) => 66); val toTI67= udf((d: Double) => 67); val toTI68= udf((d: Double) => 68);
  val toTI69= udf((d: Double) => 69); val toTI70= udf((d: Double) => 70); val toTI71= udf((d: Double) => 71); val toTI72= udf((d: Double) => 72);
  val toTI73= udf((d: Double) => 73); val toTI74= udf((d: Double) => 74); val toTI75= udf((d: Double) => 75); val toTI76= udf((d: Double) => 76);
  val toTI77= udf((d: Double) => 77); val toTI78= udf((d: Double) => 78); val toTI79= udf((d: Double) => 79); val toTI80= udf((d: Double) => 80);
  val toTI81= udf((d: Double) => 81); val toTI82= udf((d: Double) => 82); val toTI83= udf((d: Double) => 83); val toTI84= udf((d: Double) => 84);
  val toTI85= udf((d: Double) => 85); val toTI86= udf((d: Double) => 86); val toTI87= udf((d: Double) => 87); val toTI88= udf((d: Double) => 88);
  val toTI89= udf((d: Double) => 89); val toTI90= udf((d: Double) => 90); val toTI91= udf((d: Double) => 91); val toTI92= udf((d: Double) => 92);
  val toTI93= udf((d: Double) => 93); val toTI94= udf((d: Double) => 94); val toTI95= udf((d: Double) => 95); val toTI96= udf((d: Double) => 96);

  // Convert two columns of Date and Timeidx into a column of Timestamp
  val conv2TS = udf((dt: Timestamp, idx: Int) => new Timestamp(dt.getTime() + (idx-1)*15*60*1000L) )

  // Build a Map of Timestamp to Idx
  var dtiMap = scala.collection.mutable.Map[Timestamp, Long]()

  val starttime = Timestamp.valueOf("2006-01-06 00:00:00")
  val starttimeL = starttime.getTime()

  var i: Long = 0
  for (i <- 1L to numDays*96L) {  // NOT 3258*24*96
      var ts = new Timestamp(starttimeL + (i-1)*15*60*1000)
      dtiMap += (ts -> i)
  }

  // Get Datetimeinvertal id from Date and Idx out of 96
  // NOte: it's faster than using dtiMap
  val getDTI  = udf((dt: Timestamp, idx: Int) => (dt.getTime() - starttimeL)/1000/3600/24 * 96L + idx )

  // Convert Timestamp column to Datetimeinterval column -- 
  // Note: it's a bit slow using dtiMap
  val conv2DTI = udf((ts: Timestamp) => dtiMap(ts)) 


  /**
   *  Process Voltage data 
   *      -find missing and null values;
   *      -find low/high voltages
   *      -find outgage data 
   */
  def voltProcessing(sc: SparkContext, sqlContext: SQLContext, voltDF: DataFrame, rdtyMap: scala.collection.mutable.Map[String, Long]) = {

    import sqlContext.implicits._

    // Convert Voltage 96 columns to rows
    val vDF = convCol2RowV(sqlContext, voltDF)  //.sort("ID", "DTI", "PHASE_FLAG")

    // Convert Phase A,B,C rows into Column Phase_A, Phase_B, Phase_C
    //val vfDF = convPhaseRow2Col(sqlContext, vDF).sort("ID", "DATA_DATE", "TIMEIDX")

    // Select low voltage data
    val voltLow4DF = vDF.filter(s"VOLT < $voltLow4 and VOLT > $voltLow3") 
    //val voltLow4DF = vfDF.filter(s"(VOLT_A < $voltLow4 and VOLT_A > $voltLow3) OR (VOLT_B < $voltLow4 and VOLT_B > $voltLow3) OR (VOLT_C < $voltLow4 and VOLT_C > $voltLow3)") 

    // Select high voltage data
    val voltHighDF = vDF.filter(s"VOLT > $voltHigh") 
    //val voltHighDF = vfDF.filter(s"VOLT_A > $voltHigh OR VOLT_B > $voltHigh OR VOLT_C > $voltHigh ") 

    // Select outage voltage data
    val voltOutDF = vDF.filter(s"VOLT < $voltLow1") 
    //val voltOutDF = vfDF.filter(s"VOLT_A < $voltLow1 OR VOLT_B < $voltLow1 OR VOLT_C < $voltLow1 ") 


    // drop null values
    val voltdnDF = voltDF.na.drop()
 
    val N = voltdnDF.count * 96

    // Calculate voltSummary statistics
    val vData = voltdnDF.select("U1","U2","U3","U4","U5","U6","U7","U8","U9","U10","U11","U12","U13","U14","U15","U16","U17","U18",
                              "U19","U20","U21","U22","U23","U24","U25","U26","U27","U28","U29","U30","U31","U32","U33","U34","U35",
                              "U36","U37","U38","U39","U40","U41","U42","U43","U44","U45","U46","U47","U48","U49","U50","U51","U52",
                              "U53","U54","U55","U56","U57","U58","U59","U60","U61","U62","U63","U64","U65","U66","U67","U68","U69",
                              "U70","U71","U72","U73","U74","U75","U76","U77","U78","U79","U80","U81","U82","U83","U84","U85","U86",
                              "U87","U88","U89","U90","U91","U92","U93","U94","U95","U96")

    val voltData = vData.map{r =>
      val dArray = Array(
           r.getDecimal(0).toString.toDouble, r.getDecimal(1).toString.toDouble, r.getDecimal(2).toString.toDouble, r.getDecimal(3).toString.toDouble,
           r.getDecimal(4).toString.toDouble, r.getDecimal(5).toString.toDouble, r.getDecimal(6).toString.toDouble, r.getDecimal(7).toString.toDouble, 
           r.getDecimal(8).toString.toDouble, r.getDecimal(9).toString.toDouble, r.getDecimal(10).toString.toDouble, r.getDecimal(11).toString.toDouble,
           r.getDecimal(12).toString.toDouble, r.getDecimal(13).toString.toDouble, r.getDecimal(14).toString.toDouble, r.getDecimal(15).toString.toDouble, 
           r.getDecimal(16).toString.toDouble, r.getDecimal(17).toString.toDouble, r.getDecimal(18).toString.toDouble, r.getDecimal(19).toString.toDouble, 
           r.getDecimal(20).toString.toDouble, r.getDecimal(21).toString.toDouble, r.getDecimal(22).toString.toDouble, r.getDecimal(23).toString.toDouble,
           r.getDecimal(24).toString.toDouble, r.getDecimal(25).toString.toDouble, r.getDecimal(26).toString.toDouble, r.getDecimal(27).toString.toDouble,
           r.getDecimal(28).toString.toDouble, r.getDecimal(29).toString.toDouble, r.getDecimal(30).toString.toDouble, r.getDecimal(31).toString.toDouble, 
           r.getDecimal(32).toString.toDouble, r.getDecimal(33).toString.toDouble, r.getDecimal(34).toString.toDouble, r.getDecimal(35).toString.toDouble,
           r.getDecimal(36).toString.toDouble, r.getDecimal(37).toString.toDouble, r.getDecimal(38).toString.toDouble, r.getDecimal(39).toString.toDouble, 
           r.getDecimal(40).toString.toDouble, r.getDecimal(41).toString.toDouble, r.getDecimal(42).toString.toDouble, r.getDecimal(43).toString.toDouble,
           r.getDecimal(44).toString.toDouble, r.getDecimal(45).toString.toDouble, r.getDecimal(46).toString.toDouble, r.getDecimal(47).toString.toDouble,
           r.getDecimal(48).toString.toDouble, r.getDecimal(49).toString.toDouble, r.getDecimal(50).toString.toDouble, r.getDecimal(51).toString.toDouble, 
           r.getDecimal(52).toString.toDouble, r.getDecimal(53).toString.toDouble, r.getDecimal(54).toString.toDouble, r.getDecimal(55).toString.toDouble,
           r.getDecimal(56).toString.toDouble, r.getDecimal(57).toString.toDouble, r.getDecimal(58).toString.toDouble, r.getDecimal(59).toString.toDouble,
           r.getDecimal(60).toString.toDouble, r.getDecimal(61).toString.toDouble, r.getDecimal(62).toString.toDouble, r.getDecimal(63).toString.toDouble,
           r.getDecimal(64).toString.toDouble, r.getDecimal(65).toString.toDouble, r.getDecimal(66).toString.toDouble, r.getDecimal(67).toString.toDouble,
           r.getDecimal(68).toString.toDouble, r.getDecimal(69).toString.toDouble, r.getDecimal(70).toString.toDouble, r.getDecimal(71).toString.toDouble,
           r.getDecimal(72).toString.toDouble, r.getDecimal(73).toString.toDouble, r.getDecimal(74).toString.toDouble, r.getDecimal(75).toString.toDouble,
           r.getDecimal(76).toString.toDouble, r.getDecimal(77).toString.toDouble, r.getDecimal(78).toString.toDouble, r.getDecimal(79).toString.toDouble,
           r.getDecimal(80).toString.toDouble, r.getDecimal(81).toString.toDouble, r.getDecimal(82).toString.toDouble, r.getDecimal(83).toString.toDouble,
           r.getDecimal(84).toString.toDouble, r.getDecimal(85).toString.toDouble, r.getDecimal(86).toString.toDouble, r.getDecimal(87).toString.toDouble,
           r.getDecimal(88).toString.toDouble, r.getDecimal(89).toString.toDouble, r.getDecimal(90).toString.toDouble, r.getDecimal(91).toString.toDouble,
           r.getDecimal(92).toString.toDouble, r.getDecimal(93).toString.toDouble, r.getDecimal(94).toString.toDouble, r.getDecimal(95).toString.toDouble)

      Vectors.dense(dArray)
    }   //.cache()

    val voltSummary: MultivariateStatisticalSummary = Statistics.colStats(voltData)

    // Populate Basereading table for Voltage data
    popBasereading(sqlContext, vDF, "V", rdtyMap)
 
    // Fill null values with some statistical values or defaults
    //val voltnDF = voltDF.na.fill(voltDefault)

    // Convert RDD to DataFrame
    //val vtDF = sqlContext.createDataFrame(voltLow4RDD, schemaVolt)
    //val voltLow4DF = sqlContext.createDataFrame(voltLow4RDD, schemaVolt)

    // Write data to PostgreSQL table
    //voltLow4DF.write.jdbc(pgurl, pgtest, new java.util.Properties)

    //voltLow4DF.write.jdbc(pgurl, pgtestvl, new java.util.Properties)
    //voltHighDF.write.jdbc(pgurl, pgtestvh, new java.util.Properties)
    //voltOutDF.write.jdbc(pgurl, pgtestvo, new java.util.Properties)

    vDF.write.mode("append").jdbc(pgurl, pgtestv, new java.util.Properties)

    //val lowVolDF = mrdsm.filter("code = 'V' and value > 190 and value < 201").withColumn("prbcode", toVolLow(mrdsm("idodesc")))
    //val highVolDF = mrdsm.filter("code = 'V' and value > 278.8 and value < 279").withColumn("prbcode", toVolHigh(mrdsm("idodesc")))
    //val missingVolDF = mrdsm.filter("code = 'V' and value < 0.1").withColumn("prbcode", toVolMissing(mrdsm("idodesc")))

  }

  /**
   *  Convert 96 Voltage columns to rows. 
   *  First, select individual U[1-96] column as DataFrame from Voltage DataFrame;
   *  In the meantime, add a column of "TIMEIDX" (1-96) to indicate time of day;
   *  Then, unionAll 96 DataFrames of U[1-96].
   */
  def convCol2RowV(sqlContext: SQLContext, voltDF: DataFrame): DataFrame = {

    import sqlContext.implicits._

    // Select individual U[1-96], then UNION to convert columns to rows
    val v1DF  = voltDF.withColumn("TIMEIDX", toTI1(voltDF("U1"))).withColumnRenamed("U1", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v2DF  = voltDF.withColumn("TIMEIDX", toTI2(voltDF("U2"))).withColumnRenamed("U2", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v3DF  = voltDF.withColumn("TIMEIDX", toTI3(voltDF("U3"))).withColumnRenamed("U3", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v4DF  = voltDF.withColumn("TIMEIDX", toTI4(voltDF("U4"))).withColumnRenamed("U4", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v5DF  = voltDF.withColumn("TIMEIDX", toTI5(voltDF("U5"))).withColumnRenamed("U5", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v6DF  = voltDF.withColumn("TIMEIDX", toTI6(voltDF("U6"))).withColumnRenamed("U6", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v7DF  = voltDF.withColumn("TIMEIDX", toTI7(voltDF("U7"))).withColumnRenamed("U7", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v8DF  = voltDF.withColumn("TIMEIDX", toTI8(voltDF("U8"))).withColumnRenamed("U8", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v9DF  = voltDF.withColumn("TIMEIDX", toTI9(voltDF("U9"))).withColumnRenamed("U9", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v10DF = voltDF.withColumn("TIMEIDX", toTI10(voltDF("U10"))).withColumnRenamed("U10", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v11DF = voltDF.withColumn("TIMEIDX", toTI11(voltDF("U11"))).withColumnRenamed("U11", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v12DF = voltDF.withColumn("TIMEIDX", toTI12(voltDF("U12"))).withColumnRenamed("U12", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v13DF = voltDF.withColumn("TIMEIDX", toTI13(voltDF("U13"))).withColumnRenamed("U13", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v14DF = voltDF.withColumn("TIMEIDX", toTI14(voltDF("U14"))).withColumnRenamed("U14", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v15DF = voltDF.withColumn("TIMEIDX", toTI15(voltDF("U15"))).withColumnRenamed("U15", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v16DF = voltDF.withColumn("TIMEIDX", toTI16(voltDF("U16"))).withColumnRenamed("U16", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v17DF = voltDF.withColumn("TIMEIDX", toTI17(voltDF("U17"))).withColumnRenamed("U17", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v18DF = voltDF.withColumn("TIMEIDX", toTI18(voltDF("U18"))).withColumnRenamed("U18", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v19DF = voltDF.withColumn("TIMEIDX", toTI19(voltDF("U19"))).withColumnRenamed("U19", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v20DF = voltDF.withColumn("TIMEIDX", toTI20(voltDF("U20"))).withColumnRenamed("U20", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v21DF = voltDF.withColumn("TIMEIDX", toTI21(voltDF("U21"))).withColumnRenamed("U21", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v22DF = voltDF.withColumn("TIMEIDX", toTI22(voltDF("U22"))).withColumnRenamed("U22", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v23DF = voltDF.withColumn("TIMEIDX", toTI23(voltDF("U23"))).withColumnRenamed("U23", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v24DF = voltDF.withColumn("TIMEIDX", toTI24(voltDF("U24"))).withColumnRenamed("U24", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v25DF = voltDF.withColumn("TIMEIDX", toTI25(voltDF("U25"))).withColumnRenamed("U25", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v26DF = voltDF.withColumn("TIMEIDX", toTI26(voltDF("U26"))).withColumnRenamed("U26", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v27DF = voltDF.withColumn("TIMEIDX", toTI27(voltDF("U27"))).withColumnRenamed("U27", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v28DF = voltDF.withColumn("TIMEIDX", toTI28(voltDF("U28"))).withColumnRenamed("U28", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v29DF = voltDF.withColumn("TIMEIDX", toTI29(voltDF("U29"))).withColumnRenamed("U29", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v30DF = voltDF.withColumn("TIMEIDX", toTI30(voltDF("U30"))).withColumnRenamed("U30", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v31DF = voltDF.withColumn("TIMEIDX", toTI31(voltDF("U31"))).withColumnRenamed("U31", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v32DF = voltDF.withColumn("TIMEIDX", toTI32(voltDF("U32"))).withColumnRenamed("U32", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v33DF = voltDF.withColumn("TIMEIDX", toTI33(voltDF("U33"))).withColumnRenamed("U33", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v34DF = voltDF.withColumn("TIMEIDX", toTI34(voltDF("U34"))).withColumnRenamed("U34", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v35DF = voltDF.withColumn("TIMEIDX", toTI35(voltDF("U35"))).withColumnRenamed("U35", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v36DF = voltDF.withColumn("TIMEIDX", toTI36(voltDF("U36"))).withColumnRenamed("U36", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v37DF = voltDF.withColumn("TIMEIDX", toTI37(voltDF("U37"))).withColumnRenamed("U37", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v38DF = voltDF.withColumn("TIMEIDX", toTI38(voltDF("U38"))).withColumnRenamed("U38", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v39DF = voltDF.withColumn("TIMEIDX", toTI39(voltDF("U39"))).withColumnRenamed("U39", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v40DF = voltDF.withColumn("TIMEIDX", toTI40(voltDF("U40"))).withColumnRenamed("U40", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v41DF = voltDF.withColumn("TIMEIDX", toTI41(voltDF("U41"))).withColumnRenamed("U41", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v42DF = voltDF.withColumn("TIMEIDX", toTI42(voltDF("U42"))).withColumnRenamed("U42", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v43DF = voltDF.withColumn("TIMEIDX", toTI43(voltDF("U43"))).withColumnRenamed("U43", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v44DF = voltDF.withColumn("TIMEIDX", toTI44(voltDF("U44"))).withColumnRenamed("U44", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v45DF = voltDF.withColumn("TIMEIDX", toTI45(voltDF("U45"))).withColumnRenamed("U45", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v46DF = voltDF.withColumn("TIMEIDX", toTI46(voltDF("U46"))).withColumnRenamed("U46", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v47DF = voltDF.withColumn("TIMEIDX", toTI47(voltDF("U47"))).withColumnRenamed("U47", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v48DF = voltDF.withColumn("TIMEIDX", toTI48(voltDF("U48"))).withColumnRenamed("U48", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v49DF = voltDF.withColumn("TIMEIDX", toTI49(voltDF("U49"))).withColumnRenamed("U49", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v50DF = voltDF.withColumn("TIMEIDX", toTI50(voltDF("U50"))).withColumnRenamed("U50", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v51DF = voltDF.withColumn("TIMEIDX", toTI51(voltDF("U51"))).withColumnRenamed("U51", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v52DF = voltDF.withColumn("TIMEIDX", toTI52(voltDF("U52"))).withColumnRenamed("U52", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v53DF = voltDF.withColumn("TIMEIDX", toTI53(voltDF("U53"))).withColumnRenamed("U53", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v54DF = voltDF.withColumn("TIMEIDX", toTI54(voltDF("U54"))).withColumnRenamed("U54", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v55DF = voltDF.withColumn("TIMEIDX", toTI55(voltDF("U55"))).withColumnRenamed("U55", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v56DF = voltDF.withColumn("TIMEIDX", toTI56(voltDF("U56"))).withColumnRenamed("U56", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v57DF = voltDF.withColumn("TIMEIDX", toTI57(voltDF("U57"))).withColumnRenamed("U57", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v58DF = voltDF.withColumn("TIMEIDX", toTI58(voltDF("U58"))).withColumnRenamed("U58", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v59DF = voltDF.withColumn("TIMEIDX", toTI59(voltDF("U59"))).withColumnRenamed("U59", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v60DF = voltDF.withColumn("TIMEIDX", toTI60(voltDF("U60"))).withColumnRenamed("U60", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v61DF = voltDF.withColumn("TIMEIDX", toTI61(voltDF("U61"))).withColumnRenamed("U61", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v62DF = voltDF.withColumn("TIMEIDX", toTI62(voltDF("U62"))).withColumnRenamed("U62", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v63DF = voltDF.withColumn("TIMEIDX", toTI63(voltDF("U63"))).withColumnRenamed("U63", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v64DF = voltDF.withColumn("TIMEIDX", toTI64(voltDF("U64"))).withColumnRenamed("U64", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v65DF = voltDF.withColumn("TIMEIDX", toTI65(voltDF("U65"))).withColumnRenamed("U65", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v66DF = voltDF.withColumn("TIMEIDX", toTI66(voltDF("U66"))).withColumnRenamed("U66", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v67DF = voltDF.withColumn("TIMEIDX", toTI67(voltDF("U67"))).withColumnRenamed("U67", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v68DF = voltDF.withColumn("TIMEIDX", toTI68(voltDF("U68"))).withColumnRenamed("U68", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v69DF = voltDF.withColumn("TIMEIDX", toTI69(voltDF("U69"))).withColumnRenamed("U69", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v70DF = voltDF.withColumn("TIMEIDX", toTI70(voltDF("U70"))).withColumnRenamed("U70", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v71DF = voltDF.withColumn("TIMEIDX", toTI71(voltDF("U71"))).withColumnRenamed("U71", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v72DF = voltDF.withColumn("TIMEIDX", toTI72(voltDF("U72"))).withColumnRenamed("U72", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v73DF = voltDF.withColumn("TIMEIDX", toTI73(voltDF("U73"))).withColumnRenamed("U73", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v74DF = voltDF.withColumn("TIMEIDX", toTI74(voltDF("U74"))).withColumnRenamed("U74", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v75DF = voltDF.withColumn("TIMEIDX", toTI75(voltDF("U75"))).withColumnRenamed("U75", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v76DF = voltDF.withColumn("TIMEIDX", toTI76(voltDF("U76"))).withColumnRenamed("U76", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v77DF = voltDF.withColumn("TIMEIDX", toTI77(voltDF("U77"))).withColumnRenamed("U77", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v78DF = voltDF.withColumn("TIMEIDX", toTI78(voltDF("U78"))).withColumnRenamed("U78", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v79DF = voltDF.withColumn("TIMEIDX", toTI79(voltDF("U79"))).withColumnRenamed("U79", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v80DF = voltDF.withColumn("TIMEIDX", toTI80(voltDF("U80"))).withColumnRenamed("U80", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v81DF = voltDF.withColumn("TIMEIDX", toTI81(voltDF("U81"))).withColumnRenamed("U81", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v82DF = voltDF.withColumn("TIMEIDX", toTI82(voltDF("U82"))).withColumnRenamed("U82", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v83DF = voltDF.withColumn("TIMEIDX", toTI83(voltDF("U83"))).withColumnRenamed("U83", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v84DF = voltDF.withColumn("TIMEIDX", toTI84(voltDF("U84"))).withColumnRenamed("U84", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v85DF = voltDF.withColumn("TIMEIDX", toTI85(voltDF("U85"))).withColumnRenamed("U85", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v86DF = voltDF.withColumn("TIMEIDX", toTI86(voltDF("U86"))).withColumnRenamed("U86", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v87DF = voltDF.withColumn("TIMEIDX", toTI87(voltDF("U87"))).withColumnRenamed("U87", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v88DF = voltDF.withColumn("TIMEIDX", toTI88(voltDF("U88"))).withColumnRenamed("U88", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v89DF = voltDF.withColumn("TIMEIDX", toTI89(voltDF("U89"))).withColumnRenamed("U89", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v90DF = voltDF.withColumn("TIMEIDX", toTI90(voltDF("U90"))).withColumnRenamed("U90", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v91DF = voltDF.withColumn("TIMEIDX", toTI91(voltDF("U91"))).withColumnRenamed("U91", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v92DF = voltDF.withColumn("TIMEIDX", toTI92(voltDF("U92"))).withColumnRenamed("U92", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v93DF = voltDF.withColumn("TIMEIDX", toTI93(voltDF("U93"))).withColumnRenamed("U93", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v94DF = voltDF.withColumn("TIMEIDX", toTI94(voltDF("U94"))).withColumnRenamed("U94", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v95DF = voltDF.withColumn("TIMEIDX", toTI95(voltDF("U95"))).withColumnRenamed("U95", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")
    val v96DF = voltDF.withColumn("TIMEIDX", toTI96(voltDF("U96"))).withColumnRenamed("U96", "VOLT").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "VOLT").withColumn("TS", conv2TS(voltDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(voltDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "VOLT")

    v1DF.unionAll(v2DF).unionAll(v3DF).unionAll(v4DF).unionAll(v5DF).unionAll(v6DF).unionAll(v7DF).unionAll(v8DF).unionAll(v9DF).unionAll(v10DF)
        .unionAll(v11DF).unionAll(v12DF).unionAll(v13DF).unionAll(v14DF).unionAll(v15DF).unionAll(v16DF).unionAll(v17DF).unionAll(v18DF).unionAll(v19DF).unionAll(v20DF)
        .unionAll(v21DF).unionAll(v22DF).unionAll(v23DF).unionAll(v24DF).unionAll(v25DF).unionAll(v26DF).unionAll(v27DF).unionAll(v28DF).unionAll(v29DF).unionAll(v30DF)
        .unionAll(v31DF).unionAll(v32DF).unionAll(v33DF).unionAll(v34DF).unionAll(v35DF).unionAll(v36DF).unionAll(v37DF).unionAll(v38DF).unionAll(v39DF).unionAll(v40DF)
        .unionAll(v41DF).unionAll(v42DF).unionAll(v43DF).unionAll(v44DF).unionAll(v45DF).unionAll(v46DF).unionAll(v47DF).unionAll(v48DF).unionAll(v49DF).unionAll(v50DF)
        .unionAll(v51DF).unionAll(v52DF).unionAll(v53DF).unionAll(v54DF).unionAll(v55DF).unionAll(v56DF).unionAll(v57DF).unionAll(v58DF).unionAll(v59DF).unionAll(v60DF)
        .unionAll(v61DF).unionAll(v62DF).unionAll(v63DF).unionAll(v64DF).unionAll(v65DF).unionAll(v66DF).unionAll(v67DF).unionAll(v68DF).unionAll(v69DF).unionAll(v70DF)
        .unionAll(v71DF).unionAll(v72DF).unionAll(v73DF).unionAll(v74DF).unionAll(v75DF).unionAll(v76DF).unionAll(v77DF).unionAll(v78DF).unionAll(v79DF).unionAll(v80DF)
        .unionAll(v81DF).unionAll(v82DF).unionAll(v83DF).unionAll(v84DF).unionAll(v85DF).unionAll(v86DF).unionAll(v87DF).unionAll(v88DF).unionAll(v89DF).unionAll(v90DF)
        .unionAll(v91DF).unionAll(v92DF).unionAll(v93DF).unionAll(v94DF).unionAll(v95DF).unionAll(v96DF)
  }

  /**
   *  Convert Phase A, B, C rows into columns Phase_A, Phase_B, Phase_C.
   *  First, use CASE ... WHEN ... ELSE ... END to construct Phase columns; 
   *  Then SELECT max() ... GROUP BY ... to remove rows containing -999.
   */
  def convPhaseRow2Col(sqlContext: SQLContext, vDF: DataFrame): DataFrame = {

    vDF.registerTempTable("VDF")

    sqlContext.sql("""SELECT ID, DATA_DATE, TIMEIDX, max(PHASEA) as VOLT_A, max(PHASEB) as VOLT_B, max(PHASEC) as VOLT_C 
                    FROM (select ID, DATA_DATE, TIMEIDX, 
                                 CASE WHEN PHASE_FLAG = 1 THEN VOLT  END PHASEA, 
                                 CASE WHEN PHASE_FLAG = 2 THEN VOLT  END PHASEB, 
                                 CASE WHEN PHASE_FLAG = 3 THEN VOLT  END PHASEC 
                          from VDF) vdf 
                    GROUP BY ID, DATA_DATE, TIMEIDX""")

  }

  /**
   *  Convert BJ meter reading data into SGDM data format and related-tables:
   *     basereading, datetimeinterval, measurementkind, readingtype, meter, etc.
   */
  def conv2SGDM(vfDF: DataFrame, apDF: DataFrame, rpDF: DataFrame) = {


  }

  /*
   * Populate Datetimeinterval table in PostgreSQL
   * In BJ data, starting date is 2006-01-06; ending date is 2014-12-07;
   * total 3258 days or (465 weeks and 3 days)
   */
  def popDTItv(sc: SparkContext, sqlContext: SQLContext, numDays: Int): DataFrame = {

    var arrRowDti = new ArrayBuffer[Row]() 
    var i: Long = 0

    for (i <- 1L to numDays*96L) {
      var ts = new Timestamp(starttimeL + (i-1)*15*60*1000) 
      arrRowDti += Row(i, null, ts)
    }
    val dtiRDD = sc.parallelize(arrRowDti)
   
    val schemaDTI = StructType(List(StructField("datetimeintervalid", LongType), StructField("start", TimestampType),  StructField(""""end"""", TimestampType)))

    sqlContext.createDataFrame(dtiRDD, schemaDTI)
  }

  /*
   * Populate Basereading table in PostgreSQL
   * In BJ data, starting date is 2006-01-06; ending date is 2014-12-07;
   * 
   */
  def popBasereading(sqlContext: SQLContext, dataDF: DataFrame, codename: String, rdtyMap: scala.collection.mutable.Map[String, Long]) = {

    // Create DataFrame Schema for basereading table
    val schemaBR = StructType(List(StructField("meter", LongType), StructField("timeperiod", LongType), StructField("readingtype", LongType), StructField("reason", LongType), 
                                   StructField("reporteddatatime", TimestampType), StructField("value", DoubleType), StructField("source", StringType)))

    if (codename == "V") {

      val rdtyidVA = rdtyMap("rdtyVA")
      val vaRDD = dataDF.filter("PHASE_FLAG = 1").select("ID", "DTI", "VOLT").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidVA, null, null, r.getDecimal(2).toString.toDouble, null))

      val vaDF = sqlContext.createDataFrame(vaRDD, schemaBR)

      val rdtyidVB = rdtyMap("rdtyVB")
      val vbRDD = dataDF.filter("PHASE_FLAG = 2").select("ID", "DTI", "VOLT").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidVB, null, null, r.getDecimal(2).toString.toDouble, null))

      val vbDF = sqlContext.createDataFrame(vbRDD, schemaBR)

      val rdtyidVC = rdtyMap("rdtyVC")
      val vcRDD = dataDF.filter("PHASE_FLAG = 3").select("ID", "DTI", "VOLT").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidVC, null, null, r.getDecimal(2).toString.toDouble, null))

      val vcDF = sqlContext.createDataFrame(vcRDD, schemaBR)

      val vtDF = vaDF.unionAll(vbDF).unionAll(vcDF)

      // Write Voltage data into PostgreSQL basereading table
      vtDF.write.mode("append").jdbc(pgurl, pgbasereading, new java.util.Properties)
    }
    else if (codename == "A") {
      val rdtyidIA = rdtyMap("rdtyIA")
      val iaRDD = dataDF.filter("PHASE_FLAG = 1").select("ID", "DTI", "CUR").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidIA, null, null, r.getDecimal(2).toString.toDouble, null))

      val iaDF = sqlContext.createDataFrame(iaRDD, schemaBR)

      val rdtyidIB = rdtyMap("rdtyIB")
      val ibRDD = dataDF.filter("PHASE_FLAG = 2").select("ID", "DTI", "CUR").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidIB, null, null, r.getDecimal(2).toString.toDouble, null))

      val ibDF = sqlContext.createDataFrame(ibRDD, schemaBR)

      val rdtyidIC = rdtyMap("rdtyIC")
      val icRDD = dataDF.filter("PHASE_FLAG = 3").select("ID", "DTI", "CUR").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidIC, null, null, r.getDecimal(2).toString.toDouble, null))

      val icDF = sqlContext.createDataFrame(icRDD, schemaBR)

      val itDF = iaDF.unionAll(ibDF).unionAll(icDF)

      // Write Voltage data into PostgreSQL basereading table
      itDF.write.mode("append").jdbc(pgurl, pgbasereading, new java.util.Properties)

    }
    else if (codename == "PF") {

      val rdtyidPF = rdtyMap("rdtyPF")
      val pfRDD = dataDF.filter("PHASE_FLAG = 0").select("ID", "DTI", "PF").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPF, null, null, r.getDecimal(2).toString.toDouble, null))
      val pfDF = sqlContext.createDataFrame(pfRDD, schemaBR)

      val rdtyidPFA = rdtyMap("rdtyPFA")
      val pfaRDD = dataDF.filter("PHASE_FLAG = 1").select("ID", "DTI", "PF").na.drop().rdd
                         .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPFA, null, null, r.getDecimal(2).toString.toDouble, null))
      val pfaDF = sqlContext.createDataFrame(pfaRDD, schemaBR)

      val rdtyidPFB = rdtyMap("rdtyPFB")
      val pfbRDD = dataDF.filter("PHASE_FLAG = 2").select("ID", "DTI", "PF").na.drop().rdd
                         .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPFB, null, null, r.getDecimal(2).toString.toDouble, null))
      val pfbDF = sqlContext.createDataFrame(pfbRDD, schemaBR)

      val rdtyidPFC = rdtyMap("rdtyPFC")
      val pfcRDD = dataDF.filter("PHASE_FLAG = 3").select("ID", "DTI", "PF").na.drop().rdd
                         .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPFC, null, null, r.getDecimal(2).toString.toDouble, null))
      val pfcDF = sqlContext.createDataFrame(pfcRDD, schemaBR)

      val pftDF = pfDF.unionAll(pfaDF).unionAll(pfbDF).unionAll(pfcDF)

      // Write Voltage data into PostgreSQL basereading table
      pftDF.write.mode("append").jdbc(pgurl, pgbasereading, new java.util.Properties)

    }
    else if (codename == "P") {

      // Prepare P, PA, PB, PC data RDD and DataFrame
      val rdtyidP = rdtyMap("rdtyP")

      val pRDD = dataDF.filter("DATA_TYPE = 1").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidP, null, null, r.getDecimal(2).toString.toDouble, null))

      val pDF = sqlContext.createDataFrame(pRDD, schemaBR)

      val rdtyidPA = rdtyMap("rdtyPA")

      val paRDD = dataDF.filter("DATA_TYPE = 2").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPA, null, null, r.getDecimal(2).toString.toDouble, null))

      val paDF = sqlContext.createDataFrame(paRDD, schemaBR)

      val rdtyidPB = rdtyMap("rdtyPB")

      val pbRDD = dataDF.filter("DATA_TYPE = 3").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPB, null, null, r.getDecimal(2).toString.toDouble, null))

      val pbDF = sqlContext.createDataFrame(pbRDD, schemaBR)

      val rdtyidPC = rdtyMap("rdtyPC")

      val pcRDD = dataDF.filter("DATA_TYPE = 4").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidPC, null, null, r.getDecimal(2).toString.toDouble, null))

      val pcDF = sqlContext.createDataFrame(pcRDD, schemaBR)

      // Prepare Q, QA, QB, QC data RDD and DataFrame
      val rdtyidQ = rdtyMap("rdtyQ")

      val qRDD = dataDF.filter("DATA_TYPE = 5").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidQ, null, null, r.getDecimal(2).toString.toDouble, null))

      val qDF = sqlContext.createDataFrame(qRDD, schemaBR)

      val rdtyidQA = rdtyMap("rdtyQA")

      val qaRDD = dataDF.filter("DATA_TYPE = 6").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidQA, null, null, r.getDecimal(2).toString.toDouble, null))

      val qaDF = sqlContext.createDataFrame(qaRDD, schemaBR)

      val rdtyidQB = rdtyMap("rdtyQB")

      val qbRDD = dataDF.filter("DATA_TYPE = 7").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidQB, null, null, r.getDecimal(2).toString.toDouble, null))

      val qbDF = sqlContext.createDataFrame(qbRDD, schemaBR)

      val rdtyidQC = rdtyMap("rdtyQC")

      val qcRDD = dataDF.filter("DATA_TYPE = 8").select("ID", "DTI", "POWER").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidQC, null, null, r.getDecimal(2).toString.toDouble, null))

      val qcDF = sqlContext.createDataFrame(qcRDD, schemaBR)

      //Union all active/reactive power
      val pqallDF = pDF.unionAll(paDF).unionAll(pbDF).unionAll(pcDF).unionAll(qDF).unionAll(qaDF).unionAll(qbDF).unionAll(qcDF)

      // Write Power data into PostgreSQL basereading table
      pqallDF.write.mode("append").jdbc(pgurl, pgbasereading, new java.util.Properties)

    }
    else if (codename == "E") {

      // Prepare Energy P reads
      val rdtyidEP = rdtyMap("rdtyEP")

      val epRDD = dataDF.filter("DATA_TYPE = 1").select("ID", "DTI", "ENERGY").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidEP, null, null, r.getDecimal(2).toString.toDouble, null))

      val epDF = sqlContext.createDataFrame(epRDD, schemaBR)

      // Prepare Energy Q reads
      val rdtyidEQ = rdtyMap("rdtyEQ")

      val eqRDD = dataDF.filter("DATA_TYPE = 2").select("ID", "DTI", "ENERGY").na.drop().rdd
                        .map(r => Row(r.getDecimal(0).toString.toLong, r.getLong(1), rdtyidEQ, null, null, r.getDecimal(2).toString.toDouble, null))

      val eqDF = sqlContext.createDataFrame(eqRDD, schemaBR)

      //Union all P/Q Energy reads
      val epqDF = epDF.unionAll(eqDF)

      // Write Energy reads into PostgreSQL basereading table
      epqDF.write.mode("append").jdbc(pgurl, pgbasereading, new java.util.Properties)

    }
    else {
      println("No action for this reading type code !!!")
    }
  }

  /*
   * Get readingtype id; also return as the following:
   * +-------------+---------+----+----+
   * |readingtypeid|phasename| tou|code|
   * +-------------+---------+----+----+
   */
  def getReadingType(sqlContext: SQLContext, rdtyDF: DataFrame, mskDF: DataFrame, phaseDF: DataFrame) = {

    import sqlContext.implicits._
  
    var rdtyMap = scala.collection.mutable.Map[String, Long]()

    // readingtype join phase
    val rdtyMeasDF = rdtyDF.as('rdty).join(phaseDF.as('phase), $"rdty.phases" === $"phase.phasecode")
                        .select($"rdty.readingtypeid" as 'readingtypeid, $"phase.code" as 'phasename, $"rdty.tou" as 'tou, $"rdty.measurementkind" as 'measkind)

    // then join measurementkind
    val rdtymphDF = rdtyMeasDF.join(mskDF.as('msk), $"measkind" === $"msk.measurementkindid").select("readingtypeid", "phasename", "tou", "code").cache()

    // Now get the readingtypeid for each type of data
    val rdtyidVA = rdtymphDF.filter("tou is null and code = 'V' and phasename LIKE '%A'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidVB = rdtymphDF.filter("tou is null and code = 'V' and phasename LIKE '%B'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidVC = rdtymphDF.filter("tou is null and code = 'V' and phasename LIKE '%C'").select("readingtypeid").rdd.map(r => r.getLong(0)).first

    val rdtyidIA = rdtymphDF.filter("tou is null and code = 'A' and phasename LIKE '%A'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidIB = rdtymphDF.filter("tou is null and code = 'A' and phasename LIKE '%B'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidIC = rdtymphDF.filter("tou is null and code = 'A' and phasename LIKE '%C'").select("readingtypeid").rdd.map(r => r.getLong(0)).first

    val rdtyidP = rdtymphDF.filter("tou is null and code = 'P' and phasename = 'none'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidPA = rdtymphDF.filter("tou is null and code = 'P' and phasename LIKE '%A'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidPB = rdtymphDF.filter("tou is null and code = 'P' and phasename LIKE '%B'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidPC = rdtymphDF.filter("tou is null and code = 'P' and phasename LIKE '%C'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidQ = rdtymphDF.filter("tou is null and code = 'Q' and phasename = 'none'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidQA = rdtymphDF.filter("tou is null and code = 'Q' and phasename LIKE '%A'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidQB = rdtymphDF.filter("tou is null and code = 'Q' and phasename LIKE '%B'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidQC = rdtymphDF.filter("tou is null and code = 'Q' and phasename LIKE '%C'").select("readingtypeid").rdd.map(r => r.getLong(0)).first

    val rdtyidPF = rdtymphDF.filter("tou is null and code = 'PF' and phasename = 'none'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidPFA = rdtymphDF.filter("tou is null and code = 'PF' and phasename LIKE '%A'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidPFB = rdtymphDF.filter("tou is null and code = 'PF' and phasename LIKE '%B'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidPFC = rdtymphDF.filter("tou is null and code = 'PF' and phasename LIKE '%C'").select("readingtypeid").rdd.map(r => r.getLong(0)).first

    val rdtyidEP = rdtymphDF.filter("tou is null and code = 'EP' and phasename = 'none'").select("readingtypeid").rdd.map(r => r.getLong(0)).first
    val rdtyidEQ = rdtymphDF.filter("tou is null and code = 'EQ' and phasename = 'none'").select("readingtypeid").rdd.map(r => r.getLong(0)).first

    // Generate Map for readingtype id
    rdtyMap += ("rdtyVA" -> rdtyidVA)
    rdtyMap += ("rdtyVB" -> rdtyidVB)
    rdtyMap += ("rdtyVC" -> rdtyidVC)
    rdtyMap += ("rdtyIA" -> rdtyidIA)
    rdtyMap += ("rdtyIB" -> rdtyidIB)
    rdtyMap += ("rdtyIC" -> rdtyidIC)
    rdtyMap += ("rdtyP"  -> rdtyidP)
    rdtyMap += ("rdtyPA" -> rdtyidPA)
    rdtyMap += ("rdtyPB" -> rdtyidPB)
    rdtyMap += ("rdtyPC" -> rdtyidPC)
    rdtyMap += ("rdtyQ"  -> rdtyidQ)
    rdtyMap += ("rdtyQA" -> rdtyidQA)
    rdtyMap += ("rdtyQB" -> rdtyidQB)
    rdtyMap += ("rdtyQC" -> rdtyidQC)
    rdtyMap += ("rdtyPF" -> rdtyidPF)
    rdtyMap += ("rdtyPFA" -> rdtyidPFA)
    rdtyMap += ("rdtyPFB" -> rdtyidPFB)
    rdtyMap += ("rdtyPFC" -> rdtyidPFC)
    rdtyMap += ("rdtyEP" -> rdtyidEP)
    rdtyMap += ("rdtyEQ" -> rdtyidEQ)

  }

  /**
   *  Process Active & Reactive Power data
   *      -find missing and null values;
   *      -find low/high active power
   *      -find outgage data
   *      -populate basereading table
   */
  def powerProcessing(sc: SparkContext, sqlContext: SQLContext, powerDF: DataFrame, rdtyMap: scala.collection.mutable.Map[String, Long]) {
   
    // Convert Active Power 96 columns to rows
    //val apDF = convCol2RowP(powerDF).sort("ID", "DATA_DATE", "TIMEIDX")

    // Convert Reactive Power 96 columns to rows
    //val rpDF = convCol2RowRP(powerDF).sort("ID", "DATA_DATE", "TIMEIDX")

    // Convert all power data of 96 columns to rows
    val pwrDF = convCol2RowPwr(sqlContext, powerDF).sort("ID", "DTI", "DATA_TYPE").cache()

    // Populate basereading table for active & reactive power data
    popBasereading(sqlContext, pwrDF, "P", rdtyMap)

  }

  /**
   *  Process Current data
   *      -find missing and null values;
   *      -find low/high current 
   *      -find outgage data
   *      -populate basereading table
   */
  def curProcessing(sc: SparkContext, sqlContext: SQLContext, curDF: DataFrame, rdtyMap: scala.collection.mutable.Map[String, Long]) {
   
    // Convert all current data of 96 columns to rows
    val cDF = convCol2RowCur(sqlContext, curDF) //.sort("ID", "DTI", "PHASE_FLAG")

    // Populate basereading table for active & reactive power data
    popBasereading(sqlContext, cDF, "A", rdtyMap)

  }

  /**
   *  Process Power Factor data
   *      -find missing and null values;
   *      -find low power factor 
   *      -populate basereading table
   */
  def pfProcessing(sc: SparkContext, sqlContext: SQLContext, pfDF: DataFrame, rdtyMap: scala.collection.mutable.Map[String, Long]) {
   
    // Convert all power factor data of 96 columns to rows
    val pftDF = convCol2RowPF(sqlContext, pfDF) //.sort("ID", "DTI", "PHASE_FLAG")

    // Populate basereading table for power factor data
    popBasereading(sqlContext, pftDF, "PF", rdtyMap)

  }

  /**
   *  Process Active & Reactive Accumulated Energy data
   *      -find missing and null values;
   *      -populate basereading table
   */
  def enerProcessing(sc: SparkContext, sqlContext: SQLContext, readDF: DataFrame, rdtyMap: scala.collection.mutable.Map[String, Long]) {
   
    // Convert all power data of 96 columns to rows
    val enerDF = convCol2RowE(sqlContext, readDF)

    // Populate basereading table for active & reactive Accumulated Energy data
    popBasereading(sqlContext, enerDF, "E", rdtyMap)

  }

  /**
   *  Convert 96 Power columns to rows. 
   *  First, select individual P[1-96] column as DataFrame from Power DataFrame, no filtering - all types of data ;
   *  In the meantime, add a column of "TIMEIDX" (1-96) to indicate time of day;
   *  Then, unionAll 96 DataFrames of P[1-96].
   */
  def convCol2RowPwr(sqlContext: SQLContext, powerDF: DataFrame): DataFrame = {

    import sqlContext.implicits._

    // Select individual P[1-96], then UNION to convert columns to rows
    val p1DF  = powerDF.withColumn("TIMEIDX", toTI1(powerDF("P1"))).withColumnRenamed("P1", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p2DF  = powerDF.withColumn("TIMEIDX", toTI2(powerDF("P2"))).withColumnRenamed("P2", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p3DF  = powerDF.withColumn("TIMEIDX", toTI3(powerDF("P3"))).withColumnRenamed("P3", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p4DF  = powerDF.withColumn("TIMEIDX", toTI4(powerDF("P4"))).withColumnRenamed("P4", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p5DF  = powerDF.withColumn("TIMEIDX", toTI5(powerDF("P5"))).withColumnRenamed("P5", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p6DF  = powerDF.withColumn("TIMEIDX", toTI6(powerDF("P6"))).withColumnRenamed("P6", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p7DF  = powerDF.withColumn("TIMEIDX", toTI7(powerDF("P7"))).withColumnRenamed("P7", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p8DF  = powerDF.withColumn("TIMEIDX", toTI8(powerDF("P8"))).withColumnRenamed("P8", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p9DF  = powerDF.withColumn("TIMEIDX", toTI9(powerDF("P9"))).withColumnRenamed("P9", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p10DF = powerDF.withColumn("TIMEIDX", toTI10(powerDF("P10"))).withColumnRenamed("P10", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p11DF = powerDF.withColumn("TIMEIDX", toTI11(powerDF("P11"))).withColumnRenamed("P11", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p12DF = powerDF.withColumn("TIMEIDX", toTI12(powerDF("P12"))).withColumnRenamed("P12", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p13DF = powerDF.withColumn("TIMEIDX", toTI13(powerDF("P13"))).withColumnRenamed("P13", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p14DF = powerDF.withColumn("TIMEIDX", toTI14(powerDF("P14"))).withColumnRenamed("P14", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p15DF = powerDF.withColumn("TIMEIDX", toTI15(powerDF("P15"))).withColumnRenamed("P15", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p16DF = powerDF.withColumn("TIMEIDX", toTI16(powerDF("P16"))).withColumnRenamed("P16", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p17DF = powerDF.withColumn("TIMEIDX", toTI17(powerDF("P17"))).withColumnRenamed("P17", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p18DF = powerDF.withColumn("TIMEIDX", toTI18(powerDF("P18"))).withColumnRenamed("P18", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p19DF = powerDF.withColumn("TIMEIDX", toTI19(powerDF("P19"))).withColumnRenamed("P19", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p20DF = powerDF.withColumn("TIMEIDX", toTI20(powerDF("P20"))).withColumnRenamed("P20", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p21DF = powerDF.withColumn("TIMEIDX", toTI21(powerDF("P21"))).withColumnRenamed("P21", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p22DF = powerDF.withColumn("TIMEIDX", toTI22(powerDF("P22"))).withColumnRenamed("P22", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p23DF = powerDF.withColumn("TIMEIDX", toTI23(powerDF("P23"))).withColumnRenamed("P23", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p24DF = powerDF.withColumn("TIMEIDX", toTI24(powerDF("P24"))).withColumnRenamed("P24", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p25DF = powerDF.withColumn("TIMEIDX", toTI25(powerDF("P25"))).withColumnRenamed("P25", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p26DF = powerDF.withColumn("TIMEIDX", toTI26(powerDF("P26"))).withColumnRenamed("P26", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p27DF = powerDF.withColumn("TIMEIDX", toTI27(powerDF("P27"))).withColumnRenamed("P27", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p28DF = powerDF.withColumn("TIMEIDX", toTI28(powerDF("P28"))).withColumnRenamed("P28", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p29DF = powerDF.withColumn("TIMEIDX", toTI29(powerDF("P29"))).withColumnRenamed("P29", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p30DF = powerDF.withColumn("TIMEIDX", toTI30(powerDF("P30"))).withColumnRenamed("P30", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p31DF = powerDF.withColumn("TIMEIDX", toTI31(powerDF("P31"))).withColumnRenamed("P31", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p32DF = powerDF.withColumn("TIMEIDX", toTI32(powerDF("P32"))).withColumnRenamed("P32", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p33DF = powerDF.withColumn("TIMEIDX", toTI33(powerDF("P33"))).withColumnRenamed("P33", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p34DF = powerDF.withColumn("TIMEIDX", toTI34(powerDF("P34"))).withColumnRenamed("P34", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p35DF = powerDF.withColumn("TIMEIDX", toTI35(powerDF("P35"))).withColumnRenamed("P35", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p36DF = powerDF.withColumn("TIMEIDX", toTI36(powerDF("P36"))).withColumnRenamed("P36", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p37DF = powerDF.withColumn("TIMEIDX", toTI37(powerDF("P37"))).withColumnRenamed("P37", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p38DF = powerDF.withColumn("TIMEIDX", toTI38(powerDF("P38"))).withColumnRenamed("P38", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p39DF = powerDF.withColumn("TIMEIDX", toTI39(powerDF("P39"))).withColumnRenamed("P39", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p40DF = powerDF.withColumn("TIMEIDX", toTI40(powerDF("P40"))).withColumnRenamed("P40", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p41DF = powerDF.withColumn("TIMEIDX", toTI41(powerDF("P41"))).withColumnRenamed("P41", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p42DF = powerDF.withColumn("TIMEIDX", toTI42(powerDF("P42"))).withColumnRenamed("P42", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p43DF = powerDF.withColumn("TIMEIDX", toTI43(powerDF("P43"))).withColumnRenamed("P43", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p44DF = powerDF.withColumn("TIMEIDX", toTI44(powerDF("P44"))).withColumnRenamed("P44", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p45DF = powerDF.withColumn("TIMEIDX", toTI45(powerDF("P45"))).withColumnRenamed("P45", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p46DF = powerDF.withColumn("TIMEIDX", toTI46(powerDF("P46"))).withColumnRenamed("P46", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p47DF = powerDF.withColumn("TIMEIDX", toTI47(powerDF("P47"))).withColumnRenamed("P47", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p48DF = powerDF.withColumn("TIMEIDX", toTI48(powerDF("P48"))).withColumnRenamed("P48", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p49DF = powerDF.withColumn("TIMEIDX", toTI49(powerDF("P49"))).withColumnRenamed("P49", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p50DF = powerDF.withColumn("TIMEIDX", toTI50(powerDF("P50"))).withColumnRenamed("P50", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p51DF = powerDF.withColumn("TIMEIDX", toTI51(powerDF("P51"))).withColumnRenamed("P51", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p52DF = powerDF.withColumn("TIMEIDX", toTI52(powerDF("P52"))).withColumnRenamed("P52", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p53DF = powerDF.withColumn("TIMEIDX", toTI53(powerDF("P53"))).withColumnRenamed("P53", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p54DF = powerDF.withColumn("TIMEIDX", toTI54(powerDF("P54"))).withColumnRenamed("P54", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p55DF = powerDF.withColumn("TIMEIDX", toTI55(powerDF("P55"))).withColumnRenamed("P55", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p56DF = powerDF.withColumn("TIMEIDX", toTI56(powerDF("P56"))).withColumnRenamed("P56", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p57DF = powerDF.withColumn("TIMEIDX", toTI57(powerDF("P57"))).withColumnRenamed("P57", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p58DF = powerDF.withColumn("TIMEIDX", toTI58(powerDF("P58"))).withColumnRenamed("P58", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p59DF = powerDF.withColumn("TIMEIDX", toTI59(powerDF("P59"))).withColumnRenamed("P59", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p60DF = powerDF.withColumn("TIMEIDX", toTI60(powerDF("P60"))).withColumnRenamed("P60", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p61DF = powerDF.withColumn("TIMEIDX", toTI61(powerDF("P61"))).withColumnRenamed("P61", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p62DF = powerDF.withColumn("TIMEIDX", toTI62(powerDF("P62"))).withColumnRenamed("P62", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p63DF = powerDF.withColumn("TIMEIDX", toTI63(powerDF("P63"))).withColumnRenamed("P63", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p64DF = powerDF.withColumn("TIMEIDX", toTI64(powerDF("P64"))).withColumnRenamed("P64", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p65DF = powerDF.withColumn("TIMEIDX", toTI65(powerDF("P65"))).withColumnRenamed("P65", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p66DF = powerDF.withColumn("TIMEIDX", toTI66(powerDF("P66"))).withColumnRenamed("P66", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p67DF = powerDF.withColumn("TIMEIDX", toTI67(powerDF("P67"))).withColumnRenamed("P67", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p68DF = powerDF.withColumn("TIMEIDX", toTI68(powerDF("P68"))).withColumnRenamed("P68", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p69DF = powerDF.withColumn("TIMEIDX", toTI69(powerDF("P69"))).withColumnRenamed("P69", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p70DF = powerDF.withColumn("TIMEIDX", toTI70(powerDF("P70"))).withColumnRenamed("P70", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p71DF = powerDF.withColumn("TIMEIDX", toTI71(powerDF("P71"))).withColumnRenamed("P71", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p72DF = powerDF.withColumn("TIMEIDX", toTI72(powerDF("P72"))).withColumnRenamed("P72", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p73DF = powerDF.withColumn("TIMEIDX", toTI73(powerDF("P73"))).withColumnRenamed("P73", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p74DF = powerDF.withColumn("TIMEIDX", toTI74(powerDF("P74"))).withColumnRenamed("P74", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p75DF = powerDF.withColumn("TIMEIDX", toTI75(powerDF("P75"))).withColumnRenamed("P75", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p76DF = powerDF.withColumn("TIMEIDX", toTI76(powerDF("P76"))).withColumnRenamed("P76", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p77DF = powerDF.withColumn("TIMEIDX", toTI77(powerDF("P77"))).withColumnRenamed("P77", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p78DF = powerDF.withColumn("TIMEIDX", toTI78(powerDF("P78"))).withColumnRenamed("P78", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p79DF = powerDF.withColumn("TIMEIDX", toTI79(powerDF("P79"))).withColumnRenamed("P79", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p80DF = powerDF.withColumn("TIMEIDX", toTI80(powerDF("P80"))).withColumnRenamed("P80", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p81DF = powerDF.withColumn("TIMEIDX", toTI81(powerDF("P81"))).withColumnRenamed("P81", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p82DF = powerDF.withColumn("TIMEIDX", toTI82(powerDF("P82"))).withColumnRenamed("P82", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p83DF = powerDF.withColumn("TIMEIDX", toTI83(powerDF("P83"))).withColumnRenamed("P83", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p84DF = powerDF.withColumn("TIMEIDX", toTI84(powerDF("P84"))).withColumnRenamed("P84", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p85DF = powerDF.withColumn("TIMEIDX", toTI85(powerDF("P85"))).withColumnRenamed("P85", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p86DF = powerDF.withColumn("TIMEIDX", toTI86(powerDF("P86"))).withColumnRenamed("P86", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p87DF = powerDF.withColumn("TIMEIDX", toTI87(powerDF("P87"))).withColumnRenamed("P87", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p88DF = powerDF.withColumn("TIMEIDX", toTI88(powerDF("P88"))).withColumnRenamed("P88", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p89DF = powerDF.withColumn("TIMEIDX", toTI89(powerDF("P89"))).withColumnRenamed("P89", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p90DF = powerDF.withColumn("TIMEIDX", toTI90(powerDF("P90"))).withColumnRenamed("P90", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p91DF = powerDF.withColumn("TIMEIDX", toTI91(powerDF("P91"))).withColumnRenamed("P91", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p92DF = powerDF.withColumn("TIMEIDX", toTI92(powerDF("P92"))).withColumnRenamed("P92", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p93DF = powerDF.withColumn("TIMEIDX", toTI93(powerDF("P93"))).withColumnRenamed("P93", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p94DF = powerDF.withColumn("TIMEIDX", toTI94(powerDF("P94"))).withColumnRenamed("P94", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p95DF = powerDF.withColumn("TIMEIDX", toTI95(powerDF("P95"))).withColumnRenamed("P95", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")
    val p96DF = powerDF.withColumn("TIMEIDX", toTI96(powerDF("P96"))).withColumnRenamed("P96", "POWER").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "POWER").withColumn("TS", conv2TS(powerDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(powerDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "POWER")

    p1DF.unionAll(p2DF).unionAll(p3DF).unionAll(p4DF).unionAll(p5DF).unionAll(p6DF).unionAll(p7DF).unionAll(p8DF).unionAll(p9DF).unionAll(p10DF)
        .unionAll(p11DF).unionAll(p12DF).unionAll(p13DF).unionAll(p14DF).unionAll(p15DF).unionAll(p16DF).unionAll(p17DF).unionAll(p18DF).unionAll(p19DF).unionAll(p20DF)
        .unionAll(p21DF).unionAll(p22DF).unionAll(p23DF).unionAll(p24DF).unionAll(p25DF).unionAll(p26DF).unionAll(p27DF).unionAll(p28DF).unionAll(p29DF).unionAll(p30DF)
        .unionAll(p31DF).unionAll(p32DF).unionAll(p33DF).unionAll(p34DF).unionAll(p35DF).unionAll(p36DF).unionAll(p37DF).unionAll(p38DF).unionAll(p39DF).unionAll(p40DF)
        .unionAll(p41DF).unionAll(p42DF).unionAll(p43DF).unionAll(p44DF).unionAll(p45DF).unionAll(p46DF).unionAll(p47DF).unionAll(p48DF).unionAll(p49DF).unionAll(p50DF)
        .unionAll(p51DF).unionAll(p52DF).unionAll(p53DF).unionAll(p54DF).unionAll(p55DF).unionAll(p56DF).unionAll(p57DF).unionAll(p58DF).unionAll(p59DF).unionAll(p60DF)
        .unionAll(p61DF).unionAll(p62DF).unionAll(p63DF).unionAll(p64DF).unionAll(p65DF).unionAll(p66DF).unionAll(p67DF).unionAll(p68DF).unionAll(p69DF).unionAll(p70DF)
        .unionAll(p71DF).unionAll(p72DF).unionAll(p73DF).unionAll(p74DF).unionAll(p75DF).unionAll(p76DF).unionAll(p77DF).unionAll(p78DF).unionAll(p79DF).unionAll(p80DF)
        .unionAll(p81DF).unionAll(p82DF).unionAll(p83DF).unionAll(p84DF).unionAll(p85DF).unionAll(p86DF).unionAll(p87DF).unionAll(p88DF).unionAll(p89DF).unionAll(p90DF)
        .unionAll(p91DF).unionAll(p92DF).unionAll(p93DF).unionAll(p94DF).unionAll(p95DF).unionAll(p96DF)
  }

  /**
   *  Convert 96 Current columns to rows. 
   *  First, select individual I[1-96] column as DataFrame from Power DataFrame, no filtering - all phases of data ;
   *  In the meantime, add a column of "TIMEIDX" (1-96) to indicate time of day;
   *  Then, unionAll 96 DataFrames of P[1-96].
   */
  def convCol2RowCur(sqlContext: SQLContext, curDF: DataFrame): DataFrame = {

    import sqlContext.implicits._

    // Select individual I[1-96], then UNION to convert columns to rows
    val c1DF  = curDF.withColumn("TIMEIDX", toTI1(curDF("I1"))).withColumnRenamed("I1", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c2DF  = curDF.withColumn("TIMEIDX", toTI2(curDF("I2"))).withColumnRenamed("I2", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c3DF  = curDF.withColumn("TIMEIDX", toTI3(curDF("I3"))).withColumnRenamed("I3", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c4DF  = curDF.withColumn("TIMEIDX", toTI4(curDF("I4"))).withColumnRenamed("I4", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c5DF  = curDF.withColumn("TIMEIDX", toTI5(curDF("I5"))).withColumnRenamed("I5", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c6DF  = curDF.withColumn("TIMEIDX", toTI6(curDF("I6"))).withColumnRenamed("I6", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c7DF  = curDF.withColumn("TIMEIDX", toTI7(curDF("I7"))).withColumnRenamed("I7", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c8DF  = curDF.withColumn("TIMEIDX", toTI8(curDF("I8"))).withColumnRenamed("I8", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c9DF  = curDF.withColumn("TIMEIDX", toTI9(curDF("I9"))).withColumnRenamed("I9", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c10DF = curDF.withColumn("TIMEIDX", toTI10(curDF("I10"))).withColumnRenamed("I10", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c11DF = curDF.withColumn("TIMEIDX", toTI11(curDF("I11"))).withColumnRenamed("I11", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c12DF = curDF.withColumn("TIMEIDX", toTI12(curDF("I12"))).withColumnRenamed("I12", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c13DF = curDF.withColumn("TIMEIDX", toTI13(curDF("I13"))).withColumnRenamed("I13", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c14DF = curDF.withColumn("TIMEIDX", toTI14(curDF("I14"))).withColumnRenamed("I14", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c15DF = curDF.withColumn("TIMEIDX", toTI15(curDF("I15"))).withColumnRenamed("I15", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c16DF = curDF.withColumn("TIMEIDX", toTI16(curDF("I16"))).withColumnRenamed("I16", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c17DF = curDF.withColumn("TIMEIDX", toTI17(curDF("I17"))).withColumnRenamed("I17", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c18DF = curDF.withColumn("TIMEIDX", toTI18(curDF("I18"))).withColumnRenamed("I18", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c19DF = curDF.withColumn("TIMEIDX", toTI19(curDF("I19"))).withColumnRenamed("I19", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c20DF = curDF.withColumn("TIMEIDX", toTI20(curDF("I20"))).withColumnRenamed("I20", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c21DF = curDF.withColumn("TIMEIDX", toTI21(curDF("I21"))).withColumnRenamed("I21", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c22DF = curDF.withColumn("TIMEIDX", toTI22(curDF("I22"))).withColumnRenamed("I22", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c23DF = curDF.withColumn("TIMEIDX", toTI23(curDF("I23"))).withColumnRenamed("I23", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c24DF = curDF.withColumn("TIMEIDX", toTI24(curDF("I24"))).withColumnRenamed("I24", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c25DF = curDF.withColumn("TIMEIDX", toTI25(curDF("I25"))).withColumnRenamed("I25", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c26DF = curDF.withColumn("TIMEIDX", toTI26(curDF("I26"))).withColumnRenamed("I26", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c27DF = curDF.withColumn("TIMEIDX", toTI27(curDF("I27"))).withColumnRenamed("I27", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c28DF = curDF.withColumn("TIMEIDX", toTI28(curDF("I28"))).withColumnRenamed("I28", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c29DF = curDF.withColumn("TIMEIDX", toTI29(curDF("I29"))).withColumnRenamed("I29", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c30DF = curDF.withColumn("TIMEIDX", toTI30(curDF("I30"))).withColumnRenamed("I30", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c31DF = curDF.withColumn("TIMEIDX", toTI31(curDF("I31"))).withColumnRenamed("I31", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c32DF = curDF.withColumn("TIMEIDX", toTI32(curDF("I32"))).withColumnRenamed("I32", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c33DF = curDF.withColumn("TIMEIDX", toTI33(curDF("I33"))).withColumnRenamed("I33", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c34DF = curDF.withColumn("TIMEIDX", toTI34(curDF("I34"))).withColumnRenamed("I34", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c35DF = curDF.withColumn("TIMEIDX", toTI35(curDF("I35"))).withColumnRenamed("I35", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c36DF = curDF.withColumn("TIMEIDX", toTI36(curDF("I36"))).withColumnRenamed("I36", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c37DF = curDF.withColumn("TIMEIDX", toTI37(curDF("I37"))).withColumnRenamed("I37", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c38DF = curDF.withColumn("TIMEIDX", toTI38(curDF("I38"))).withColumnRenamed("I38", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c39DF = curDF.withColumn("TIMEIDX", toTI39(curDF("I39"))).withColumnRenamed("I39", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c40DF = curDF.withColumn("TIMEIDX", toTI40(curDF("I40"))).withColumnRenamed("I40", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c41DF = curDF.withColumn("TIMEIDX", toTI41(curDF("I41"))).withColumnRenamed("I41", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c42DF = curDF.withColumn("TIMEIDX", toTI42(curDF("I42"))).withColumnRenamed("I42", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c43DF = curDF.withColumn("TIMEIDX", toTI43(curDF("I43"))).withColumnRenamed("I43", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c44DF = curDF.withColumn("TIMEIDX", toTI44(curDF("I44"))).withColumnRenamed("I44", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c45DF = curDF.withColumn("TIMEIDX", toTI45(curDF("I45"))).withColumnRenamed("I45", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c46DF = curDF.withColumn("TIMEIDX", toTI46(curDF("I46"))).withColumnRenamed("I46", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c47DF = curDF.withColumn("TIMEIDX", toTI47(curDF("I47"))).withColumnRenamed("I47", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c48DF = curDF.withColumn("TIMEIDX", toTI48(curDF("I48"))).withColumnRenamed("I48", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c49DF = curDF.withColumn("TIMEIDX", toTI49(curDF("I49"))).withColumnRenamed("I49", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c50DF = curDF.withColumn("TIMEIDX", toTI50(curDF("I50"))).withColumnRenamed("I50", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c51DF = curDF.withColumn("TIMEIDX", toTI51(curDF("I51"))).withColumnRenamed("I51", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c52DF = curDF.withColumn("TIMEIDX", toTI52(curDF("I52"))).withColumnRenamed("I52", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c53DF = curDF.withColumn("TIMEIDX", toTI53(curDF("I53"))).withColumnRenamed("I53", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c54DF = curDF.withColumn("TIMEIDX", toTI54(curDF("I54"))).withColumnRenamed("I54", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c55DF = curDF.withColumn("TIMEIDX", toTI55(curDF("I55"))).withColumnRenamed("I55", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c56DF = curDF.withColumn("TIMEIDX", toTI56(curDF("I56"))).withColumnRenamed("I56", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c57DF = curDF.withColumn("TIMEIDX", toTI57(curDF("I57"))).withColumnRenamed("I57", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c58DF = curDF.withColumn("TIMEIDX", toTI58(curDF("I58"))).withColumnRenamed("I58", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c59DF = curDF.withColumn("TIMEIDX", toTI59(curDF("I59"))).withColumnRenamed("I59", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c60DF = curDF.withColumn("TIMEIDX", toTI60(curDF("I60"))).withColumnRenamed("I60", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c61DF = curDF.withColumn("TIMEIDX", toTI61(curDF("I61"))).withColumnRenamed("I61", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c62DF = curDF.withColumn("TIMEIDX", toTI62(curDF("I62"))).withColumnRenamed("I62", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c63DF = curDF.withColumn("TIMEIDX", toTI63(curDF("I63"))).withColumnRenamed("I63", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c64DF = curDF.withColumn("TIMEIDX", toTI64(curDF("I64"))).withColumnRenamed("I64", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c65DF = curDF.withColumn("TIMEIDX", toTI65(curDF("I65"))).withColumnRenamed("I65", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c66DF = curDF.withColumn("TIMEIDX", toTI66(curDF("I66"))).withColumnRenamed("I66", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c67DF = curDF.withColumn("TIMEIDX", toTI67(curDF("I67"))).withColumnRenamed("I67", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c68DF = curDF.withColumn("TIMEIDX", toTI68(curDF("I68"))).withColumnRenamed("I68", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c69DF = curDF.withColumn("TIMEIDX", toTI69(curDF("I69"))).withColumnRenamed("I69", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c70DF = curDF.withColumn("TIMEIDX", toTI70(curDF("I70"))).withColumnRenamed("I70", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c71DF = curDF.withColumn("TIMEIDX", toTI71(curDF("I71"))).withColumnRenamed("I71", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c72DF = curDF.withColumn("TIMEIDX", toTI72(curDF("I72"))).withColumnRenamed("I72", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c73DF = curDF.withColumn("TIMEIDX", toTI73(curDF("I73"))).withColumnRenamed("I73", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c74DF = curDF.withColumn("TIMEIDX", toTI74(curDF("I74"))).withColumnRenamed("I74", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c75DF = curDF.withColumn("TIMEIDX", toTI75(curDF("I75"))).withColumnRenamed("I75", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c76DF = curDF.withColumn("TIMEIDX", toTI76(curDF("I76"))).withColumnRenamed("I76", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c77DF = curDF.withColumn("TIMEIDX", toTI77(curDF("I77"))).withColumnRenamed("I77", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c78DF = curDF.withColumn("TIMEIDX", toTI78(curDF("I78"))).withColumnRenamed("I78", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c79DF = curDF.withColumn("TIMEIDX", toTI79(curDF("I79"))).withColumnRenamed("I79", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c80DF = curDF.withColumn("TIMEIDX", toTI80(curDF("I80"))).withColumnRenamed("I80", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c81DF = curDF.withColumn("TIMEIDX", toTI81(curDF("I81"))).withColumnRenamed("I81", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c82DF = curDF.withColumn("TIMEIDX", toTI82(curDF("I82"))).withColumnRenamed("I82", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c83DF = curDF.withColumn("TIMEIDX", toTI83(curDF("I83"))).withColumnRenamed("I83", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c84DF = curDF.withColumn("TIMEIDX", toTI84(curDF("I84"))).withColumnRenamed("I84", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c85DF = curDF.withColumn("TIMEIDX", toTI85(curDF("I85"))).withColumnRenamed("I85", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c86DF = curDF.withColumn("TIMEIDX", toTI86(curDF("I86"))).withColumnRenamed("I86", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c87DF = curDF.withColumn("TIMEIDX", toTI87(curDF("I87"))).withColumnRenamed("I87", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c88DF = curDF.withColumn("TIMEIDX", toTI88(curDF("I88"))).withColumnRenamed("I88", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c89DF = curDF.withColumn("TIMEIDX", toTI89(curDF("I89"))).withColumnRenamed("I89", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c90DF = curDF.withColumn("TIMEIDX", toTI90(curDF("I90"))).withColumnRenamed("I90", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c91DF = curDF.withColumn("TIMEIDX", toTI91(curDF("I91"))).withColumnRenamed("I91", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c92DF = curDF.withColumn("TIMEIDX", toTI92(curDF("I92"))).withColumnRenamed("I92", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c93DF = curDF.withColumn("TIMEIDX", toTI93(curDF("I93"))).withColumnRenamed("I93", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c94DF = curDF.withColumn("TIMEIDX", toTI94(curDF("I94"))).withColumnRenamed("I94", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c95DF = curDF.withColumn("TIMEIDX", toTI95(curDF("I95"))).withColumnRenamed("I95", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")
    val c96DF = curDF.withColumn("TIMEIDX", toTI96(curDF("I96"))).withColumnRenamed("I96", "CUR").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "CUR").withColumn("TS", conv2TS(curDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(curDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "CUR")

    c1DF.unionAll(c2DF).unionAll(c3DF).unionAll(c4DF).unionAll(c5DF).unionAll(c6DF).unionAll(c7DF).unionAll(c8DF).unionAll(c9DF).unionAll(c10DF)
        .unionAll(c11DF).unionAll(c12DF).unionAll(c13DF).unionAll(c14DF).unionAll(c15DF).unionAll(c16DF).unionAll(c17DF).unionAll(c18DF).unionAll(c19DF).unionAll(c20DF)
        .unionAll(c21DF).unionAll(c22DF).unionAll(c23DF).unionAll(c24DF).unionAll(c25DF).unionAll(c26DF).unionAll(c27DF).unionAll(c28DF).unionAll(c29DF).unionAll(c30DF)
        .unionAll(c31DF).unionAll(c32DF).unionAll(c33DF).unionAll(c34DF).unionAll(c35DF).unionAll(c36DF).unionAll(c37DF).unionAll(c38DF).unionAll(c39DF).unionAll(c40DF)
        .unionAll(c41DF).unionAll(c42DF).unionAll(c43DF).unionAll(c44DF).unionAll(c45DF).unionAll(c46DF).unionAll(c47DF).unionAll(c48DF).unionAll(c49DF).unionAll(c50DF)
        .unionAll(c51DF).unionAll(c52DF).unionAll(c53DF).unionAll(c54DF).unionAll(c55DF).unionAll(c56DF).unionAll(c57DF).unionAll(c58DF).unionAll(c59DF).unionAll(c60DF)
        .unionAll(c61DF).unionAll(c62DF).unionAll(c63DF).unionAll(c64DF).unionAll(c65DF).unionAll(c66DF).unionAll(c67DF).unionAll(c68DF).unionAll(c69DF).unionAll(c70DF)
        .unionAll(c71DF).unionAll(c72DF).unionAll(c73DF).unionAll(c74DF).unionAll(c75DF).unionAll(c76DF).unionAll(c77DF).unionAll(c78DF).unionAll(c79DF).unionAll(c80DF)
        .unionAll(c81DF).unionAll(c82DF).unionAll(c83DF).unionAll(c84DF).unionAll(c85DF).unionAll(c86DF).unionAll(c87DF).unionAll(c88DF).unionAll(c89DF).unionAll(c90DF)
        .unionAll(c91DF).unionAll(c92DF).unionAll(c93DF).unionAll(c94DF).unionAll(c95DF).unionAll(c96DF)
  }


  /**
   *  Convert 96 Power Factor columns to rows. 
   *  First, select individual C[1-96] column as DataFrame from Power DataFrame, no filtering - all phases of data ;
   *  In the meantime, add a column of "TIMEIDX" (1-96) to indicate time of day;
   *  Then, unionAll 96 DataFrames of C[1-96].
   */
  def convCol2RowPF(sqlContext: SQLContext, pfDF: DataFrame): DataFrame = {

    import sqlContext.implicits._

    // Select individual C[1-96], then UNION to convert columns to rows
    val f1DF  = pfDF.withColumn("TIMEIDX", toTI1(pfDF("C1"))).withColumnRenamed("C1", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f2DF  = pfDF.withColumn("TIMEIDX", toTI2(pfDF("C2"))).withColumnRenamed("C2", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f3DF  = pfDF.withColumn("TIMEIDX", toTI3(pfDF("C3"))).withColumnRenamed("C3", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f4DF  = pfDF.withColumn("TIMEIDX", toTI4(pfDF("C4"))).withColumnRenamed("C4", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f5DF  = pfDF.withColumn("TIMEIDX", toTI5(pfDF("C5"))).withColumnRenamed("C5", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f6DF  = pfDF.withColumn("TIMEIDX", toTI6(pfDF("C6"))).withColumnRenamed("C6", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f7DF  = pfDF.withColumn("TIMEIDX", toTI7(pfDF("C7"))).withColumnRenamed("C7", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f8DF  = pfDF.withColumn("TIMEIDX", toTI8(pfDF("C8"))).withColumnRenamed("C8", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f9DF  = pfDF.withColumn("TIMEIDX", toTI9(pfDF("C9"))).withColumnRenamed("C9", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f10DF = pfDF.withColumn("TIMEIDX", toTI10(pfDF("C10"))).withColumnRenamed("C10", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f11DF = pfDF.withColumn("TIMEIDX", toTI11(pfDF("C11"))).withColumnRenamed("C11", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f12DF = pfDF.withColumn("TIMEIDX", toTI12(pfDF("C12"))).withColumnRenamed("C12", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f13DF = pfDF.withColumn("TIMEIDX", toTI13(pfDF("C13"))).withColumnRenamed("C13", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f14DF = pfDF.withColumn("TIMEIDX", toTI14(pfDF("C14"))).withColumnRenamed("C14", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f15DF = pfDF.withColumn("TIMEIDX", toTI15(pfDF("C15"))).withColumnRenamed("C15", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f16DF = pfDF.withColumn("TIMEIDX", toTI16(pfDF("C16"))).withColumnRenamed("C16", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f17DF = pfDF.withColumn("TIMEIDX", toTI17(pfDF("C17"))).withColumnRenamed("C17", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f18DF = pfDF.withColumn("TIMEIDX", toTI18(pfDF("C18"))).withColumnRenamed("C18", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f19DF = pfDF.withColumn("TIMEIDX", toTI19(pfDF("C19"))).withColumnRenamed("C19", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f20DF = pfDF.withColumn("TIMEIDX", toTI20(pfDF("C20"))).withColumnRenamed("C20", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f21DF = pfDF.withColumn("TIMEIDX", toTI21(pfDF("C21"))).withColumnRenamed("C21", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f22DF = pfDF.withColumn("TIMEIDX", toTI22(pfDF("C22"))).withColumnRenamed("C22", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f23DF = pfDF.withColumn("TIMEIDX", toTI23(pfDF("C23"))).withColumnRenamed("C23", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f24DF = pfDF.withColumn("TIMEIDX", toTI24(pfDF("C24"))).withColumnRenamed("C24", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f25DF = pfDF.withColumn("TIMEIDX", toTI25(pfDF("C25"))).withColumnRenamed("C25", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f26DF = pfDF.withColumn("TIMEIDX", toTI26(pfDF("C26"))).withColumnRenamed("C26", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f27DF = pfDF.withColumn("TIMEIDX", toTI27(pfDF("C27"))).withColumnRenamed("C27", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f28DF = pfDF.withColumn("TIMEIDX", toTI28(pfDF("C28"))).withColumnRenamed("C28", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f29DF = pfDF.withColumn("TIMEIDX", toTI29(pfDF("C29"))).withColumnRenamed("C29", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f30DF = pfDF.withColumn("TIMEIDX", toTI30(pfDF("C30"))).withColumnRenamed("C30", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f31DF = pfDF.withColumn("TIMEIDX", toTI31(pfDF("C31"))).withColumnRenamed("C31", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f32DF = pfDF.withColumn("TIMEIDX", toTI32(pfDF("C32"))).withColumnRenamed("C32", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f33DF = pfDF.withColumn("TIMEIDX", toTI33(pfDF("C33"))).withColumnRenamed("C33", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f34DF = pfDF.withColumn("TIMEIDX", toTI34(pfDF("C34"))).withColumnRenamed("C34", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f35DF = pfDF.withColumn("TIMEIDX", toTI35(pfDF("C35"))).withColumnRenamed("C35", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f36DF = pfDF.withColumn("TIMEIDX", toTI36(pfDF("C36"))).withColumnRenamed("C36", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f37DF = pfDF.withColumn("TIMEIDX", toTI37(pfDF("C37"))).withColumnRenamed("C37", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f38DF = pfDF.withColumn("TIMEIDX", toTI38(pfDF("C38"))).withColumnRenamed("C38", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f39DF = pfDF.withColumn("TIMEIDX", toTI39(pfDF("C39"))).withColumnRenamed("C39", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f40DF = pfDF.withColumn("TIMEIDX", toTI40(pfDF("C40"))).withColumnRenamed("C40", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f41DF = pfDF.withColumn("TIMEIDX", toTI41(pfDF("C41"))).withColumnRenamed("C41", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f42DF = pfDF.withColumn("TIMEIDX", toTI42(pfDF("C42"))).withColumnRenamed("C42", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f43DF = pfDF.withColumn("TIMEIDX", toTI43(pfDF("C43"))).withColumnRenamed("C43", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f44DF = pfDF.withColumn("TIMEIDX", toTI44(pfDF("C44"))).withColumnRenamed("C44", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f45DF = pfDF.withColumn("TIMEIDX", toTI45(pfDF("C45"))).withColumnRenamed("C45", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f46DF = pfDF.withColumn("TIMEIDX", toTI46(pfDF("C46"))).withColumnRenamed("C46", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f47DF = pfDF.withColumn("TIMEIDX", toTI47(pfDF("C47"))).withColumnRenamed("C47", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f48DF = pfDF.withColumn("TIMEIDX", toTI48(pfDF("C48"))).withColumnRenamed("C48", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f49DF = pfDF.withColumn("TIMEIDX", toTI49(pfDF("C49"))).withColumnRenamed("C49", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f50DF = pfDF.withColumn("TIMEIDX", toTI50(pfDF("C50"))).withColumnRenamed("C50", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f51DF = pfDF.withColumn("TIMEIDX", toTI51(pfDF("C51"))).withColumnRenamed("C51", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f52DF = pfDF.withColumn("TIMEIDX", toTI52(pfDF("C52"))).withColumnRenamed("C52", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f53DF = pfDF.withColumn("TIMEIDX", toTI53(pfDF("C53"))).withColumnRenamed("C53", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f54DF = pfDF.withColumn("TIMEIDX", toTI54(pfDF("C54"))).withColumnRenamed("C54", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f55DF = pfDF.withColumn("TIMEIDX", toTI55(pfDF("C55"))).withColumnRenamed("C55", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f56DF = pfDF.withColumn("TIMEIDX", toTI56(pfDF("C56"))).withColumnRenamed("C56", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f57DF = pfDF.withColumn("TIMEIDX", toTI57(pfDF("C57"))).withColumnRenamed("C57", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f58DF = pfDF.withColumn("TIMEIDX", toTI58(pfDF("C58"))).withColumnRenamed("C58", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f59DF = pfDF.withColumn("TIMEIDX", toTI59(pfDF("C59"))).withColumnRenamed("C59", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f60DF = pfDF.withColumn("TIMEIDX", toTI60(pfDF("C60"))).withColumnRenamed("C60", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f61DF = pfDF.withColumn("TIMEIDX", toTI61(pfDF("C61"))).withColumnRenamed("C61", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f62DF = pfDF.withColumn("TIMEIDX", toTI62(pfDF("C62"))).withColumnRenamed("C62", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f63DF = pfDF.withColumn("TIMEIDX", toTI63(pfDF("C63"))).withColumnRenamed("C63", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f64DF = pfDF.withColumn("TIMEIDX", toTI64(pfDF("C64"))).withColumnRenamed("C64", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f65DF = pfDF.withColumn("TIMEIDX", toTI65(pfDF("C65"))).withColumnRenamed("C65", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f66DF = pfDF.withColumn("TIMEIDX", toTI66(pfDF("C66"))).withColumnRenamed("C66", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f67DF = pfDF.withColumn("TIMEIDX", toTI67(pfDF("C67"))).withColumnRenamed("C67", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f68DF = pfDF.withColumn("TIMEIDX", toTI68(pfDF("C68"))).withColumnRenamed("C68", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f69DF = pfDF.withColumn("TIMEIDX", toTI69(pfDF("C69"))).withColumnRenamed("C69", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f70DF = pfDF.withColumn("TIMEIDX", toTI70(pfDF("C70"))).withColumnRenamed("C70", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f71DF = pfDF.withColumn("TIMEIDX", toTI71(pfDF("C71"))).withColumnRenamed("C71", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f72DF = pfDF.withColumn("TIMEIDX", toTI72(pfDF("C72"))).withColumnRenamed("C72", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f73DF = pfDF.withColumn("TIMEIDX", toTI73(pfDF("C73"))).withColumnRenamed("C73", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f74DF = pfDF.withColumn("TIMEIDX", toTI74(pfDF("C74"))).withColumnRenamed("C74", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f75DF = pfDF.withColumn("TIMEIDX", toTI75(pfDF("C75"))).withColumnRenamed("C75", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f76DF = pfDF.withColumn("TIMEIDX", toTI76(pfDF("C76"))).withColumnRenamed("C76", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f77DF = pfDF.withColumn("TIMEIDX", toTI77(pfDF("C77"))).withColumnRenamed("C77", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f78DF = pfDF.withColumn("TIMEIDX", toTI78(pfDF("C78"))).withColumnRenamed("C78", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f79DF = pfDF.withColumn("TIMEIDX", toTI79(pfDF("C79"))).withColumnRenamed("C79", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f80DF = pfDF.withColumn("TIMEIDX", toTI80(pfDF("C80"))).withColumnRenamed("C80", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f81DF = pfDF.withColumn("TIMEIDX", toTI81(pfDF("C81"))).withColumnRenamed("C81", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f82DF = pfDF.withColumn("TIMEIDX", toTI82(pfDF("C82"))).withColumnRenamed("C82", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f83DF = pfDF.withColumn("TIMEIDX", toTI83(pfDF("C83"))).withColumnRenamed("C83", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f84DF = pfDF.withColumn("TIMEIDX", toTI84(pfDF("C84"))).withColumnRenamed("C84", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f85DF = pfDF.withColumn("TIMEIDX", toTI85(pfDF("C85"))).withColumnRenamed("C85", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f86DF = pfDF.withColumn("TIMEIDX", toTI86(pfDF("C86"))).withColumnRenamed("C86", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f87DF = pfDF.withColumn("TIMEIDX", toTI87(pfDF("C87"))).withColumnRenamed("C87", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f88DF = pfDF.withColumn("TIMEIDX", toTI88(pfDF("C88"))).withColumnRenamed("C88", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f89DF = pfDF.withColumn("TIMEIDX", toTI89(pfDF("C89"))).withColumnRenamed("C89", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f90DF = pfDF.withColumn("TIMEIDX", toTI90(pfDF("C90"))).withColumnRenamed("C90", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f91DF = pfDF.withColumn("TIMEIDX", toTI91(pfDF("C91"))).withColumnRenamed("C91", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f92DF = pfDF.withColumn("TIMEIDX", toTI92(pfDF("C92"))).withColumnRenamed("C92", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f93DF = pfDF.withColumn("TIMEIDX", toTI93(pfDF("C93"))).withColumnRenamed("C93", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f94DF = pfDF.withColumn("TIMEIDX", toTI94(pfDF("C94"))).withColumnRenamed("C94", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f95DF = pfDF.withColumn("TIMEIDX", toTI95(pfDF("C95"))).withColumnRenamed("C95", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")
    val f96DF = pfDF.withColumn("TIMEIDX", toTI96(pfDF("C96"))).withColumnRenamed("C96", "PF").select("ID", "DATA_DATE", "TIMEIDX", "PHASE_FLAG", "PF").withColumn("TS", conv2TS(pfDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(pfDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "PHASE_FLAG", "PF")

    f1DF.unionAll(f2DF).unionAll(f3DF).unionAll(f4DF).unionAll(f5DF).unionAll(f6DF).unionAll(f7DF).unionAll(f8DF).unionAll(f9DF).unionAll(f10DF)
        .unionAll(f11DF).unionAll(f12DF).unionAll(f13DF).unionAll(f14DF).unionAll(f15DF).unionAll(f16DF).unionAll(f17DF).unionAll(f18DF).unionAll(f19DF).unionAll(f20DF)
        .unionAll(f21DF).unionAll(f22DF).unionAll(f23DF).unionAll(f24DF).unionAll(f25DF).unionAll(f26DF).unionAll(f27DF).unionAll(f28DF).unionAll(f29DF).unionAll(f30DF)
        .unionAll(f31DF).unionAll(f32DF).unionAll(f33DF).unionAll(f34DF).unionAll(f35DF).unionAll(f36DF).unionAll(f37DF).unionAll(f38DF).unionAll(f39DF).unionAll(f40DF)
        .unionAll(f41DF).unionAll(f42DF).unionAll(f43DF).unionAll(f44DF).unionAll(f45DF).unionAll(f46DF).unionAll(f47DF).unionAll(f48DF).unionAll(f49DF).unionAll(f50DF)
        .unionAll(f51DF).unionAll(f52DF).unionAll(f53DF).unionAll(f54DF).unionAll(f55DF).unionAll(f56DF).unionAll(f57DF).unionAll(f58DF).unionAll(f59DF).unionAll(f60DF)
        .unionAll(f61DF).unionAll(f62DF).unionAll(f63DF).unionAll(f64DF).unionAll(f65DF).unionAll(f66DF).unionAll(f67DF).unionAll(f68DF).unionAll(f69DF).unionAll(f70DF)
        .unionAll(f71DF).unionAll(f72DF).unionAll(f73DF).unionAll(f74DF).unionAll(f75DF).unionAll(f76DF).unionAll(f77DF).unionAll(f78DF).unionAll(f79DF).unionAll(f80DF)
        .unionAll(f81DF).unionAll(f82DF).unionAll(f83DF).unionAll(f84DF).unionAll(f85DF).unionAll(f86DF).unionAll(f87DF).unionAll(f88DF).unionAll(f89DF).unionAll(f90DF)
        .unionAll(f91DF).unionAll(f92DF).unionAll(f93DF).unionAll(f94DF).unionAll(f95DF).unionAll(f96DF)
  }


  /**
   *  Convert 96 Accumulated Energy columns to rows. 
   *  First, select individual R[1-96] column as DataFrame from Power DataFrame, no filtering - all types of data ;
   *  In the meantime, add a column of "TIMEIDX" (1-96) to indicate time of day;
   *  Then, unionAll 96 DataFrames of R[1-96].
   */
  def convCol2RowE(sqlContext: SQLContext, readDF: DataFrame): DataFrame = {

    import sqlContext.implicits._

    // Select individual R[1-96], then UNION to convert columns to rows
    val e1DF  = readDF.withColumn("TIMEIDX", toTI1(readDF("R1"))).withColumnRenamed("R1", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e2DF  = readDF.withColumn("TIMEIDX", toTI2(readDF("R2"))).withColumnRenamed("R2", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e3DF  = readDF.withColumn("TIMEIDX", toTI3(readDF("R3"))).withColumnRenamed("R3", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e4DF  = readDF.withColumn("TIMEIDX", toTI4(readDF("R4"))).withColumnRenamed("R4", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e5DF  = readDF.withColumn("TIMEIDX", toTI5(readDF("R5"))).withColumnRenamed("R5", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e6DF  = readDF.withColumn("TIMEIDX", toTI6(readDF("R6"))).withColumnRenamed("R6", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e7DF  = readDF.withColumn("TIMEIDX", toTI7(readDF("R7"))).withColumnRenamed("R7", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e8DF  = readDF.withColumn("TIMEIDX", toTI8(readDF("R8"))).withColumnRenamed("R8", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e9DF  = readDF.withColumn("TIMEIDX", toTI9(readDF("R9"))).withColumnRenamed("R9", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e10DF = readDF.withColumn("TIMEIDX", toTI10(readDF("R10"))).withColumnRenamed("R10", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e11DF = readDF.withColumn("TIMEIDX", toTI11(readDF("R11"))).withColumnRenamed("R11", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e12DF = readDF.withColumn("TIMEIDX", toTI12(readDF("R12"))).withColumnRenamed("R12", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e13DF = readDF.withColumn("TIMEIDX", toTI13(readDF("R13"))).withColumnRenamed("R13", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e14DF = readDF.withColumn("TIMEIDX", toTI14(readDF("R14"))).withColumnRenamed("R14", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e15DF = readDF.withColumn("TIMEIDX", toTI15(readDF("R15"))).withColumnRenamed("R15", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e16DF = readDF.withColumn("TIMEIDX", toTI16(readDF("R16"))).withColumnRenamed("R16", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e17DF = readDF.withColumn("TIMEIDX", toTI17(readDF("R17"))).withColumnRenamed("R17", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e18DF = readDF.withColumn("TIMEIDX", toTI18(readDF("R18"))).withColumnRenamed("R18", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e19DF = readDF.withColumn("TIMEIDX", toTI19(readDF("R19"))).withColumnRenamed("R19", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e20DF = readDF.withColumn("TIMEIDX", toTI20(readDF("R20"))).withColumnRenamed("R20", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e21DF = readDF.withColumn("TIMEIDX", toTI21(readDF("R21"))).withColumnRenamed("R21", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e22DF = readDF.withColumn("TIMEIDX", toTI22(readDF("R22"))).withColumnRenamed("R22", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e23DF = readDF.withColumn("TIMEIDX", toTI23(readDF("R23"))).withColumnRenamed("R23", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e24DF = readDF.withColumn("TIMEIDX", toTI24(readDF("R24"))).withColumnRenamed("R24", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e25DF = readDF.withColumn("TIMEIDX", toTI25(readDF("R25"))).withColumnRenamed("R25", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e26DF = readDF.withColumn("TIMEIDX", toTI26(readDF("R26"))).withColumnRenamed("R26", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e27DF = readDF.withColumn("TIMEIDX", toTI27(readDF("R27"))).withColumnRenamed("R27", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e28DF = readDF.withColumn("TIMEIDX", toTI28(readDF("R28"))).withColumnRenamed("R28", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e29DF = readDF.withColumn("TIMEIDX", toTI29(readDF("R29"))).withColumnRenamed("R29", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e30DF = readDF.withColumn("TIMEIDX", toTI30(readDF("R30"))).withColumnRenamed("R30", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e31DF = readDF.withColumn("TIMEIDX", toTI31(readDF("R31"))).withColumnRenamed("R31", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e32DF = readDF.withColumn("TIMEIDX", toTI32(readDF("R32"))).withColumnRenamed("R32", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e33DF = readDF.withColumn("TIMEIDX", toTI33(readDF("R33"))).withColumnRenamed("R33", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e34DF = readDF.withColumn("TIMEIDX", toTI34(readDF("R34"))).withColumnRenamed("R34", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e35DF = readDF.withColumn("TIMEIDX", toTI35(readDF("R35"))).withColumnRenamed("R35", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e36DF = readDF.withColumn("TIMEIDX", toTI36(readDF("R36"))).withColumnRenamed("R36", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e37DF = readDF.withColumn("TIMEIDX", toTI37(readDF("R37"))).withColumnRenamed("R37", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e38DF = readDF.withColumn("TIMEIDX", toTI38(readDF("R38"))).withColumnRenamed("R38", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e39DF = readDF.withColumn("TIMEIDX", toTI39(readDF("R39"))).withColumnRenamed("R39", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e40DF = readDF.withColumn("TIMEIDX", toTI40(readDF("R40"))).withColumnRenamed("R40", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e41DF = readDF.withColumn("TIMEIDX", toTI41(readDF("R41"))).withColumnRenamed("R41", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e42DF = readDF.withColumn("TIMEIDX", toTI42(readDF("R42"))).withColumnRenamed("R42", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e43DF = readDF.withColumn("TIMEIDX", toTI43(readDF("R43"))).withColumnRenamed("R43", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e44DF = readDF.withColumn("TIMEIDX", toTI44(readDF("R44"))).withColumnRenamed("R44", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e45DF = readDF.withColumn("TIMEIDX", toTI45(readDF("R45"))).withColumnRenamed("R45", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e46DF = readDF.withColumn("TIMEIDX", toTI46(readDF("R46"))).withColumnRenamed("R46", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e47DF = readDF.withColumn("TIMEIDX", toTI47(readDF("R47"))).withColumnRenamed("R47", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e48DF = readDF.withColumn("TIMEIDX", toTI48(readDF("R48"))).withColumnRenamed("R48", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e49DF = readDF.withColumn("TIMEIDX", toTI49(readDF("R49"))).withColumnRenamed("R49", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e50DF = readDF.withColumn("TIMEIDX", toTI50(readDF("R50"))).withColumnRenamed("R50", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e51DF = readDF.withColumn("TIMEIDX", toTI51(readDF("R51"))).withColumnRenamed("R51", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e52DF = readDF.withColumn("TIMEIDX", toTI52(readDF("R52"))).withColumnRenamed("R52", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e53DF = readDF.withColumn("TIMEIDX", toTI53(readDF("R53"))).withColumnRenamed("R53", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e54DF = readDF.withColumn("TIMEIDX", toTI54(readDF("R54"))).withColumnRenamed("R54", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e55DF = readDF.withColumn("TIMEIDX", toTI55(readDF("R55"))).withColumnRenamed("R55", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e56DF = readDF.withColumn("TIMEIDX", toTI56(readDF("R56"))).withColumnRenamed("R56", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e57DF = readDF.withColumn("TIMEIDX", toTI57(readDF("R57"))).withColumnRenamed("R57", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e58DF = readDF.withColumn("TIMEIDX", toTI58(readDF("R58"))).withColumnRenamed("R58", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e59DF = readDF.withColumn("TIMEIDX", toTI59(readDF("R59"))).withColumnRenamed("R59", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e60DF = readDF.withColumn("TIMEIDX", toTI60(readDF("R60"))).withColumnRenamed("R60", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e61DF = readDF.withColumn("TIMEIDX", toTI61(readDF("R61"))).withColumnRenamed("R61", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e62DF = readDF.withColumn("TIMEIDX", toTI62(readDF("R62"))).withColumnRenamed("R62", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e63DF = readDF.withColumn("TIMEIDX", toTI63(readDF("R63"))).withColumnRenamed("R63", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e64DF = readDF.withColumn("TIMEIDX", toTI64(readDF("R64"))).withColumnRenamed("R64", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e65DF = readDF.withColumn("TIMEIDX", toTI65(readDF("R65"))).withColumnRenamed("R65", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e66DF = readDF.withColumn("TIMEIDX", toTI66(readDF("R66"))).withColumnRenamed("R66", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e67DF = readDF.withColumn("TIMEIDX", toTI67(readDF("R67"))).withColumnRenamed("R67", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e68DF = readDF.withColumn("TIMEIDX", toTI68(readDF("R68"))).withColumnRenamed("R68", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e69DF = readDF.withColumn("TIMEIDX", toTI69(readDF("R69"))).withColumnRenamed("R69", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e70DF = readDF.withColumn("TIMEIDX", toTI70(readDF("R70"))).withColumnRenamed("R70", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e71DF = readDF.withColumn("TIMEIDX", toTI71(readDF("R71"))).withColumnRenamed("R71", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e72DF = readDF.withColumn("TIMEIDX", toTI72(readDF("R72"))).withColumnRenamed("R72", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e73DF = readDF.withColumn("TIMEIDX", toTI73(readDF("R73"))).withColumnRenamed("R73", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e74DF = readDF.withColumn("TIMEIDX", toTI74(readDF("R74"))).withColumnRenamed("R74", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e75DF = readDF.withColumn("TIMEIDX", toTI75(readDF("R75"))).withColumnRenamed("R75", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e76DF = readDF.withColumn("TIMEIDX", toTI76(readDF("R76"))).withColumnRenamed("R76", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e77DF = readDF.withColumn("TIMEIDX", toTI77(readDF("R77"))).withColumnRenamed("R77", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e78DF = readDF.withColumn("TIMEIDX", toTI78(readDF("R78"))).withColumnRenamed("R78", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e79DF = readDF.withColumn("TIMEIDX", toTI79(readDF("R79"))).withColumnRenamed("R79", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e80DF = readDF.withColumn("TIMEIDX", toTI80(readDF("R80"))).withColumnRenamed("R80", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e81DF = readDF.withColumn("TIMEIDX", toTI81(readDF("R81"))).withColumnRenamed("R81", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e82DF = readDF.withColumn("TIMEIDX", toTI82(readDF("R82"))).withColumnRenamed("R82", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e83DF = readDF.withColumn("TIMEIDX", toTI83(readDF("R83"))).withColumnRenamed("R83", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e84DF = readDF.withColumn("TIMEIDX", toTI84(readDF("R84"))).withColumnRenamed("R84", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e85DF = readDF.withColumn("TIMEIDX", toTI85(readDF("R85"))).withColumnRenamed("R85", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e86DF = readDF.withColumn("TIMEIDX", toTI86(readDF("R86"))).withColumnRenamed("R86", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e87DF = readDF.withColumn("TIMEIDX", toTI87(readDF("R87"))).withColumnRenamed("R87", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e88DF = readDF.withColumn("TIMEIDX", toTI88(readDF("R88"))).withColumnRenamed("R88", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e89DF = readDF.withColumn("TIMEIDX", toTI89(readDF("R89"))).withColumnRenamed("R89", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e90DF = readDF.withColumn("TIMEIDX", toTI90(readDF("R90"))).withColumnRenamed("R90", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e91DF = readDF.withColumn("TIMEIDX", toTI91(readDF("R91"))).withColumnRenamed("R91", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e92DF = readDF.withColumn("TIMEIDX", toTI92(readDF("R92"))).withColumnRenamed("R92", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e93DF = readDF.withColumn("TIMEIDX", toTI93(readDF("R93"))).withColumnRenamed("R93", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e94DF = readDF.withColumn("TIMEIDX", toTI94(readDF("R94"))).withColumnRenamed("R94", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e95DF = readDF.withColumn("TIMEIDX", toTI95(readDF("R95"))).withColumnRenamed("R95", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")
    val e96DF = readDF.withColumn("TIMEIDX", toTI96(readDF("R96"))).withColumnRenamed("R96", "ENERGY").select("ID", "DATA_DATE", "TIMEIDX", "DATA_TYPE", "ENERGY").withColumn("TS", conv2TS(readDF("DATA_DATE"), $"TIMEIDX")).withColumn("DTI", getDTI(readDF("DATA_DATE"), $"TIMEIDX")).select("ID", "TS", "DTI", "DATA_TYPE", "ENERGY")

    e1DF.unionAll(e2DF).unionAll(e3DF).unionAll(e4DF).unionAll(e5DF).unionAll(e6DF).unionAll(e7DF).unionAll(e8DF).unionAll(e9DF).unionAll(e10DF)
        .unionAll(e11DF).unionAll(e12DF).unionAll(e13DF).unionAll(e14DF).unionAll(e15DF).unionAll(e16DF).unionAll(e17DF).unionAll(e18DF).unionAll(e19DF).unionAll(e20DF)
        .unionAll(e21DF).unionAll(e22DF).unionAll(e23DF).unionAll(e24DF).unionAll(e25DF).unionAll(e26DF).unionAll(e27DF).unionAll(e28DF).unionAll(e29DF).unionAll(e30DF)
        .unionAll(e31DF).unionAll(e32DF).unionAll(e33DF).unionAll(e34DF).unionAll(e35DF).unionAll(e36DF).unionAll(e37DF).unionAll(e38DF).unionAll(e39DF).unionAll(e40DF)
        .unionAll(e41DF).unionAll(e42DF).unionAll(e43DF).unionAll(e44DF).unionAll(e45DF).unionAll(e46DF).unionAll(e47DF).unionAll(e48DF).unionAll(e49DF).unionAll(e50DF)
        .unionAll(e51DF).unionAll(e52DF).unionAll(e53DF).unionAll(e54DF).unionAll(e55DF).unionAll(e56DF).unionAll(e57DF).unionAll(e58DF).unionAll(e59DF).unionAll(e60DF)
        .unionAll(e61DF).unionAll(e62DF).unionAll(e63DF).unionAll(e64DF).unionAll(e65DF).unionAll(e66DF).unionAll(e67DF).unionAll(e68DF).unionAll(e69DF).unionAll(e70DF)
        .unionAll(e71DF).unionAll(e72DF).unionAll(e73DF).unionAll(e74DF).unionAll(e75DF).unionAll(e76DF).unionAll(e77DF).unionAll(e78DF).unionAll(e79DF).unionAll(e80DF)
        .unionAll(e81DF).unionAll(e82DF).unionAll(e83DF).unionAll(e84DF).unionAll(e85DF).unionAll(e86DF).unionAll(e87DF).unionAll(e88DF).unionAll(e89DF).unionAll(e90DF)
        .unionAll(e91DF).unionAll(e92DF).unionAll(e93DF).unionAll(e94DF).unionAll(e95DF).unionAll(e96DF)
  }


  /**
   * Populate SGDM meter-related tables: identifiedobject, enddevice, meter, etc.
   *
   */
  def popSgdmTables(sc: SparkContext, sqlContext: SQLContext) = {

    import sqlContext.implicits._

      

  }


}


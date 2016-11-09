//-------------------------------------------------------------------------------------------------  
//---- Convert Postgres data of Gridlab-D into Oracle Real Meter format; and prepare ML -----------
//-------------------------------------------------------------------------------------------------  

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.math._

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

Object ConvSim2SM {

  val mdmHome = scala.util.Properties.envOrElse("MDM_HOME", "MDM/")
  val config = ConfigFactory.parseFile(new File(mdmHome + "src/main/resources/application.conf"))
  val numProcesses = config.getInt("mdms.numProcesses")
  val runmode = config.getInt("mdms.runmode")

  //val srcurl = config.getString("mdms.srcurl")
  val srcurl = "jdbc:postgresql://192.168.5.2:5433/sgdm?user=wendong&password=wendong"
  //val tgturl = "jdbc:postgresql://192.168.5.2:5433/sgdm?user=wendong&password=wendong"
  val tgturl2 = "jdbc:postgresql://192.168.5.2:5433/postgres?user=wendong&password=wendong"
  val tgturl = tgturl2

  val pgdti = config.getString("mdms.pgdti")

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

  val pgHourGroup = "data_quality.hourgroup"
  val pgPVHG = "data_quality.pvhg"
  val pgmrdsum = "data_quality.mrdsum"
  val tblMIDs = "data_quality.meterids"

def main(args: Array[String]) {

  val sparkConf = new SparkConf().setAppName("ConvSim2SM")
  //sparkConf.registerKryoClasses(Array(classOf[StorageLevel], classOf[CompactBuffer[_]], classOf[Array[Byte]], classOf[Array[Long]], classOf[SparkConf], classOf[scala.Tuple5[_, _, _, _, _]]))
 
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  
  import sqlContext.implicits._
    
 // UDF applied to DataFrame columns
  val toIntg = udf((d: java.math.BigDecimal) => d.toString.toInt)
  val toIntg2 = udf((d: java.math.BigDecimal) => d.toString.replaceAll("""\.0+$""", "").toInt)
  val toLong = udf((d: java.math.BigDecimal) => d.toString.toLong)
  val toLong2 = udf((d: java.math.BigDecimal) => d.toString.replaceAll("""\.0+$""", "").toLong)
  val toDouble = udf((d: java.math.BigDecimal) => d.toString.toDouble)
  val toStr = udf((d: java.sql.Timestamp) => d.toString.replaceAll("""\.0$""", ""))
  val toTimestamp = udf((d: String) => Timestamp.valueOf(d))
  val toDecimal = udf((d: Double) => scala.math.BigDecimal(d.toString))
  
  val toDate = udf( (d: java.sql.Timestamp) => d.toString.replaceAll("""\ .*$""", "") )
  
  val toVolLow = udf((d: String) => "Voltage-Low")
  val toVolHigh = udf((d: String) => "Voltage-High")
  val toVolMissing = udf((d: String) => "Voltage-Missing")
  val toPowerLow = udf((d: String) => "Power-Low")
  val toPowerHigh = udf((d: String) => "Power-High")
  val toPowerMissing = udf((d: String) => "Power-Missing")

  def conv2SDTI(ts: Timestamp) = {
      val cal = Calendar.getInstance()
      cal.setTime(ts)
      val mon = cal.get(Calendar.MONTH)
      val day = cal.get(Calendar.DAY_OF_WEEK)
      var sdti = 0
      var season = 0
      var daytype = 0

      if (mon == 2 || mon == 3 || mon == 4) {                 // Spring
        if (day == 2 || day == 3 || day == 4 || day == 5 || day == 6) {// Weekday
          sdti = 1; season = 1; daytype = 1
        }
        else if (day == 1 || day == 7) { // Weekend
          sdti = 2; season = 1; daytype = 2;
        }
        else
          sdti = -1 // day not recognized
      }
      else if (mon == 5 || mon == 6 || mon == 7 || mon == 8) { // Summer
        if (day == 2 || day == 3 || day == 4 || day == 5 || day == 6) {// Weekday
          sdti = 4; season = 2; daytype = 1
        }
        else if (day == 1 || day == 7) { // Weekend
          sdti = 5; season = 2; daytype = 2;
        }
        else
          sdti = -2
      }
      else if (mon == 9 || mon == 10)  {                       // Fall
        if  (day == 2 || day == 3 || day == 4 || day == 5 || day == 6) {// Weekday
          sdti = 7; season = 3; daytype = 1
        }
        else if (day == 1 || day == 7) { // Weekend
          sdti = 8; season = 3; daytype = 2;
        }
        else
          sdti = -3
      }
      else if (mon == 11 || mon == 0 || mon == 1)  {                       // Winter
        if  (day == 2 || day == 3 || day == 4 || day == 5 || day == 6) { // Weekday
          sdti = 10; season = 4; daytype = 1
        }
        else if (day == 1 || day == 7) { // Weekend
          sdti = 11; season = 4; daytype = 2;
        }
        else
          sdti = -4
      }
      else {
        sdti = -9 // Month not recognized
      }
      (sdti, season, daytype)
  }

  // Create UDF using function conv2SDTI
  val  toSDTI = udf(conv2SDTI(_: Timestamp)._1)
  val  toSeason = udf(conv2SDTI(_: Timestamp)._2)
  val  toDaytype = udf(conv2SDTI(_: Timestamp)._3)
  
    val totalMetersPerStation = 1635
    val meterStartNumber = 14
    val meterEndNumber = meterStartNumber + totalMetersPerStation - 1
    
    // Load tables from database (here is PostgreSQL) into DataFrame
    //val brDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "basereading"))
    val brDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "basereading", "partitionColumn" -> "timeperiod", "lowerBound" -> "2", "upperBound" -> "35037", "numPartitions" -> "100"))
    val brS1DF = brDF.filter(s"meter >= $meterStartNumber and meter <= $meterEndNumber").cache 
    
    val idoDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "identifiedobject"))
    val meterDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "meter"))
    val meterS1DF = meterDF.filter(s"meterid >= $meterStartNumber and meterid <= $meterEndNumber").cache
    
    val rdtyDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "readingtype"))
    val mskDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "measurementkind"))
    val dtitv = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "datetimeinterval"))
    val phaseDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "phase")).cache 
    
    brS1DF.count
    meterS1DF.count
    phaseDF.count
    
    // identifiedobject join meter, using column alias to avoid name conflict in later join
    val idoM = idoDF.as('ido).join(meterS1DF.as('m), $"ido.identifiedobjectid" === $"m.meterid").select($"ido.identifiedobjectid" as 'idoid)

    // identifiedobject join meter join basereading
    val mrDF = idoM.as('idom).join(brS1DF.as('br), $"idom.idoid" === $"br.meter").select($"br.meter" as 'meterid, $"br.timeperiod", $"br.readingtype", $"br.value").cache 
    mrDF.count
    
    // then join readingtype
    val mrtDF = mrDF.as('mr).join(rdtyDF.as('rdt), $"mr.readingtype" === $"rdt.readingtypeid").select($"mr.meterid", $"mr.timeperiod", $"mr.readingtype", $"mr.value", $"rdt.tou", $"rdt.measurementkind" as 'mesk, $"rdt.phases")

    // then join measurementkind
    val mrtmDF = mrtDF.as('mrt).join(mskDF.as('msk), $"mrt.mesk" === $"msk.measurementkindid").select($"mrt.meterid", $"mrt.timeperiod" as 'timepd, $"mrt.readingtype" as 'rdty, $"mrt.value" as 'value, $"mrt.tou", $"mrt.phases", $"msk.measurementkindid" as 'meskid, $"msk.code" as 'code).orderBy("meterid").cache
    mrtmDF.count  // OK
    mrDF.unpersist
    
    // Then join phase  (no phase info? phase = null)
    //val mrtmphDF = mrtmDF.as('mrtm).join(phaseDF.as('phs), $"mrtm.phases" === $"phs.phasecode").select($"mrtm.meterid", $"mrtm.timepd", $"mrtm.rdty", $"mrtm.value" as 'value, $"mrtm.tou", $"mrtm.phases", //$"mrtm.meskid", $"mrtm.code", $"phs.code" as 'phasename).cache 
    //mrtmphDF.count
    //mrtmDF.unpersist
    
    val mrtmphDF = mrtmDF
    
    // then join DATETIMEINTERVAL
    // Don't cache yet; may have filter later
    val mrtmphdt = mrtmphDF.as('mrtmph).join(dtitv as('dtitv), $"mrtmph.timepd" === $"dtitv.datetimeintervalid").select("meterid", "timepd", "rdty", "value", "tou", "phases", "meskid", "code", "datetimeintervalid", "end").cache 
    mrtmphdt.count
    mrtmDF.unpersist
    
    // Meter reading data summary
    // need to workaround data type problem - convert meterid to double data type
    // also add columns "oldvalue" & "newvalue"
    val mrdsDF = mrtmphdt.withColumn("meterid", toDouble(mrtmphdt("meterid"))).withColumn("mtime", toTimestamp(toStr(mrtmphdt("end")))).select("meterid", "datetimeintervalid", "mtime", "value", "code", "phases").orderBy("meterid", "datetimeintervalid").cache 
    mrdsDF.count
    mrtmphdt.unpersist
    
    val prop = new java.util.Properties
    prop.setProperty("batchsize", "10000000") // 10M batchsize
    mrdsDF.coalesce(2*numProcesses).write.mode("append").jdbc(tgturl, "data_quality.mrdsum2", prop)

    // write to real meter data format (96 columns)
    
    // if program is restarted, then we can read directly from data_quality.mrdsum into DataFrame mrdsDF by comment out the next line, and bypass the above data processing which takes time.
    // val mrdsDF = sqlContext.load("jdbc", Map("url" -> srcurl, "dbtable" -> "data_quality.mrdsum", "partitionColumn" -> "datetimeintervalid", "lowerBound" -> "2", "upperBound" -> "35037", "numPartitions" -> "100")).cache

    val apAllDF = mrdsDF.filter("code = 'P'").withColumn("mdate", toDate(mrdsDF("mtime"))) // phasename = 'A' if needed
    apAllDF.registerTempTable("apdf")
    
    //prepare active & reactive power data in 96 cols format
    val paData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as P0, max(T1) as P1, max(T2) as P2, max(T3) as P3, max(T4) as P4, max(T5) as P5, max(T6) as P6, max(T7) as P7, max(T8) as P8, max(T9) as P9,
                                                          max(T10) as P10, max(T11) as P11, max(T12) as P12, max(T13) as P13, max(T14) as P14, max(T15) as P15, max(T16) as P16, max(T17) as P17, max(T18) as P18, max(T19) as P19,
                                                          max(T20) as P20, max(T21) as P21, max(T22) as P22, max(T23) as P23, max(T24) as P24, max(T25) as P25, max(T26) as P26, max(T27) as P27, max(T28) as P28, max(T29) as P29,
                                                          max(T30) as P30, max(T31) as P31, max(T32) as P32, max(T33) as P33, max(T34) as P34, max(T35) as P35, max(T36) as P36, max(T37) as P37, max(T38) as P38, max(T39) as P39,
                                                          max(T40) as P40, max(T41) as P41, max(T42) as P42, max(T43) as P43, max(T44) as P44, max(T45) as P45, max(T46) as P46, max(T47) as P47, max(T48) as P48, max(T49) as P49,
                                                          max(T50) as P50, max(T51) as P51, max(T52) as P52, max(T53) as P53, max(T54) as P54, max(T55) as P55, max(T56) as P56, max(T57) as P57, max(T58) as P58, max(T59) as P59,
                                                          max(T60) as P60, max(T61) as P61, max(T62) as P62, max(T63) as P63, max(T64) as P64, max(T65) as P65, max(T66) as P66, max(T67) as P67, max(T68) as P68, max(T69) as P69,
                                                          max(T70) as P70, max(T71) as P71, max(T72) as P72, max(T73) as P73, max(T74) as P74, max(T75) as P75, max(T76) as P76, max(T77) as P77, max(T78) as P78, max(T79) as P79,
                                                          max(T80) as P80, max(T81) as P81, max(T82) as P82, max(T83) as P83, max(T84) as P84, max(T85) as P85, max(T86) as P86, max(T87) as P87, max(T88) as P88, max(T89) as P89,
                                                          max(T90) as P90, max(T91) as P91, max(T92) as P92, max(T93) as P93, max(T94) as P94, max(T95) as P95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from apdf) apdf
                     GROUP BY meterid, mdate""").cache() //57.27M   but in memory TungstenAggregate 21.6 GB + Project 5.8 GB? 
    paData.count
    
    val powerData = paData.withColumn("data_type", lit(1)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "data_type", "data_point_flag", "data_whole_flag", "P0", 
                                      "P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11", "P12", "P13", "P14", "P15", "P16", "P17", "P18", "P19", "P20", "P21", "P22", "P23", "P24", "P25", "P26", "P27", 
                                      "P28", "P29", "P30", "P31", "P32", "P33", "P34", "P35", "P36", "P37", "P38", "P39", "P40", "P41", "P42", "P43", "P44", "P45", "P46", "P47", "P48", "P49", "P50", "P51", "P52", "P53",
                                      "P54", "P55", "P56", "P57", "P58", "P59", "P60", "P61", "P62", "P63", "P64", "P65", "P66", "P67", "P68", "P69", "P70", "P71", "P72", "P73", "P74", "P75", "P76", "P77", "P78", "P79",
                                      "P80", "P81", "P82", "P83", "P84", "P85", "P86", "P87", "P88", "P89", "P90", "P91", "P92", "P93", "P94", "P95" )                

    //powerData.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_power_curve", prop) //write to database table 
    
    paData.unpersist  //free memory
   
    val rpAllDF = mrdsDF.filter("code = 'Q'").withColumn("mdate", toDate(mrdsDF("mtime"))) //and phasename = 'A'
    rpAllDF.registerTempTable("rpdf")

    val qData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as P0, max(T1) as P1, max(T2) as P2, max(T3) as P3, max(T4) as P4, max(T5) as P5, max(T6) as P6, max(T7) as P7, max(T8) as P8, max(T9) as P9,
                                                          max(T10) as P10, max(T11) as P11, max(T12) as P12, max(T13) as P13, max(T14) as P14, max(T15) as P15, max(T16) as P16, max(T17) as P17, max(T18) as P18, max(T19) as P19,
                                                          max(T20) as P20, max(T21) as P21, max(T22) as P22, max(T23) as P23, max(T24) as P24, max(T25) as P25, max(T26) as P26, max(T27) as P27, max(T28) as P28, max(T29) as P29,
                                                          max(T30) as P30, max(T31) as P31, max(T32) as P32, max(T33) as P33, max(T34) as P34, max(T35) as P35, max(T36) as P36, max(T37) as P37, max(T38) as P38, max(T39) as P39,
                                                          max(T40) as P40, max(T41) as P41, max(T42) as P42, max(T43) as P43, max(T44) as P44, max(T45) as P45, max(T46) as P46, max(T47) as P47, max(T48) as P48, max(T49) as P49,
                                                          max(T50) as P50, max(T51) as P51, max(T52) as P52, max(T53) as P53, max(T54) as P54, max(T55) as P55, max(T56) as P56, max(T57) as P57, max(T58) as P58, max(T59) as P59,
                                                          max(T60) as P60, max(T61) as P61, max(T62) as P62, max(T63) as P63, max(T64) as P64, max(T65) as P65, max(T66) as P66, max(T67) as P67, max(T68) as P68, max(T69) as P69,
                                                          max(T70) as P70, max(T71) as P71, max(T72) as P72, max(T73) as P73, max(T74) as P74, max(T75) as P75, max(T76) as P76, max(T77) as P77, max(T78) as P78, max(T79) as P79,
                                                          max(T80) as P80, max(T81) as P81, max(T82) as P82, max(T83) as P83, max(T84) as P84, max(T85) as P85, max(T86) as P86, max(T87) as P87, max(T88) as P88, max(T89) as P89,
                                                          max(T90) as P90, max(T91) as P91, max(T92) as P92, max(T93) as P93, max(T94) as P94, max(T95) as P95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from rpdf) rpdf
                     GROUP BY meterid, mdate""").cache() //57.27M   but in memory TungstenAggregate 21.6 GB + Project 5.8 GB? 
    qData.count
    
    val rpData = qData.withColumn("data_type", lit(5)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "data_type", "data_point_flag", "data_whole_flag", "P0", 
                                      "P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11", "P12", "P13", "P14", "P15", "P16", "P17", "P18", "P19", "P20", "P21", "P22", "P23", "P24", "P25", "P26", "P27", 
                                      "P28", "P29", "P30", "P31", "P32", "P33", "P34", "P35", "P36", "P37", "P38", "P39", "P40", "P41", "P42", "P43", "P44", "P45", "P46", "P47", "P48", "P49", "P50", "P51", "P52", "P53",
                                      "P54", "P55", "P56", "P57", "P58", "P59", "P60", "P61", "P62", "P63", "P64", "P65", "P66", "P67", "P68", "P69", "P70", "P71", "P72", "P73", "P74", "P75", "P76", "P77", "P78", "P79",
                                      "P80", "P81", "P82", "P83", "P84", "P85", "P86", "P87", "P88", "P89", "P90", "P91", "P92", "P93", "P94", "P95" )                

    //rpData.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_power_curve", prop) //write to database table 
    qData.unpersist
    
    val voltDF = mrdsDF.filter("code = 'V'").withColumn("mdate", toDate(mrdsDF("mtime"))) //and phasename = 'A'
    voltDF.registerTempTable("vdf")

    //prepare voltage data in 96 cols format
    val vData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as U0, max(T1) as U1, max(T2) as U2, max(T3) as U3, max(T4) as U4, max(T5) as U5, max(T6) as U6, max(T7) as U7, max(T8) as U8, max(T9) as U9,
                                                          max(T10) as U10, max(T11) as U11, max(T12) as U12, max(T13) as U13, max(T14) as U14, max(T15) as U15, max(T16) as U16, max(T17) as U17, max(T18) as U18, max(T19) as U19,
                                                          max(T20) as U20, max(T21) as U21, max(T22) as U22, max(T23) as U23, max(T24) as U24, max(T25) as U25, max(T26) as U26, max(T27) as U27, max(T28) as U28, max(T29) as U29,
                                                          max(T30) as U30, max(T31) as U31, max(T32) as U32, max(T33) as U33, max(T34) as U34, max(T35) as U35, max(T36) as U36, max(T37) as U37, max(T38) as U38, max(T39) as U39,
                                                          max(T40) as U40, max(T41) as U41, max(T42) as U42, max(T43) as U43, max(T44) as U44, max(T45) as U45, max(T46) as U46, max(T47) as U47, max(T48) as U48, max(T49) as U49,
                                                          max(T50) as U50, max(T51) as U51, max(T52) as U52, max(T53) as U53, max(T54) as U54, max(T55) as U55, max(T56) as U56, max(T57) as U57, max(T58) as U58, max(T59) as U59,
                                                          max(T60) as U60, max(T61) as U61, max(T62) as U62, max(T63) as U63, max(T64) as U64, max(T65) as U65, max(T66) as U66, max(T67) as U67, max(T68) as U68, max(T69) as U69,
                                                          max(T70) as U70, max(T71) as U71, max(T72) as U72, max(T73) as U73, max(T74) as U74, max(T75) as U75, max(T76) as U76, max(T77) as U77, max(T78) as U78, max(T79) as U79,
                                                          max(T80) as U80, max(T81) as U81, max(T82) as U82, max(T83) as U83, max(T84) as U84, max(T85) as U85, max(T86) as U86, max(T87) as U87, max(T88) as U88, max(T89) as U89,
                                                          max(T90) as U90, max(T91) as U91, max(T92) as U92, max(T93) as U93, max(T94) as U94, max(T95) as U95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from vdf) vdf
                     GROUP BY meterid, mdate""").cache() 
    vData.count
    
    val v2Data = vData.withColumn("phase_flag", lit(0)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "phase_flag", "data_point_flag", "data_whole_flag", "U0", 
                                      "U1", "U2", "U3", "U4", "U5", "U6", "U7", "U8", "U9", "U10", "U11", "U12", "U13", "U14", "U15", "U16", "U17", "U18", "U19", "U20", "U21", "U22", "U23", "U24", "U25", "U26", "U27", 
                                      "U28", "U29", "U30", "U31", "U32", "U33", "U34", "U35", "U36", "U37", "U38", "U39", "U40", "U41", "U42", "U43", "U44", "U45", "U46", "U47", "U48", "U49", "U50", "U51", "U52", "U53",
                                      "U54", "U55", "U56", "U57", "U58", "U59", "U60", "U61", "U62", "U63", "U64", "U65", "U66", "U67", "U68", "U69", "U70", "U71", "U72", "U73", "U74", "U75", "U76", "U77", "U78", "U79",
                                      "U80", "U81", "U82", "U83", "U84", "U85", "U86", "U87", "U88", "U89", "U90", "U91", "U92", "U93", "U94", "U95" )                

    //v2Data.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_vol_curve", prop)
    vData.unpersist
    
    val curDF = mrdsDF.filter("code = 'A'").withColumn("mdate", toDate(mrdsDF("mtime"))) //and phasename = 'A'
    curDF.registerTempTable("curdf")

    //prepare current data in 96 cols format
    val curData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as I0, max(T1) as I1, max(T2) as I2, max(T3) as I3, max(T4) as I4, max(T5) as I5, max(T6) as I6, max(T7) as I7, max(T8) as I8, max(T9) as I9,
                                                          max(T10) as I10, max(T11) as I11, max(T12) as I12, max(T13) as I13, max(T14) as I14, max(T15) as I15, max(T16) as I16, max(T17) as I17, max(T18) as I18, max(T19) as I19,
                                                          max(T20) as I20, max(T21) as I21, max(T22) as I22, max(T23) as I23, max(T24) as I24, max(T25) as I25, max(T26) as I26, max(T27) as I27, max(T28) as I28, max(T29) as I29,
                                                          max(T30) as I30, max(T31) as I31, max(T32) as I32, max(T33) as I33, max(T34) as I34, max(T35) as I35, max(T36) as I36, max(T37) as I37, max(T38) as I38, max(T39) as I39,
                                                          max(T40) as I40, max(T41) as I41, max(T42) as I42, max(T43) as I43, max(T44) as I44, max(T45) as I45, max(T46) as I46, max(T47) as I47, max(T48) as I48, max(T49) as I49,
                                                          max(T50) as I50, max(T51) as I51, max(T52) as I52, max(T53) as I53, max(T54) as I54, max(T55) as I55, max(T56) as I56, max(T57) as I57, max(T58) as I58, max(T59) as I59,
                                                          max(T60) as I60, max(T61) as I61, max(T62) as I62, max(T63) as I63, max(T64) as I64, max(T65) as I65, max(T66) as I66, max(T67) as I67, max(T68) as I68, max(T69) as I69,
                                                          max(T70) as I70, max(T71) as I71, max(T72) as I72, max(T73) as I73, max(T74) as I74, max(T75) as I75, max(T76) as I76, max(T77) as I77, max(T78) as I78, max(T79) as I79,
                                                          max(T80) as I80, max(T81) as I81, max(T82) as I82, max(T83) as I83, max(T84) as I84, max(T85) as I85, max(T86) as I86, max(T87) as I87, max(T88) as I88, max(T89) as I89,
                                                          max(T90) as I90, max(T91) as I91, max(T92) as I92, max(T93) as I93, max(T94) as I94, max(T95) as I95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from curdf) curdf
                     GROUP BY meterid, mdate""").cache() 
    curData.count
    
    val cur2Data = curData.withColumn("phase_flag", lit(0)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "phase_flag", "data_point_flag", "data_whole_flag", "I0", 
                                      "I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I10", "I11", "I12", "I13", "I14", "I15", "I16", "I17", "I18", "I19", "I20", "I21", "I22", "I23", "I24", "I25", "I26", "I27", 
                                      "I28", "I29", "I30", "I31", "I32", "I33", "I34", "I35", "I36", "I37", "I38", "I39", "I40", "I41", "I42", "I43", "I44", "I45", "I46", "I47", "I48", "I49", "I50", "I51", "I52", "I53",
                                      "I54", "I55", "I56", "I57", "I58", "I59", "I60", "I61", "I62", "I63", "I64", "I65", "I66", "I67", "I68", "I69", "I70", "I71", "I72", "I73", "I74", "I75", "I76", "I77", "I78", "I79",
                                      "I80", "I81", "I82", "I83", "I84", "I85", "I86", "I87", "I88", "I89", "I90", "I91", "I92", "I93", "I94", "I95" )                

    //cur2Data.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_cur_curve", prop)
    curData.unpersist

    val pfDF = mrdsDF.filter("code = 'PF'").withColumn("mdate", toDate(mrdsDF("mtime")))
    pfDF.registerTempTable("pfdf")

    val pfData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as C0, max(T1) as C1, max(T2) as C2, max(T3) as C3, max(T4) as C4, max(T5) as C5, max(T6) as C6, max(T7) as C7, max(T8) as C8, max(T9) as C9,
                                                          max(T10) as C10, max(T11) as C11, max(T12) as C12, max(T13) as C13, max(T14) as C14, max(T15) as C15, max(T16) as C16, max(T17) as C17, max(T18) as C18, max(T19) as C19,
                                                          max(T20) as C20, max(T21) as C21, max(T22) as C22, max(T23) as C23, max(T24) as C24, max(T25) as C25, max(T26) as C26, max(T27) as C27, max(T28) as C28, max(T29) as C29,
                                                          max(T30) as C30, max(T31) as C31, max(T32) as C32, max(T33) as C33, max(T34) as C34, max(T35) as C35, max(T36) as C36, max(T37) as C37, max(T38) as C38, max(T39) as C39,
                                                          max(T40) as C40, max(T41) as C41, max(T42) as C42, max(T43) as C43, max(T44) as C44, max(T45) as C45, max(T46) as C46, max(T47) as C47, max(T48) as C48, max(T49) as C49,
                                                          max(T50) as C50, max(T51) as C51, max(T52) as C52, max(T53) as C53, max(T54) as C54, max(T55) as C55, max(T56) as C56, max(T57) as C57, max(T58) as C58, max(T59) as C59,
                                                          max(T60) as C60, max(T61) as C61, max(T62) as C62, max(T63) as C63, max(T64) as C64, max(T65) as C65, max(T66) as C66, max(T67) as C67, max(T68) as C68, max(T69) as C69,
                                                          max(T70) as C70, max(T71) as C71, max(T72) as C72, max(T73) as C73, max(T74) as C74, max(T75) as C75, max(T76) as C76, max(T77) as C77, max(T78) as C78, max(T79) as C79,
                                                          max(T80) as C80, max(T81) as C81, max(T82) as C82, max(T83) as C83, max(T84) as C84, max(T85) as C85, max(T86) as C86, max(T87) as C87, max(T88) as C88, max(T89) as C89,
                                                          max(T90) as C90, max(T91) as C91, max(T92) as C92, max(T93) as C93, max(T94) as C94, max(T95) as C95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from pfdf) pfdf
                     GROUP BY meterid, mdate""").cache() 
    pfData.count

    val pf2Data = pfData.withColumn("phase_flag", lit(0)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "phase_flag", "data_point_flag", "data_whole_flag", "C0", 
                                      "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26", "C27", 
                                      "C28", "C29", "C30", "C31", "C32", "C33", "C34", "C35", "C36", "C37", "C38", "C39", "C40", "C41", "C42", "C43", "C44", "C45", "C46", "C47", "C48", "C49", "C50", "C51", "C52", "C53",
                                      "C54", "C55", "C56", "C57", "C58", "C59", "C60", "C61", "C62", "C63", "C64", "C65", "C66", "C67", "C68", "C69", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C77", "C78", "C79",
                                      "C80", "C81", "C82", "C83", "C84", "C85", "C86", "C87", "C88", "C89", "C90", "C91", "C92", "C93", "C94", "C95" )                

    //pf2Data.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_factor_curve", prop)
    pfData.unpersist

    //energy readings
    val epDF = mrdsDF.filter("code = 'EP'").withColumn("mdate", toDate(mrdsDF("mtime")))
    epDF.registerTempTable("epdf")

    val epData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as R0, max(T1) as R1, max(T2) as R2, max(T3) as R3, max(T4) as R4, max(T5) as R5, max(T6) as R6, max(T7) as R7, max(T8) as R8, max(T9) as R9,
                                                          max(T10) as R10, max(T11) as R11, max(T12) as R12, max(T13) as R13, max(T14) as R14, max(T15) as R15, max(T16) as R16, max(T17) as R17, max(T18) as R18, max(T19) as R19,
                                                          max(T20) as R20, max(T21) as R21, max(T22) as R22, max(T23) as R23, max(T24) as R24, max(T25) as R25, max(T26) as R26, max(T27) as R27, max(T28) as R28, max(T29) as R29,
                                                          max(T30) as R30, max(T31) as R31, max(T32) as R32, max(T33) as R33, max(T34) as R34, max(T35) as R35, max(T36) as R36, max(T37) as R37, max(T38) as R38, max(T39) as R39,
                                                          max(T40) as R40, max(T41) as R41, max(T42) as R42, max(T43) as R43, max(T44) as R44, max(T45) as R45, max(T46) as R46, max(T47) as R47, max(T48) as R48, max(T49) as R49,
                                                          max(T50) as R50, max(T51) as R51, max(T52) as R52, max(T53) as R53, max(T54) as R54, max(T55) as R55, max(T56) as R56, max(T57) as R57, max(T58) as R58, max(T59) as R59,
                                                          max(T60) as R60, max(T61) as R61, max(T62) as R62, max(T63) as R63, max(T64) as R64, max(T65) as R65, max(T66) as R66, max(T67) as R67, max(T68) as R68, max(T69) as R69,
                                                          max(T70) as R70, max(T71) as R71, max(T72) as R72, max(T73) as R73, max(T74) as R74, max(T75) as R75, max(T76) as R76, max(T77) as R77, max(T78) as R78, max(T79) as R79,
                                                          max(T80) as R80, max(T81) as R81, max(T82) as R82, max(T83) as R83, max(T84) as R84, max(T85) as R85, max(T86) as R86, max(T87) as R87, max(T88) as R88, max(T89) as R89,
                                                          max(T90) as R90, max(T91) as R91, max(T92) as R92, max(T93) as R93, max(T94) as R94, max(T95) as R95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from epdf) epdf
                     GROUP BY meterid, mdate""").cache() 

    val ep2Data = epData.withColumn("data_type", lit(1)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "data_type", "data_point_flag", "data_whole_flag", "R0", 
                                      "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10", "R11", "R12", "R13", "R14", "R15", "R16", "R17", "R18", "R19", "R20", "R21", "R22", "R23", "R24", "R25", "R26", "R27", 
                                      "R28", "R29", "R30", "R31", "R32", "R33", "R34", "R35", "R36", "R37", "R38", "R39", "R40", "R41", "R42", "R43", "R44", "R45", "R46", "R47", "R48", "R49", "R50", "R51", "R52", "R53",
                                      "R54", "R55", "R56", "R57", "R58", "R59", "R60", "R61", "R62", "R63", "R64", "R65", "R66", "R67", "R68", "R69", "R70", "R71", "R72", "R73", "R74", "R75", "R76", "R77", "R78", "R79",
                                      "R80", "R81", "R82", "R83", "R84", "R85", "R86", "R87", "R88", "R89", "R90", "R91", "R92", "R93", "R94", "R95" )                

    //ep2Data.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_read_curve", prop)
    epData.unpersist                     

    val eqDF = mrdsDF.filter("code = 'EQ'").withColumn("mdate", toDate(mrdsDF("mtime")))
    eqDF.registerTempTable("eqdf")

    val eqData = sqlContext.sql("""SELECT meterid, mdate, max(T0) as R0, max(T1) as R1, max(T2) as R2, max(T3) as R3, max(T4) as R4, max(T5) as R5, max(T6) as R6, max(T7) as R7, max(T8) as R8, max(T9) as R9,
                                                          max(T10) as R10, max(T11) as R11, max(T12) as R12, max(T13) as R13, max(T14) as R14, max(T15) as R15, max(T16) as R16, max(T17) as R17, max(T18) as R18, max(T19) as R19,
                                                          max(T20) as R20, max(T21) as R21, max(T22) as R22, max(T23) as R23, max(T24) as R24, max(T25) as R25, max(T26) as R26, max(T27) as R27, max(T28) as R28, max(T29) as R29,
                                                          max(T30) as R30, max(T31) as R31, max(T32) as R32, max(T33) as R33, max(T34) as R34, max(T35) as R35, max(T36) as R36, max(T37) as R37, max(T38) as R38, max(T39) as R39,
                                                          max(T40) as R40, max(T41) as R41, max(T42) as R42, max(T43) as R43, max(T44) as R44, max(T45) as R45, max(T46) as R46, max(T47) as R47, max(T48) as R48, max(T49) as R49,
                                                          max(T50) as R50, max(T51) as R51, max(T52) as R52, max(T53) as R53, max(T54) as R54, max(T55) as R55, max(T56) as R56, max(T57) as R57, max(T58) as R58, max(T59) as R59,
                                                          max(T60) as R60, max(T61) as R61, max(T62) as R62, max(T63) as R63, max(T64) as R64, max(T65) as R65, max(T66) as R66, max(T67) as R67, max(T68) as R68, max(T69) as R69,
                                                          max(T70) as R70, max(T71) as R71, max(T72) as R72, max(T73) as R73, max(T74) as R74, max(T75) as R75, max(T76) as R76, max(T77) as R77, max(T78) as R78, max(T79) as R79,
                                                          max(T80) as R80, max(T81) as R81, max(T82) as R82, max(T83) as R83, max(T84) as R84, max(T85) as R85, max(T86) as R86, max(T87) as R87, max(T88) as R88, max(T89) as R89,
                                                          max(T90) as R90, max(T91) as R91, max(T92) as R92, max(T93) as R93, max(T94) as R94, max(T95) as R95
                     FROM (select meterid, mdate,
                                  CASE pmod(datetimeintervalid, 96) WHEN 2 THEN value end T0, CASE pmod(datetimeintervalid, 96) WHEN 3 THEN value end T1, CASE pmod(datetimeintervalid, 96) WHEN 4 THEN value end T2,
                                  CASE pmod(datetimeintervalid, 96) WHEN 5 THEN value end T3, CASE pmod(datetimeintervalid, 96) WHEN 6 THEN value end T4, CASE pmod(datetimeintervalid, 96) WHEN 7 THEN value end T5,
                                  CASE pmod(datetimeintervalid, 96) WHEN 8 THEN value end T6, CASE pmod(datetimeintervalid, 96) WHEN 9 THEN value end T7, CASE pmod(datetimeintervalid, 96) WHEN 10 THEN value end T8,
                                  CASE pmod(datetimeintervalid, 96) WHEN 11 THEN value end T9, CASE pmod(datetimeintervalid, 96) WHEN 12 THEN value end T10, CASE pmod(datetimeintervalid, 96) WHEN 13 THEN value end T11,
                                  CASE pmod(datetimeintervalid, 96) WHEN 14 THEN value end T12, CASE pmod(datetimeintervalid, 96) WHEN 15 THEN value end T13, CASE pmod(datetimeintervalid, 96) WHEN 16 THEN value end T14,
                                  CASE pmod(datetimeintervalid, 96) WHEN 17 THEN value end T15, CASE pmod(datetimeintervalid, 96) WHEN 18 THEN value end T16, CASE pmod(datetimeintervalid, 96) WHEN 19 THEN value end T17,
                                  CASE pmod(datetimeintervalid, 96) WHEN 20 THEN value end T18, CASE pmod(datetimeintervalid, 96) WHEN 21 THEN value end T19, CASE pmod(datetimeintervalid, 96) WHEN 22 THEN value end T20,
                                  CASE pmod(datetimeintervalid, 96) WHEN 23 THEN value end T21, CASE pmod(datetimeintervalid, 96) WHEN 24 THEN value end T22, CASE pmod(datetimeintervalid, 96) WHEN 25 THEN value end T23,
                                  CASE pmod(datetimeintervalid, 96) WHEN 26 THEN value end T24, CASE pmod(datetimeintervalid, 96) WHEN 27 THEN value end T25, CASE pmod(datetimeintervalid, 96) WHEN 28 THEN value end T26,
                                  CASE pmod(datetimeintervalid, 96) WHEN 29 THEN value end T27, CASE pmod(datetimeintervalid, 96) WHEN 30 THEN value end T28, CASE pmod(datetimeintervalid, 96) WHEN 31 THEN value end T29,
                                  CASE pmod(datetimeintervalid, 96) WHEN 32 THEN value end T30, CASE pmod(datetimeintervalid, 96) WHEN 33 THEN value end T31, CASE pmod(datetimeintervalid, 96) WHEN 34 THEN value end T32,
                                  CASE pmod(datetimeintervalid, 96) WHEN 35 THEN value end T33, CASE pmod(datetimeintervalid, 96) WHEN 36 THEN value end T34, CASE pmod(datetimeintervalid, 96) WHEN 37 THEN value end T35,
                                  CASE pmod(datetimeintervalid, 96) WHEN 38 THEN value end T36, CASE pmod(datetimeintervalid, 96) WHEN 39 THEN value end T37, CASE pmod(datetimeintervalid, 96) WHEN 40 THEN value end T38,
                                  CASE pmod(datetimeintervalid, 96) WHEN 41 THEN value end T39, CASE pmod(datetimeintervalid, 96) WHEN 42 THEN value end T40, CASE pmod(datetimeintervalid, 96) WHEN 43 THEN value end T41,
                                  CASE pmod(datetimeintervalid, 96) WHEN 44 THEN value end T42, CASE pmod(datetimeintervalid, 96) WHEN 45 THEN value end T43, CASE pmod(datetimeintervalid, 96) WHEN 46 THEN value end T44,
                                  CASE pmod(datetimeintervalid, 96) WHEN 47 THEN value end T45, CASE pmod(datetimeintervalid, 96) WHEN 48 THEN value end T46, CASE pmod(datetimeintervalid, 96) WHEN 49 THEN value end T47,
                                  CASE pmod(datetimeintervalid, 96) WHEN 50 THEN value end T48, CASE pmod(datetimeintervalid, 96) WHEN 51 THEN value end T49, CASE pmod(datetimeintervalid, 96) WHEN 52 THEN value end T50,
                                  CASE pmod(datetimeintervalid, 96) WHEN 53 THEN value end T51, CASE pmod(datetimeintervalid, 96) WHEN 54 THEN value end T52, CASE pmod(datetimeintervalid, 96) WHEN 55 THEN value end T53,
                                  CASE pmod(datetimeintervalid, 96) WHEN 56 THEN value end T54, CASE pmod(datetimeintervalid, 96) WHEN 57 THEN value end T55, CASE pmod(datetimeintervalid, 96) WHEN 58 THEN value end T56,
                                  CASE pmod(datetimeintervalid, 96) WHEN 59 THEN value end T57, CASE pmod(datetimeintervalid, 96) WHEN 60 THEN value end T58, CASE pmod(datetimeintervalid, 96) WHEN 61 THEN value end T59,
                                  CASE pmod(datetimeintervalid, 96) WHEN 62 THEN value end T60, CASE pmod(datetimeintervalid, 96) WHEN 63 THEN value end T61, CASE pmod(datetimeintervalid, 96) WHEN 64 THEN value end T62,
                                  CASE pmod(datetimeintervalid, 96) WHEN 65 THEN value end T63, CASE pmod(datetimeintervalid, 96) WHEN 66 THEN value end T64, CASE pmod(datetimeintervalid, 96) WHEN 67 THEN value end T65,
                                  CASE pmod(datetimeintervalid, 96) WHEN 68 THEN value end T66, CASE pmod(datetimeintervalid, 96) WHEN 69 THEN value end T67, CASE pmod(datetimeintervalid, 96) WHEN 70 THEN value end T68,
                                  CASE pmod(datetimeintervalid, 96) WHEN 71 THEN value end T69, CASE pmod(datetimeintervalid, 96) WHEN 72 THEN value end T70, CASE pmod(datetimeintervalid, 96) WHEN 73 THEN value end T71,
                                  CASE pmod(datetimeintervalid, 96) WHEN 74 THEN value end T72, CASE pmod(datetimeintervalid, 96) WHEN 75 THEN value end T73, CASE pmod(datetimeintervalid, 96) WHEN 76 THEN value end T74,
                                  CASE pmod(datetimeintervalid, 96) WHEN 77 THEN value end T75, CASE pmod(datetimeintervalid, 96) WHEN 78 THEN value end T76, CASE pmod(datetimeintervalid, 96) WHEN 79 THEN value end T77,
                                  CASE pmod(datetimeintervalid, 96) WHEN 80 THEN value end T78, CASE pmod(datetimeintervalid, 96) WHEN 81 THEN value end T79, CASE pmod(datetimeintervalid, 96) WHEN 82 THEN value end T80,
                                  CASE pmod(datetimeintervalid, 96) WHEN 83 THEN value end T81, CASE pmod(datetimeintervalid, 96) WHEN 84 THEN value end T82, CASE pmod(datetimeintervalid, 96) WHEN 85 THEN value end T83,
                                  CASE pmod(datetimeintervalid, 96) WHEN 86 THEN value end T84, CASE pmod(datetimeintervalid, 96) WHEN 87 THEN value end T85, CASE pmod(datetimeintervalid, 96) WHEN 88 THEN value end T86,
                                  CASE pmod(datetimeintervalid, 96) WHEN 89 THEN value end T87, CASE pmod(datetimeintervalid, 96) WHEN 90 THEN value end T88, CASE pmod(datetimeintervalid, 96) WHEN 91 THEN value end T89,
                                  CASE pmod(datetimeintervalid, 96) WHEN 92 THEN value end T90, CASE pmod(datetimeintervalid, 96) WHEN 93 THEN value end T91, CASE pmod(datetimeintervalid, 96) WHEN 94 THEN value end T92,
                                  CASE pmod(datetimeintervalid, 96) WHEN 95 THEN value end T93, CASE pmod(datetimeintervalid, 96) WHEN 0  THEN value end T94, CASE pmod(datetimeintervalid, 96) WHEN 1  THEN value end T95   
                           from eqdf) eqdf
                     GROUP BY meterid, mdate""").cache() 

    val eq2Data = eqData.withColumn("data_type", lit(1)).withColumn("data_point_flag", lit(1)).withColumn("data_whole_flag", lit(1)).select("meterid", "mdate", "data_type", "data_point_flag", "data_whole_flag", "R0", 
                                      "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10", "R11", "R12", "R13", "R14", "R15", "R16", "R17", "R18", "R19", "R20", "R21", "R22", "R23", "R24", "R25", "R26", "R27", 
                                      "R28", "R29", "R30", "R31", "R32", "R33", "R34", "R35", "R36", "R37", "R38", "R39", "R40", "R41", "R42", "R43", "R44", "R45", "R46", "R47", "R48", "R49", "R50", "R51", "R52", "R53",
                                      "R54", "R55", "R56", "R57", "R58", "R59", "R60", "R61", "R62", "R63", "R64", "R65", "R66", "R67", "R68", "R69", "R70", "R71", "R72", "R73", "R74", "R75", "R76", "R77", "R78", "R79",
                                      "R80", "R81", "R82", "R83", "R84", "R85", "R86", "R87", "R88", "R89", "R90", "R91", "R92", "R93", "R94", "R95" )                

    //eq2Data.coalesce(numProcesses).write.mode("append").jdbc(tgturl2, "e_mp_read_curve", prop)
    eqData.unpersist                     
  }
}    

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.{ DateTime, Days }
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat
import scala.reflect.runtime.universe
import scala.sys.process.stringToProcess
import scala.util.control.Exception.allCatch
import org.apache.spark.ml.Pipeline




// download data from 13-20 June
val file = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/74eb5d9a-f637-4764-bb39-13fd2e769a00")
val row = file.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val download = spark.createDataFrame(row, StructType(fields))
val gpmapping = spark.read.format("csv").load("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/gpmappmapping.csv")
val gpmapping1 = gpmapping.withColumnRenamed("_c0","app_gpm_id").withColumnRenamed("_c1","app_id").select("app_gpm_id","app_id").filter("app_gpm_id != 'NULL'").filter("app_id != 'NULL'").distinct()
val df1 = download.join(gpmapping1,"app_gpm_id").select("request_uid", "app_id","request_date").filter("request_date >= '2018-06-13'").filter("request_date <= '2018-06-20'").distinct()

//  rrnob data from 13-20 June
val rrnob = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/rrnob_22may-20june")
val df2 = rrnob.filter("request_date >= '2018-06-13'").filter("request_date <= '2018-06-20'").distinct()

// join of rrnob and download data on date and uid 
val df3 = df1.join(df2,Seq("request_date","request_uid")).distinct()

// user count
val df4 = df2.groupBy("request_date","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id").agg(count("*")).withColumnRenamed("count(1)","user_count")

// download count
val df5 = df3.groupBy("request_date", "app_id","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id").agg(count("*")).withColumnRenamed("count(1)","downloads")

// let outer join of df4 and df5
val df6 = df4.join(df5,Seq("request_date","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id"),"left_outer").withColumn("CVU", $"downloads" / $"user_count").na.fill(0)



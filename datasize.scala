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


// creating appsbag
val df1 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/purchase_download_custom_18april-12june")
val df2 = df1.groupBy("request_uid").agg(collect_list("app_id").alias("appsbag"))

// taking a join of appsbag and rrnob on uid level
val rrnob = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/rrnob_22may-20june")
val df3 = rrnob.filter("request_date >= '2018-06-13'").filter("request_date <= '2018-06-20'")
val df4 = df3.join(df2,Seq("request_uid"),"left_outer").distinct()

// download data for 8 Days
val file = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/74eb5d9a-f637-4764-bb39-13fd2e769a00")
val row = file.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val download = spark.createDataFrame(row, StructType(fields))
val gpmapping = spark.read.format("csv").load("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/gpmappmapping.csv")
val gpmapping1 = gpmapping.withColumnRenamed("_c0","app_gpm_id").withColumnRenamed("_c1","app_id").select("app_gpm_id","app_id").filter("app_gpm_id != 'NULL'").filter("app_id != 'NULL'").distinct()

val df5 = download.join(gpmapping1,"app_gpm_id").select("request_uid", "app_id","request_date").filter("request_date >= '2018-06-13'").filter("request_date <= '2018-06-20'").distinct()

//  taking a join of appsbag and download data on uid level
val df6 = df5.join(df2,"left_outer").distinct()

// taking join of rrnob and download data on date,uid and appsbag
val df7 = df6.join(df4,Seq("request_date","request_uid","appsbag")).distinct()

// groupby to get user count 
val df8 = df4.groupBy("request_date","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appsbag").agg(count("*")).withColumnRenamed("count(1)","user_count")

// groupby to get download count 
val df9 = df7.groupBy("request_date", "app_id","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appsbag").agg(count("*")).withColumnRenamed("count(1)","downloads")

// joining download and user count
val df10 = df8.join(df9,Seq("request_date","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appsbag")).withColumn("CVU", $"downloads" / $"user_count")

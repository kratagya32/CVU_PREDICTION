
val file_d = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/74eb5d9a-f637-4764-bb39-13fd2e769a00")
val row_d = file_d.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields_d = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val download = spark.createDataFrame(row_d, StructType(fields_d))
val download_events = download.select( "request_uid", "app_gpm_id","request_date").filter("request_date >= '2018-04-18'").filter("request_date <= '2018-06-12'")

val file_c = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/f19d789e-a575-4116-811b-e077764a256b")
val row_c = file_c.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields_c = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val custom = spark.createDataFrame(row_c, StructType(fields_c))
val custom_events = custom.select( "request_uid", "app_gpm_id","request_date").filter("request_date >= '2018-04-18'").filter("request_date <= '2018-06-12'")

val file_p = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/d5d8cc75-d9d5-4cfe-9f79-409dd95ef3bc")
val row_p = file_c.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields_p = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val purchase = spark.createDataFrame(row_p, StructType(fields_p))
val purchase_events = purchase.select( "request_uid", "app_gpm_id", "request_date").filter("request_date >= '2018-04-18'").filter("request_date <= '2018-06-12'")


val df = download_events.unionAll(custom_events).unionAll(purchase_events).distinct()
val gpmapping = spark.read.format("csv").load("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/gpmappmapping.csv")
val gpmapping1 = gpmapping.withColumnRenamed("_c0","app_gpm_id").withColumnRenamed("_c1","app_id").select("app_gpm_id","app_id").filter("app_gpm_id != 'NULL'").filter("app_id != 'NULL'").distinct()

// purchase download and custom data for 60 days
val df1 = df.join(gpmapping1,"app_gpm_id").select("request_uid","app_id").distinct()

df1.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/purchase_download_custom_18april-12june")
val df1 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/purchase_download_custom_18april-12june")




def numDayBack(date: String, diff: Int) = {
     DateTime.parse(date, DateTimeFormat.forPattern("yyyy-MM-dd"))
     .plusDays(diff)
     .toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
     }
val train_enddate = "2018-06-20"
val dates30daysTrain = (0 to 29).map(x => numDayBack(train_enddate, -x).toSeq)

val adpool_reqs_Train = dates30daysTrain.map(x => "/secure/projects/perf/data-pipeline/data/rrnob/adrequest/" + x)
val rrnob = adpool_reqs_Train.map(x => spark.read.parquet(x).select("request_date", "request_uid", "display_marketing_name", "os_version_id", "market_id","device_os_id","country_id","device_type_id")).reduce((x, y) => x.union(y)).distinct()

rrnob.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/rrnob_22may-20june")
val rrnob = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/rrnob_22may-20june")
val rrnob = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/rrnob_22may-20june")
// join on df1 and rrnob on uid level
val df2 = df1.join(rrnob,"request_uid")

df2.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/AppOwnership18April-12June2018")




// download data for last 7 days
val file = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/74eb5d9a-f637-4764-bb39-13fd2e769a00")
val row = file.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val download_7days = spark.createDataFrame(row, StructType(fields))
val download_events_7days = download_7days.select( "request_uid", "app_gpm_id","request_date").filter("request_date >= '2018-06-06'").filter("request_date <= '2018-06-12'")
val df3 = download_events_7days.join(gpmapping1,"app_gpm_id").select("request_uid","app_id").distinct()



// join of last 7 days data with rrnob on request_uid
val df4 = df3.join(rrnob,Seq("request_uid")).distinct()

df4.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/TargetApp6June-12June2018")

// user appsbag
val df5 = df1.groupBy("request_uid").agg(collect_list("app_id").alias("appsbag"))

// rrnob for 8 days
val df6 = rrnob.filter("request_date >= '2018-06-13'").filter("request_date <= '2018-06-20'")

// join of df5 and df6 on request_uid
val df7 = df6.join(df5,Seq("request_uid"),"left_outer").distinct()

df7.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/requestfeed13-20june")
val df7= spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/requestfeed13-20june")

// download data train and test period

val file_d1 = sc.textFile("hdfs://platinum/secure/projects/lens/lensreports/hdfsout/74eb5d9a-f637-4764-bb39-13fd2e769a00")
val row_d1 = file_d1.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields_d1 = StructType(Seq(StructField("request_uid", StringType), StructField("app_gpm_id", StringType), StructField("request_date", StringType)))
val download1 = spark.createDataFrame(row_d1, StructType(fields_d1))
val gpmapping = spark.read.format("csv").load("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/gpmappmapping.csv")
val gpmapping1 = gpmapping.withColumnRenamed("_c0","app_gpm_id").withColumnRenamed("_c1","app_id").select("app_gpm_id","app_id").filter("app_gpm_id != 'NULL'").filter("app_id != 'NULL'").distinct()

val df8 = download1.join(gpmapping1,"app_gpm_id").select("request_uid", "app_id","request_date").filter("request_date >= '2018-06-13'").filter("request_date <= '2018-06-20'").distinct()

// apps target
val appsTarget = df8.groupBy("app_id").agg(count("request_uid").alias("rqidcount")).filter("rqidcount>=10000").select("app_id").rdd.map(r => r.getString(0)).collect()
val appsTarget1 = appsTarget.toSeq


val ownership = spark.read.format("parquet").load("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/feed56daysAllOwnAppsNew")
val high = 5
val lo = -3
val appPairCVUDF = ownership.withColumn("log_norm_cvu_ROUNDED", ownership("log_norm_cvu_ROUNDED").cast("int")).withColumn("log_norm_cvu_ROUNDED", when(col("log_norm_cvu_ROUNDED") >= high, high).otherwise(col("log_norm_cvu_ROUNDED"))).withColumn("log_norm_cvu_ROUNDED", when(col("log_norm_cvu_ROUNDED") <= lo, lo).otherwise(col("log_norm_cvu_ROUNDED"))).withColumn("log_norm_cvu_ROUNDED", col("log_norm_cvu_ROUNDED").-(lit(lo)))

val appPairDTCVU = appPairCVUDF.collect().map(r => ((r.getAs[String]("country_id"), r.getAs[String]("device_os_id"), r.getAs[String]("device_type_id"), r.getAs[String]("app_a0"), r.getAs[String]("app_a1")), r.getAs[Int]("log_norm_cvu_ROUNDED"))).toMap

def enrichScore(appPairDTCVU: Map[(String, String, String, String, String), Int], appTarget: String, appsbag:Seq[String], country_id:String, device_os_id:String, device_type_id:String , size: Int = 9) = {
    var wordvec = Array.fill(size)(0)
    appsbag.map(x => appPairDTCVU.getOrElse((country_id,device_os_id,device_type_id,appTarget, x), -1))
      .filter(x => x != -1)
      .foreach(x => wordvec.update(x, 1))
    wordvec.deep.toString()

  }

  def enrichScoreTargetApps(appPairDTCVU: Map[(String, String, String, String, String), Int], r: Row, appsTarget: Seq[String], size: Int = 9) = {

    val appsbag = r.getAs[Seq[String]]("appsbag")
    val devtype = r.getAs[String]("device_type_id")
    val countryid = r.getAs[String]("country_id")
    val deviceos = r.getAs[String]("device_os_id")
    
    appsTarget.map(x => (x, enrichScore(appPairDTCVU,x,appsbag,countryid,deviceos,devtype)))
      .map(x => Row.fromSeq(r.toSeq ++ Seq(x._1, x._2)))

  }

  def genTargetAppEvalMat(data_1: DataFrame, appsTarget: Seq[String], appPairDTCVU: Map[(String, String, String, String, String), Int]) = {
    val newSchema = StructType(data_1.schema.fields ++ Seq(StructField("appTarget", StringType), StructField("appOwnCVUEnc", StringType)))
    val rddTrainAppOwnEnc = data_1.rdd.flatMap(r => enrichScoreTargetApps(appPairDTCVU, r, appsTarget))
    val dfTrainAppOwnEnc = spark.sqlContext.createDataFrame(rddTrainAppOwnEnc, newSchema)
    dfTrainAppOwnEnc.select("request_date","request_uid", "appTarget","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appOwnCVUEnc")
  }
  // appencoding feature for request data
val appencoding = genTargetAppEvalMat(df7, appsTarget1, appPairDTCVU)
val df10 = appencoding.withColumnRenamed("appTarget","app_id").withColumn("request_date1",appencoding("request_date").cast(StringType)).select("request_date1","request_uid", "app_id","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appOwnCVUEnc").withColumnRenamed("request_date1","request_date")
// join of df10 and download data to get the numerator 
val df11 = df10.join(df8,Seq("request_date","request_uid","app_id"))

val df12 = df10.groupBy("request_date", "app_id","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appOwnCVUEnc").agg(count("*")).withColumnRenamed("count(1)","user_count")

val df13 = df11.groupBy("request_date", "app_id","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appOwnCVUEnc").agg(count("*")).withColumnRenamed("count(1)","downloads")

val df14 = df12.join(df13,Seq("request_date", "app_id","market_id","display_marketing_name","os_version_id","country_id","device_type_id","device_os_id","appOwnCVUEnc"),"left_outer").withColumn("CVU", $"downloads" / $"user_count").na.fill(0)


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
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.DateType
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.classification.LogisticRegression

def numDayBack(date: String, diff: Int) = {
   DateTime.parse(date, DateTimeFormat.forPattern("yyyy-MM-dd"))
     .plusDays(diff)
     .toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
 }
 def readData(x: String) = {
   println("Reading " + x)
   val D = spark.read.format("orc").load(x)
   val cols = D.columns
   D.select(cols.apply(6), cols.apply(10), cols.apply(14))
     .withColumnRenamed(cols.apply(6), "request_uid")
     .withColumnRenamed(cols.apply(10), "request_date")
     .withColumnRenamed(cols.apply(14), "app_id").distinct()
 }

// getting purchase download and custom data for making the appsbag on user level.
val purchase_download_custom = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/AppOwnershipApril2018/")
val apps = purchase_download_custom.select("request_uid","market_id").distinct()
val DAppOwnStore = apps.groupBy("request_uid").agg(collect_list("market_id").alias("appsbag"))

// val Appscount = DAppOwnStore.select(size($"appsbag").as("no_of_apps"))
// Appscount.agg(avg("no_of_apps")).first.get(0)


// getting the download data for first 8 days.
val data_enddate = "2018-05-08"
val dates8days = (0 to 7).map(x => numDayBack(data_enddate, -x).replaceAll("-", "/")).toSeq
val downloaded = dates8days.map(x => "/secure/projects/perf/hive_data/download_events/load_time=" + x + "*")
val downloaded_events = downloaded.map(x => readData(x)).reduce((x, y) => x.union(y)).distinct() 
/*
root
 |-- request_uid: string (nullable = true)
 |-- request_date: timestamp (nullable = true)
 |-- app_id: string (nullable = true)

*/   

val download = downloaded_events.withColumn("request_date1", downloaded_events("request_date").cast(DateType)).select("request_uid","app_id","request_date1").withColumnRenamed("request_date1","request_date").filter("request_date >= '2018-05-01'").filter("request_date <= '2018-05-08'")
val download_event = download.withColumn("request_date1",download("request_date").cast(StringType)).select("request_uid","app_id","request_date1").withColumnRenamed("request_date1","request_date")

// download_event.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/downloaded1to8May")

val download_event = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/downloaded1to8May")



// getting the request data for first 8 days.
val file = sc.textFile("hdfs://platinum/secure/projects/ds-exp/lens/hdfsout/598e9f28-bb24-4604-bbb3-219210f3adbc")
val row = file.map(line => Row.fromSeq(line.split(",").map(_.trim.replaceAll("\"", "")).toSeq))
val fields = StructType(Seq(StructField("request_date", StringType), StructField("request_uid", StringType), StructField("country_id", StringType), StructField("device_os_id", StringType), StructField("os_version_id", StringType), StructField("display_marketing_name", StringType),StructField("market_id", StringType), StructField("device_type_id", StringType),StructField("constant", StringType) ))
val request = spark.createDataFrame(row, StructType(fields))
val ad_request = request.select("request_date", "request_uid", "display_marketing_name", "os_version_id", "market_id","device_os_id","country_id","device_type_id").filter("request_date >= '2018-05-01'").filter("request_date <= '2018-05-07'")


val ad_request_1 = spark.read.format("csv").load("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/request/may8th-119d9fe1-4d66-454b-b4a1-75cddb8ca482_part*").withColumnRenamed("_c0","request_date").withColumnRenamed("_c1","request_uid").withColumnRenamed("_c2","country_id").withColumnRenamed("_c3","device_os_id").withColumnRenamed("_c4","os_version_id").withColumnRenamed("_c5","display_marketing_name").withColumnRenamed("_c6","market_id").withColumnRenamed("_c7","device_type_id").withColumnRenamed("_c8","constant").select("request_date", "request_uid", "display_marketing_name", "os_version_id", "market_id","device_os_id","country_id","device_type_id").filter("request_date == '2018-05-08'").filter("device_type_id=='1' or device_type_id=='3'").filter("country_id=='94'").filter("device_os_id=='3' or device_os_id=='5'")

val Dreq_Train = ad_request.union(ad_request_1).distinct()
/*
scala> Dreq_Train.printSchema
root
 |-- request_date: string (nullable = true)
 |-- request_uid: string (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- device_os_id: string (nullable = true)
 |-- country_id: string (nullable = true)
 |-- device_type_id: string (nullable = true)

 */

 // taking join between download data and rquest data on date and uid level.
val data_joined = download_event.join(Dreq_Train, Seq("request_date", "request_uid")).distinct()
/*
cala> data.printSchema
root
 |-- request_date: string (nullable = true)
 |-- request_uid: string (nullable = true)
 |-- app_id: string (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- device_os_id: string (nullable = true)
 |-- country_id: string (nullable = true)
 |-- device_type_id: string (nullable = true)
*/
// adding the appsbag to the data on user level
val data_appsbag_download = data_joined.join(DAppOwnStore,Seq("request_uid"),"left_outer")
/*
scala> data1.printSchema
root
 |-- request_uid: string (nullable = true)
 |-- appsbag: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- request_date: string (nullable = true)
 |-- app_id: string (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- device_os_id: string (nullable = true)
 |-- country_id: string (nullable = true)
 |-- device_type_id: string (nullable = true)
*/

// feed data to map owned app and target app.
val ownership = spark.read.format("parquet").load("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/feedApril01AllOwnAppsNew")
val high = 5
val lo = -3
val appPairCVUDF = ownership.withColumn("log_norm_cvu_ROUNDED", ownership("log_norm_cvu_ROUNDED").cast("int")).withColumn("log_norm_cvu_ROUNDED", when(col("log_norm_cvu_ROUNDED") >= high, high).otherwise(col("log_norm_cvu_ROUNDED"))).withColumn("log_norm_cvu_ROUNDED", when(col("log_norm_cvu_ROUNDED") <= lo, lo).otherwise(col("log_norm_cvu_ROUNDED"))).withColumn("log_norm_cvu_ROUNDED", col("log_norm_cvu_ROUNDED").-(lit(lo)))



/*
root
 |-- country_id: string (nullable = true)
 |-- device_os_id: string (nullable = true)
 |-- device_type_id: string (nullable = true)
 |-- target_app_id: string (nullable = true)
 |-- owned_app_id: string (nullable = true)
 |-- log_norm_cvu_ROUNDED: integer (nullable = true)
 */


val appPairDTCVU = appPairCVUDF.collect().map(r => ((r.getAs[String]("country_id"), r.getAs[String]("device_os_id"), r.getAs[String]("device_type_id"), r.getAs[String]("target_app_id"), r.getAs[String]("owned_app_id")), r.getAs[Int]("log_norm_cvu_ROUNDED"))).toMap



def enrichScore(appPairDTCVU: Map[(String, String, String, String, String), Int], appTarget:String, appsbag:Seq[String], country_id:String, device_os_id:String, device_type_id:String ,  size: Int = 9) = {
    var wordvec = Array.fill(size)(0)
    appsbag.map(x => appPairDTCVU.getOrElse((country_id,device_os_id,device_type_id,appTarget, x), -1))
    .filter(x => x != -1)
      .foreach(x => wordvec.update(x, 1))
    wordvec.deep.toString()
  }

def enrichScoreTargetApp(appPairDTCVU: Map[(String, String, String, String, String), Int], r: Row,  size: Int = 9) = {

    val appsbag1 = r.getAs[Seq[String]]("appsbag")
    val devtype = r.getAs[String]("device_type_id")
    val countryid = r.getAs[String]("country_id")
    val deviceos = r.getAs[String]("device_os_id")
    val targetapp = r.getAs[String]("app_id")
    val xy = enrichScore(appPairDTCVU, targetapp, appsbag1,countryid,deviceos, devtype)
    r.toSeq ++ Seq(xy) 

  }

 def genTargetAppEvalMat(data1: DataFrame, appPairDTCVU: Map[(String, String, String, String, String), Int]) = {
    val newSchema = StructType(data1.schema.fields ++  Seq(StructField("appOwnCVUEnc", StringType)))
    val rddTrainAppOwnEnc = data1.rdd.map(r => enrichScoreTargetApp(appPairDTCVU, r))
    val rddTrainAppOwnEnc3 = rddTrainAppOwnEnc.map(line => Row.fromSeq(line.toSeq))
    val dfTrainAppOwnEnc = spark.sqlContext.createDataFrame(rddTrainAppOwnEnc3, newSchema)

    dfTrainAppOwnEnc.select("request_date","request_uid","display_marketing_name","os_version_id","market_id","app_id","appOwnCVUEnc")
  }   
 val DTrainAppOwnEnc = genTargetAppEvalMat(data_appsbag_download,appPairDTCVU)

 // groupBy on date, display_marketing_name, os version, market_id, app_id and app feature to get the numbers of download.
 val downloads = DTrainAppOwnEnc.groupBy("request_date","display_marketing_name","os_version_id","market_id","app_id","appOwnCVUEnc").agg(count("*")).withColumnRenamed("count(1)","downloads")

 downloads.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/downloadownership/1-8May")


// USER count



// joining appsabg on request data on user level.
val data_appsbag_request = DAppOwnStore.join(Dreq_Train,"request_uid")

val data_1_1 = data_appsbag_request.filter("request_date == '2018-05-04'")

/*
cala> data_1.printSchema
root
 |-- appsbag: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- request_uid: string (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- device_os_id: string (nullable = true)
 |-- country_id: string (nullable = true)
 |-- device_type_id: string (nullable = true)

*/


// appstarget sequence for user count more than 20,000.
val appsTarget = download_event1.groupBy("app_id").agg(count("request_uid").alias("rqidcount")).filter("rqidcount>=20000").select("app_id").rdd.map(r => r.getString(0)).collect()
val appsTarget1 = appsTarget.toSeq


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
    dfTrainAppOwnEnc.select("request_uid", "appTarget","request_date","display_marketing_name","os_version_id","market_id","appOwnCVUEnc")
  }
val DTrainAppOwnEnc_user1 = genTargetAppEvalMat(data_1_1, appsTarget1, appPairDTCVU)

 DTrainAppOwnEnc_user1.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/3May")
val df_1 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/1May")
val df_2 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/2May")
val df_3 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/3May")
val df_4 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/4May")
val df_5 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/5May")
val df_6 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/6May")
val df_7 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/7May")
val df_8 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/8May")
val dusers = df_1.unionAll(df_2).unionAll(df_3).unionAll(df_4).unionAll(df_5).unionAll(df_6).unionAll(df_7).unionAll(df_8)
// data for 8 days formed by taking union of 8 days data. 
dusers.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/1to8May")

val dusers = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownership/1to8May")
// finding user count by taking groupBy on data on date, display_marketing_name, os version, market_id and app feature level.
val users_data = dusers.groupBy("request_date", "display_marketing_name", "os_version_id", "market_id","appOwnCVUEnc").agg(count("*")).withColumnRenamed("count(1)","user_count").filter("user_count>='" + "0" + "'")

users_data.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownershipcount/1to8May")

val users_data = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/UserAppownershipcount/1to8May")
val downloads1 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/downloadownership/1-8May")

// taking a join on downloads and user count data on basis of date, display_marketing_name,os version, market id and app feature to form data for model.
val dtrain_dtest = downloads1.join(users_data,Seq("request_date", "display_marketing_name", "os_version_id", "market_id","appOwnCVUEnc")).withColumn("CVU", $"downloads" / $"user_count")

// removing the missing value with random value.
def processMissingCategory = udf[String, String] { s => if (s == "") "NA"  else s }  
val data = train_final1.withColumn("display_marketing_name",processMissingCategory('display_marketing_name)).withColumn("market_id", processMissingCategory('market_id)).withColumn("os_version_id", processMissingCategory('os_version_id)).withColumn("app_id", processMissingCategory('app_id)).withColumn("appOwnCVUEnc", processMissingCategory('appOwnCVUEnc))

dtrain_dtest.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/Data/1to8May")


val dtrain_dtest = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/Data/1to8May")

// string indexing and encoding on data.
val indexer1 = new StringIndexer().setInputCol("display_marketing_name").setOutputCol("display_marketing_name_index").fit(dtrain_dtest).transform(dtrain_dtest)
val encoder1 = new OneHotEncoder().setInputCol("display_marketing_name_index").setOutputCol("display_marketing_name_vec")
val encoded1 = encoder1.transform(indexer1)


val indexer2 = new StringIndexer().setInputCol("os_version_id").setOutputCol("os_version_id_index").fit(encoded1).transform(encoded1)
val encoder2 = new OneHotEncoder().setInputCol("os_version_id_index").setOutputCol("os_version_id_vec")
val encoded2 = encoder2.transform(indexer2)

val indexer3 = new StringIndexer().setInputCol("market_id").setOutputCol("market_id_index").fit(encoded2).transform(encoded2)
val encoder3 = new OneHotEncoder().setInputCol("market_id_index").setOutputCol("market_id_vec")
val encoded3 = encoder3.transform(indexer3)

val indexer4 = new StringIndexer().setInputCol("app_id").setOutputCol("app_id_index").fit(encoded3).transform(encoded3)
val encoder4 = new OneHotEncoder().setInputCol("app_id_index").setOutputCol("app_id_vec")
val encoded4 = encoder4.transform(indexer4)

val indexer5 = new StringIndexer().setInputCol("appOwnCVUEnc").setOutputCol("appOwnCVUEnc_index").fit(encoded4).transform(encoded4)
val encoder5 = new OneHotEncoder().setInputCol("appOwnCVUEnc_index").setOutputCol("appOwnCVUEnc_vec")
val encoded5 = encoder5.transform(indexer5)

val train = encoded5.filter("request_date>= '2018-05-01'").filter("request_date <= '2018-05-07'" )
val test = encoded5.filter("request_date== '2018-05-08'")



val file_train = train.select($"display_marketing_name_vec",$"os_version_id_vec",$"market_id_vec",$"app_id_vec",$"appOwnCVUEnc_vec",$"downloads",$"user_count",$"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"CVU")
val file_test = test.select($"display_marketing_name_vec",$"os_version_id_vec",$"market_id_vec",$"app_id_vec",$"appOwnCVUEnc_vec",$"downloads",$"user_count",$"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"CVU")

file_train.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/traindata_appownership/1to7May")
file_test.write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/testdata_appownership/8May")

val file_train = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/traindata_appownership/1to7May")
val file_test = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/kratagya/testdata_appownership/8May")

/*
scala> file_test.printSchema
root
 |-- display_marketing_name_vec: vector (nullable = true)
 |-- os_version_id_vec: vector (nullable = true)
 |-- market_id_vec: vector (nullable = true)
 |-- app_id_vec: vector (nullable = true)
 |-- appOwnCVUEnc_vec: vector (nullable = true)
 |-- downloads: long (nullable = true)
 |-- user_count: long (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- app_id: string (nullable = true)
 |-- appOwnCVUEnc: string (nullable = true)
 |-- CVU: double (nullable = true)
*/

// making new label column and weight column for trainig data.
val train1 = file_train.withColumn("negative", $"user_count" - $"downloads")
val train_2 = train1.select(train1("negative").as("weight"),$"display_marketing_name_vec",$"os_version_id_vec",$"market_id_vec",$"app_id_vec",$"appOwnCVUEnc_vec",$"downloads",$"user_count",$"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"CVU").withColumn("label", lit("0"))
val train_3 = train1.select(train1("downloads").as("weight"),$"display_marketing_name_vec",$"os_version_id_vec",$"market_id_vec",$"app_id_vec",$"appOwnCVUEnc_vec",$"downloads",$"user_count",$"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"CVU").withColumn("label", lit("1"))
val train_final = train_2.unionAll(train_3)

/*
scala> train_final.printSchema
root
 |-- weight: long (nullable = true)
 |-- display_marketing_name_vec: vector (nullable = true)
 |-- os_version_id_vec: vector (nullable = true)
 |-- market_id_vec: vector (nullable = true)
 |-- app_id_vec: vector (nullable = true)
 |-- appOwnCVUEnc_vec: vector (nullable = true)
 |-- downloads: long (nullable = true)
 |-- user_count: long (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- app_id: string (nullable = true)
 |-- appOwnCVUEnc: string (nullable = true)
 |-- CVU: double (nullable = true)
 |-- label: string (nullable = false)
*/

// vector assembel for train and test data.
val assembler_train = new VectorAssembler().setInputCols(Array("display_marketing_name_vec", "os_version_id_vec", "market_id_vec", "app_id_vec","appOwnCVUEnc_vec")).setOutputCol("features")
val input_train = assembler_train.transform(train_final).select($"features",$"weight",$"label",$"downloads",$"user_count",$"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"CVU")
val input_train1 = input_train.withColumn( "label", input_train("label").cast(IntegerType)).withColumn( "weight", input_train("weight").cast(DoubleType))


/*
scala> input_train1.printSchema
root
 |-- features: vector (nullable = true)
 |-- weight: double (nullable = true)
 |-- label: integer (nullable = true)
 |-- downloads: long (nullable = true)
 |-- user_count: long (nullable = true)
 |-- display_marketing_name: string (nullable = true)
 |-- os_version_id: string (nullable = true)
 |-- market_id: string (nullable = true)
 |-- app_id: string (nullable = true)
 |-- appOwnCVUEnc: string (nullable = true)
 |-- CVU: double (nullable = true)
*/


val assembler_test = new VectorAssembler().setInputCols(Array("display_marketing_name_vec", "os_version_id_vec", "market_id_vec", "app_id_vec","appOwnCVUEnc_vec")).setOutputCol("features")
val input_test = assembler_train.transform(file_test).select($"features",$"downloads",$"user_count",$"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"CVU")

// training logistic regression model on train data.
val logistic1 = new LogisticRegression().setWeightCol("weight").setMaxIter(2000)
val lrModel1 = logistic1.fit(input_train1)

// prob1 is the probability column
val pred1 = lrModel1.transform(input_train1)
val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))
val prob1 = pred1.withColumn("logistic_prediction", second($"probability"))

// finding rmse by dividing total_diff_1 with total_count_1 and taking square root
val prob2 = prob1.withColumn("diff", ($"CVU" - $"logistic_prediction")*($"CVU" - $"logistic_prediction"))
val prob3 = prob2.withColumn("weight_diff", $"diff" * $"user_count").select("weight_diff","user_count")
val total_diff_1 = prob3.agg(sum("weight_diff"))
val total_count_1 = prob3.agg(sum("user_count"))

val df = prob1 .select($"display_marketing_name",$"os_version_id",$"market_id",$"app_id",$"appOwnCVUEnc",$"logistic_prediction")

// selecting required fields for bining model
val train_bining = dtrain_dtest.filter("request_date>= '2018-05-01'").filter("request_date <= '2018-05-07'" ).select("display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc","downloads","user_count","CVU")
val test_bining = dtrain_dtest.filter("request_date== '2018-05-08'").select("display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc","downloads","user_count","CVU")

// taking groupby on test and traing data to add the same rows
val train_grp = train_bining.groupBy("display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc").agg(sum("downloads"),sum("user_count")).withColumnRenamed("sum(downloads)","downloads").withColumnRenamed("sum(user_count)","user_count").withColumn("CVU", $"downloads" / $"user_count")
val test_grp = test_bining.groupBy("display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc").agg(sum("downloads"),sum("user_count")).withColumnRenamed("sum(downloads)","downloads").withColumnRenamed("sum(user_count)","user_count").withColumn("CVU", $"downloads" / $"user_count")

val train_1 = train_grp.select( "display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc","CVU").withColumnRenamed("CVU","bining_prediction")
val test_2 = test_grp.select( "display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc","user_count","CVU")
val train_test_join = train_1.join(test_2,Seq( "display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc"))

val df_final = train_test_join.join(df,Seq("display_marketing_name", "os_version_id", "market_id","app_id","appOwnCVUEnc")).distinct()

val train_test_join_11 = df_final.withColumn("cvu_diff", ($"CVU" - $"bining_prediction")*($"CVU" - $"bining_prediction")).withColumn("weight_CVU",$"user_count" * $"cvu_diff")
// finding rmse by dividing both the fields and taking square root of it.
val train_test_join_21 = train_test_join_11.agg(sum("weight_CVU"),sum("user_count"))

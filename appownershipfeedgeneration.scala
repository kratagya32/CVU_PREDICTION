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

  val DAppOwn = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/AppOwnershipApril2018").filter("country_id=='94'").filter("device_os_id=='3' or device_os_id=='5'").filter("device_type_id=='1' or device_type_id=='3'")
  val DTarget = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/TargetApp23-30April2018").filter("country_id=='94'").filter("device_os_id=='3' or device_os_id=='5'").filter("device_type_id=='1' or device_type_id=='3'")

  def genCounDevtype(D: DataFrame) = {
    D.groupBy("country_id", "device_os_id", "device_type_id").count().collect()
  }

  def nCVUTableRON(dfA1: DataFrame, dfA0: DataFrame, RONcount: Long, r: Row) = {

    val dfA0A1 = dfA0.join(dfA1, Seq("request_uid"), "inner").filter("A0!=A1")

    val aggA0 = dfA0.groupBy("A0").agg(count("request_uid").as("A0users"))
    val aggA1 = dfA1.groupBy("A1").agg(count("request_uid").as("A1users"))

    val aggA0filt = aggA0.filter("A0users>=10000").select("A0").distinct()
    val aggA1filt = aggA1.filter("A1users>=10000").select("A1").distinct()
    val eligibleTuples = aggA1filt.join(aggA0filt).filter("A0!=A1").distinct()
    print(aggA0filt.count())
    print(aggA1filt.count())
    print(eligibleTuples.count())

    val aggA0A1 = dfA0A1.groupBy("A1", "A0").agg(count("request_uid").as("A1A0users"))

    val dfJoined = eligibleTuples.join(aggA1, Seq("A1"), "left_outer")
      .join(aggA0, Seq("A0"), "left_outer")
      .join(aggA0A1, Seq("A1", "A0"), "left_outer")
      .withColumn("RONusers", lit(RONcount))
      .withColumn("country_id", lit(r.getAs[String]("country_id")))
      .withColumn("device_os_id", lit(r.getAs[String]("device_os_id")))
      .withColumn("device_type_id", lit(r.getAs[String]("device_type_id")))
      .na.fill("NA").na.fill(0)

    val dfJoinedA0agg = dfJoined.groupBy("A0").agg(sum("A1A0users").as("A1A0usersAggTarget"), sum("A1users").as("A1usersAggTarget"))
    val dfJoinedNormalised = dfJoined.join(dfJoinedA0agg, Seq("A0"), "left_outer")

    dfJoinedNormalised
  }

  def genRONTuple(r: Row, DAppOwn: DataFrame, DTarget: DataFrame) = {
    println(r)
    val dfA1 = DAppOwn.filter("country_id=='" + r.getAs[String]("country_id") + "'").filter("device_os_id=='" + r.getAs[String]("device_os_id") + "'").filter("device_type_id=='" + r.getAs[String]("device_type_id") + "'").withColumnRenamed("app_id", "A1").select("A1", "request_uid").distinct()

    val dfA0 = DTarget.filter("country_id=='" + r.getAs[String]("country_id") + "'").filter("device_os_id=='" + r.getAs[String]("device_os_id") + "'").filter("device_type_id=='" + r.getAs[String]("device_type_id") + "'").withColumnRenamed("app_id", "A0").select("A0", "request_uid").distinct()

    val RONCount = dfA1.select("request_uid").distinct().count()

    (dfA1, dfA0, RONCount, r)
  }

  //A1 ownership A0 target
  def nCVUTableFinal(DAppOwn: DataFrame, DTarget: DataFrame) = {
    val RONcuts = genCounDevtype(DAppOwn)

    val nCVUTables = RONcuts.map(r => genRONTuple(r, DAppOwn, DTarget)).map(x => nCVUTableRON(x._1, x._2, x._3, x._4)).reduce((x, y) => x.union(y))

    nCVUTables.withColumn("nCVU", nCVU(nCVUTables("A1A0usersAggTarget"), nCVUTables("A1users"), nCVUTables("A1A0users"), nCVUTables("A1usersAggTarget"))).withColumn("log_norm_cvu_ROUNDED", floor(log2("nCVU"))).withColumnRenamed("A0", "app_a0").withColumnRenamed("A1", "app_a1")
  }

  val nCVU = udf((A1A0usersAggTarget: Long, A1users: Long, A1A0users: Long, A1usersAggTarget: Long) => { A1A0users * A1usersAggTarget * 1.0 / (A1A0usersAggTarget * A1users) })
  //nCVUTableFinal(DAppOwn, DTarget).select("country_id", "device_os_id", "device_type_id", "app_a0", "app_a1", "log_norm_cvu_ROUNDED").write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/feedJune01AllOwnAppsNew")
  val finalnCVUTab = nCVUTableFinal(DAppOwn, DTarget)
  finalnCVUTab.cache()
  finalnCVUTab.show()

  finalnCVUTab.na.fill(-999).write.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/feedAprilAllOwnAppsNewcode1")

  val df10 = spark.read.parquet("/secure/projects/ds-exp/users/varun.modi/ocvm/data/cvupred/feedAprilAllOwnAppsNewcode1")
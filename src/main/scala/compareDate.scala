import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
object compareDate {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.{SparkSession, DataFrame}
    val spark = SparkSession.builder()
      .config("spark.hadoop.fs.s3a.access.key","WWCTQNZDHWMVZMJY9QJN")
      .config("spark.default.parallelism", "16")
      .config("s3.secret-key","YoVuuQ9Qx7KYuODRyhWFqFxvEKKPQLjIaAm3aTam")
      .config("s3.endpoint","http://obs.cn-southwest-2.myhuaweicloud.com")
      .getOrCreate()

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val lakeosulPath = "s3://dmetasoul-bucket/dudongfeng/lakesoul/kafka/data/test/table1"
    //val lakeosulPath = "/tmp/lakesoul/kafka/nanhang717/test717"
    val lakesoulTable: Dataset[Row] = LakeSoulTable.forPath(lakeosulPath).toDF

    val data = new TransforData
    val toTransPath = "s3://dmetasoul-bucket/yunhe/k8s/data/"
    //val toTransPath = "/tmp/kafka/912/"
    val frame1 = data.transJson(toTransPath,spark)
    frame1.printSchema()
    val resPath = "s3://dmetasoul-bucket/yunhe/k8s/failData/"
    data.compareDataFrame(frame1, lakesoulTable, spark).write.csv(resPath)

    spark.stop()
  }
}
class TransforData {

  def transJson(filePath:String,spark:SparkSession):DataFrame ={
//    val file1 = filePath+"1"
//    val file2 = filePath+"2"
//    val file3 = filePath+"3"
//    val file4 = filePath+"4"
//    val file5 = filePath+"5"
//    val file6 = filePath+"6"
//    val file7 = filePath+"7"
//    val file8 = filePath+"8"
//    val file9 = filePath+"9"
//    val file10 = filePath+"10"
//    val file11 = filePath+"11"
//    val file12 = filePath+"12"
//    val file13 = filePath+"13"
//    val file14 = filePath+"14"
//    val file15 = filePath+"15"
//    val file16 = filePath+"16"
//    val file17 = filePath+"17"
//    val file18 = filePath+"18"
//    val file19 = filePath+"19"
//    val file20 = filePath+"20"
//    val file21 = filePath+"21"
//    val file22 = filePath+"22"
//    val file23 = filePath+"23"
//    val file24 = filePath+"24"
//    val file25 = filePath+"25"
//    val file26 = filePath+"26"
//    val file27 = filePath+"27"
//    val file28 = filePath+"28"
//    val file29 = filePath+"29"
//    val file30 = filePath+"30"
//    val file31 = filePath+"31"
//    val file32 = filePath+"32"
    val jsonData = spark.read.json(filePath)
    jsonData.createTempView("tb")
    val insertFrame = spark.sql(
      """
        |select *
        |from tb
        |where op = 'I'
        |
        |""".stripMargin)

    val updateFrame = spark.sql(
      """
        |select *
        |from tb
        |where op = 'U'
        |order by after.id
        |""".stripMargin)

    val updateCount = updateFrame.count()

    val deleteFrame = spark.sql(
      """
        |select *
        |from tb
        |where op = 'D'
        |""".stripMargin)

    val deleteCount = deleteFrame.count()

    val insertFrame1 = insertFrame.select("after.*","op")

    //insertFrame1.createTempView("tt")

//    spark.sql(
//      """
//        |select * from tt
//        |order by id
//        |""".stripMargin)

    def getFinalFrame(updateCount : Long,deleteCount: Long ,insertDF: DataFrame) :DataFrame = {


      insertDF.createTempView("insert_tb")
      if (updateCount==0 && deleteCount==0){
        insertDF
      }
      else if (updateCount!=0&&deleteCount==0){
        val toUpdateFrame = updateFrame.select("after.*","op")
        toUpdateFrame.createTempView("update_tb")
        val updatedFrame = spark.sql(
          """
            |select
            |   in.id,
            |   COALESCE(up.col1,in.col1) col1,
            |   COALESCE(up.col2,in.col2) col2,
            |   COALESCE(up.col3,in.col3) col3,
            |   COALESCE(up.col4,in.col4) col4,
            |   COALESCE(up.col5,in.col5) col5,
            |   COALESCE(up.col6,in.col6) col6,
            |   COALESCE(up.col7,in.col7) col7,
            |   COALESCE(up.col8,in.col8) col8,
            |   COALESCE(up.col9,in.col9) col9,
            |   COALESCE(up.col10,in.col10) col10,
            |   COALESCE(up.col11,in.col11) col11,
            |   COALESCE(up.col12,in.col12) col12,
            |   COALESCE(up.col13,in.col13) col13,
            |   COALESCE(up.col14,in.col14) col14,
            |   COALESCE(up.col15,in.col15) col15,
            |   COALESCE(up.col16,in.col16) col16,
            |   COALESCE(up.col17,in.col17) col17,
            |   COALESCE(up.col18,in.col18) col18,
            |   COALESCE(up.col19,in.col19) col19,
            |   COALESCE(up.col20,in.col20) col20,
            |   COALESCE(up.col21,in.col21) col21
            |
            |from
            |insert_tb in
            |left join
            |update_tb up
            |on in.id=up.id
            |order by id
            |""".stripMargin)
        updatedFrame
      }
      else if (updateCount==0&&deleteCount!=0){
        val toDeleteFrame = deleteFrame.select("before.*","op")
        toDeleteFrame.createTempView("delete_tb")
        val deletedFrame = spark.sql(
          """
            |select
            |   *
            |from
            |insert_tb
            |where id not in (select id from delete_tb)
            |order by id
            |""".stripMargin)
        deletedFrame
      }
      else {
        val toDeleteFrame = deleteFrame.select("before.*","op")
        val toUpdateFrame = updateFrame.select("after.*","op")
        toUpdateFrame.createTempView("update_tb")
        toDeleteFrame.createTempView("delete_tb")
        val updatedFrame = spark.sql(
          """
            |select
            |   in.id,
            |   COALESCE(up.col1,in.col1) col1,
            |   COALESCE(up.col2,in.col2) col2,
            |   COALESCE(up.col3,in.col3) col3,
            |   COALESCE(up.col4,in.col4) col4,
            |   COALESCE(up.col5,in.col5) col5,
            |   COALESCE(up.col6,in.col6) col6,
            |   COALESCE(up.col7,in.col7) col7,
            |   COALESCE(up.col8,in.col8) col8,
            |   COALESCE(up.col9,in.col9) col9,
            |   COALESCE(up.col10,in.col10) col10,
            |   COALESCE(up.col11,in.col11) col11,
            |   COALESCE(up.col12,in.col12) col12,
            |   COALESCE(up.col13,in.col13) col13,
            |   COALESCE(up.col14,in.col14) col14,
            |   COALESCE(up.col15,in.col15) col15,
            |   COALESCE(up.col16,in.col16) col16,
            |   COALESCE(up.col17,in.col17) col17,
            |   COALESCE(up.col18,in.col18) col18,
            |   COALESCE(up.col19,in.col19) col19,
            |   COALESCE(up.col20,in.col20) col20,
            |   COALESCE(up.col21,in.col21) col21
            |
            |from
            |insert_tb in
            |left join
            |update_tb up
            |on in.id=up.id
            |order by id
            |""".stripMargin)
        updatedFrame
      }
    }

//    val toUpdateFrame = updateFrame.select("after.*","op")
//    val toDeleteFrame = deleteFrame.select("before.*","op")
//    insertFrame1.createTempView("insert_tb")
//    toUpdateFrame.createTempView("update_tb")
//    toDeleteFrame.createTempView("delete_tb")
//
//    val updatedFrame = spark.sql(
//      """
//        |select
//        |   in.id,
//        |   COALESCE(up.col1,in.col1) col1,
//        |   COALESCE(up.col2,in.col2) col2,
//        |   COALESCE(up.col3,in.col3) col3,
//        |   COALESCE(up.col4,in.col4) col4,
//        |   COALESCE(up.col5,in.col5) col5,
//        |   COALESCE(up.col6,in.col6) col6,
//        |   COALESCE(up.col7,in.col7) col7,
//        |   COALESCE(up.col8,in.col8) col8,
//        |   COALESCE(up.col9,in.col9) col9,
//        |   COALESCE(up.col10,in.col10) col10,
//        |   COALESCE(up.col11,in.col11) col11,
//        |   COALESCE(up.col12,in.col12) col12,
//        |   COALESCE(up.col13,in.col13) col13,
//        |   COALESCE(up.col14,in.col14) col14,
//        |   COALESCE(up.col15,in.col15) col15,
//        |   COALESCE(up.col16,in.col16) col16,
//        |   COALESCE(up.col17,in.col17) col17,
//        |   COALESCE(up.col18,in.col18) col18,
//        |   COALESCE(up.col19,in.col19) col19,
//        |   COALESCE(up.col20,in.col20) col20,
//        |   COALESCE(up.col21,in.col21) col21,
//        |   case
//        |     when COALESCE(up.op,in.op)='I' then 'insert'
//        |   else 'update' end as rowKinds
//        |
//        |from
//        |insert_tb in
//        |left join
//        |update_tb up
//        |on in.id=up.id
//        |order by id
//        |""".stripMargin)
    val finalFrame = getFinalFrame(updateCount,deleteCount,insertFrame1)
    finalFrame.createTempView("updated_tb")

//    val finalFrame = spark.sql(
//      """
//        |select
//        |   *
//        |from
//        |updated_tb
//        |where id not in (select id from delete_tb)
//        |order by id
//        |""".stripMargin)

    val dataFrame = finalFrame
      .withColumn("col2",finalFrame.col("col2").cast("long"))
      .withColumn("col3",finalFrame.col("col3").cast("integer"))
      .withColumn("col4",finalFrame.col("col4").cast("integer"))
      .withColumn("col9",finalFrame.col("col9").cast("float"))
      .withColumn("col11",finalFrame.col("col11").cast("timestamp"))
      .withColumn("col5",finalFrame.col("col5").cast("integer"))
      .withColumn("col6",finalFrame.col("col6").cast("integer"))
      .withColumn("col7",finalFrame.col("col7").cast("integer"))
      .withColumn("id",finalFrame.col("id").cast("integer"))
      .withColumn("col13",finalFrame.col("col13").cast("decimal(10,3)"))
      .withColumn("col10",finalFrame.col("col10").cast("date"))
    dataFrame

  }

  def compareDataFrame(frame1:DataFrame,frame2:DataFrame,spark:SparkSession):DataFrame={
    frame1.createTempView("left_tb")
    frame2.createTempView("right_tb")
    spark.sql(
      """
        |select *
        |from
        |   left_tb
        |join right_tb
        |on left_tb.id=right_tb.id
        |where
        |   left_tb.col1!=right_tb.col1
        |or left_tb.col2!=right_tb.col2
        |or left_tb.col3!=right_tb.col3
        |or left_tb.col4!=right_tb.col4
        |or left_tb.col5!=right_tb.col5
        |or left_tb.col6!=right_tb.col6
        |or left_tb.col7!=right_tb.col7
        |or left_tb.col8!=right_tb.col8
        |or left_tb.col9!=right_tb.col9
        |or left_tb.col10!=right_tb.col10
        |or left_tb.col11!=right_tb.col11
        |or left_tb.col12!=right_tb.col12
        |or left_tb.col13!=right_tb.col13
        |or left_tb.col14!=right_tb.col14
        |or left_tb.col15!=right_tb.col15
        |or left_tb.col16!=right_tb.col16
        |or left_tb.col17!=right_tb.col17
        |or left_tb.col18!=right_tb.col18
        |or left_tb.col19!=right_tb.col19
        |or left_tb.col20!=right_tb.col20
        |or left_tb.col21!=right_tb.col21
        |""".stripMargin)
  }
}
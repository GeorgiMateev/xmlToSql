/**
  * Created by mateev on 11.12.2016 г..
  */
import java.io.InputStream

import com.databricks.spark.xml.XmlReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}

object XmlToSql {
  def main(args: Array[String]): Unit = {
	  Class.forName("com.mysql.jdbc.Driver").newInstance

	  val localxml = "spark-warehouse/dev_set_android.txt"
	  val originalQuestionTag = "OrgQuestion"

	  println("-------------Attach debugger now!--------------")
	  Thread.sleep(10000) //debug

	  val warehouseLocation = "file:" + System.getProperty("user.dir") + "spark-warehouse";
	  System.out.println("warehouseLocation" + warehouseLocation);

	  val toNormId = udf((id :String, index :Int) => if(Option(id).isDefined) id.split("_")(index) else "")

	  val sqlContext = SparkSession.builder()
	  	  .appName("XML to SQL")
		  .enableHiveSupport().config("set spark.sql.crossJoin.enabled", "true")
		  .getOrCreate().sqlContext

	  val df = (new XmlReader()).withRowTag(originalQuestionTag).xmlFile(sqlContext, localxml)

	  val originalQuestionsDf = df.drop("Thread")
		  .distinct()
		  .withColumn("id", monotonically_increasing_id())

	  println("Original questions schema:")
	  originalQuestionsDf.printSchema()
	  originalQuestionsDf.show()

	  var relatedQuestionsDf =  df.select("Thread.RelQuestion.*")
		  .filter((r: Row) => Option(r.getAs("_RELQ_ID")).isDefined)
		  .distinct()
		  .withColumn("id", monotonically_increasing_id())

		  relatedQuestionsDf = relatedQuestionsDf
			  .withColumn("RELQ_NORM_ID", toNormId(relatedQuestionsDf("_RELQ_ID"), lit(1)))

	  println("Related questions schema:")
	  relatedQuestionsDf.printSchema()
	  relatedQuestionsDf.show()

	  var relatedAnswersDf = df
		  .select(
			  explode(
				  df.col("Thread.RelAnswer"))
				  .as("ExplodedRelA"))
		  .select("ExplodedRelA.*")
		  .drop("RelAComment")
		  .filter((r: Row) => Option(r.getAs("_RELA_ID")).isDefined)
		  .withColumn("id", monotonically_increasing_id())

	  relatedAnswersDf = relatedAnswersDf
	  	    .withColumn("RELA_NORM_ID", toNormId(
				relatedAnswersDf("_RELA_ID"), lit(2)))

	  println("Related question answers schema:")
	  relatedAnswersDf.printSchema()
	  relatedAnswersDf.show()

	  val relatedAnswerCommentsArr = df
		  .select(
			  explode(
				  df.col("Thread.RelAnswer.RelAComment"))
				  .as("ExplodedRelAC"))
		  .withColumn("id", monotonically_increasing_id())

	  var relatedAnswerCommentsDf = relatedAnswerCommentsArr
		  .select(
			  explode(
				  relatedAnswerCommentsArr.col("ExplodedRelAC"))
				  .as("ExplodedRelACc"))
		  .select("ExplodedRelACc.*")
		  .filter((r: Row) => Option(r.getAs("_RELAC_ID")).isDefined)
		  .withColumn("id", monotonically_increasing_id())

	  relatedAnswerCommentsDf = relatedAnswerCommentsDf
	      .withColumn("RELAC_NORM_ID", toNormId(relatedAnswerCommentsDf("_RELAC_ID"), lit(3)))

	  println("Related answer comments schema:")
	  relatedAnswerCommentsDf.printSchema()
	  relatedAnswerCommentsDf.show()

	  var relatedQuestionCommentsDf = df
		  .select(
			  explode(
				  df.col("Thread.RelComment"))
				  .as("ExplodedRelC"))
		  .select("ExplodedRelC.*")
		  .filter((r: Row) => Option(r.getAs("_RELC_ID")).isDefined)
		  .withColumn("id", monotonically_increasing_id())

	  relatedQuestionCommentsDf = relatedQuestionCommentsDf
	      .withColumn("RELC_NORM_ID", toNormId(relatedQuestionCommentsDf("_RELC_ID"), lit(2)))

	  println("Related question comments schema:")
	  relatedQuestionCommentsDf.printSchema()
	  relatedQuestionCommentsDf.show()

	  var threads = df.select("Thread._THREAD_SEQUENCE")
		  .filter((r: Row) => Option(r.getAs("_THREAD_SEQUENCE")).isDefined)
		  .withColumn("id", monotonically_increasing_id())

	  threads = threads
	      .withColumn("ORG_ID", toNormId(threads("_THREAD_SEQUENCE"), lit(0)))
		  .withColumn("REL_ID", toNormId(threads("_THREAD_SEQUENCE"), lit(1)))
	      .drop("_THREAD_SEQUENCE")
	  println("Threads ids")
	  threads.printSchema()
	  threads.show()

	  val stream : InputStream = getClass.getResourceAsStream("credentials/db.txt")
	  val source = scala.io.Source.fromInputStream(stream)
	  val lines = try source.getLines().toArray finally source.close()

	  val host = lines(0)
	  val user = lines(1)
	  val password = lines(2)
	  val dbName = lines(3)

	  val prop = new java.util.Properties()
	  prop.put("user", user)
	  prop.put("password", password)
	  prop.put("driver", "com.mysql.jdbc.Driver")
	  val url="jdbc:mysql://" + host + "/" + dbName

	  originalQuestionsDf.write.mode(SaveMode.Overwrite).jdbc(url, "OrgQuestions", prop)
	  relatedQuestionsDf.write.mode(SaveMode.Overwrite).jdbc(url, "RelQuestions", prop)
	  relatedAnswersDf.write.mode(SaveMode.Overwrite).jdbc(url, "RelAnswers", prop)
	  relatedAnswerCommentsDf.write.mode(SaveMode.Overwrite).jdbc(url, "RelAnswerComments", prop)
	  relatedQuestionCommentsDf.write.mode(SaveMode.Overwrite).jdbc(url, "RelQuestionComments", prop)
	  threads.write.mode(SaveMode.Overwrite).jdbc(url, "Threads", prop)
  }
}

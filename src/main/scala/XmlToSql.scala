/**
  * Created by mateev on 11.12.2016 г..
  */
import java.io.InputStream
import java.util.Properties

import com.databricks.spark.xml.XmlReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object XmlToSql {
  def main(args: Array[String]): Unit = {
	  Class.forName("com.mysql.jdbc.Driver").newInstance

	  val localxmlUsers = "spark-warehouse/stackexchange_android_users.xml"

	  val androidDev = "s3://semeval-stackexchange/SemEval_DevData/dev_set_android.txt"
	  val englishDev = "s3://semeval-stackexchange/SemEval_DevData/dev_set_english.txt"
	  val gamingDev = "s3://semeval-stackexchange/SemEval_DevData/dev_set_gaming.txt"
	  val wordpressDev = "s3://semeval-stackexchange/SemEval_DevData/dev_set_wordpress.txt"

	  val originalQuestionTag = "OrgQuestion"

	  val usersTag = "xml"

	  println("-------------Attach debugger now!--------------")
	  Thread.sleep(10000) //debug

	  val androidTrain = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_android_train_v1_2/stackexchange_android_train.xml"

	  val englishTrain = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_english_train_v1_2/stackexchange_english_train.xml"

	  val gamingTrain = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_gaming_train_v1_2/stackexchange_gaming_train.xml"

	  val wordpressTrain = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_wordpress_train_v1_2/stackexchange_wordpress_train.xml"

	  val androidUsers = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_android_train_v1_2/stackexchange_android_users.xml"

	  val englishUsers = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_english_train_v1_2/stackexchange_english_users.xml"

	  val gamingUsers = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_gaming_train_v1_2/stackexchange_gaming_users.xml"

	  val wordpressUsers = "s3://semeval-stackexchange/stackexchange_train_v1_2/stackexchange_wordpress_train_v1_2/stackexchange_wordpress_users.xml"

	  val sqlContext = SparkSession.builder()
		  .appName("XML to SQL")
		  .enableHiveSupport().config("set spark.sql.crossJoin.enabled", "true")
		  .getOrCreate().sqlContext

//	  processXmlFile(sqlContext, originalQuestionTag, androidTrain, "android")
//	  processXmlFile(sqlContext, originalQuestionTag, englishTrain, "english")
//	  processXmlFile(sqlContext, originalQuestionTag, gamingTrain, "gaming")
//	  processXmlFile(sqlContext, originalQuestionTag, wordpressTrain, "wordpress")

	  //processXmlFile(sqlContext, originalQuestionTag, androidDev, "android")
//	  processXmlFile(sqlContext, originalQuestionTag, englishDev, "english")
//	  processXmlFile(sqlContext, originalQuestionTag, gamingDev, "gaming")
//	  processXmlFile(sqlContext, originalQuestionTag, wordpressDev, "wordpress")

	  processUsersXmlFile(sqlContext, usersTag, androidUsers, "android")
	  processUsersXmlFile(sqlContext, usersTag, englishUsers, "english")
	  processUsersXmlFile(sqlContext, usersTag, gamingUsers, "gaming")
	  processUsersXmlFile(sqlContext, usersTag, wordpressUsers, "wordpress")

	  //processUsersXmlFile(sqlContext, "xml", localxmlUsers, "android")
  }
	val toNormId = udf((id :String, index :Int) => if(Option(id).isDefined) id.split("_")(index) else "")

	def processXmlFile(sqlContext: SQLContext, originalQuestionTag: String, localxml: String, category: String) = {

		val df = (new XmlReader()).withRowTag(originalQuestionTag).xmlFile(sqlContext, localxml)

		val originalQuestionsDf = df.drop("Thread")
			.distinct()
			.withColumn("id", monotonically_increasing_id())
		    .withColumn("category", lit(category))

		println("Original questions schema:")
		originalQuestionsDf.printSchema()
		originalQuestionsDf.show()

		var relatedQuestionsDf = df.select("Thread.RelQuestion.*")
			.filter((r: Row) => Option(r.getAs("_RELQ_ID")).isDefined)
			.distinct()
			.withColumn("id", monotonically_increasing_id())
			.withColumn("category", lit(category))

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
			.withColumn("category", lit(category))

		relatedAnswersDf = relatedAnswersDf
			.withColumn("RELA_NORM_ID", toNormId(
				relatedAnswersDf("_RELA_ID"), lit(2)))
			.withColumn("RELQ_NORM_ID", toNormId(
				relatedAnswersDf("_RELA_ID"), lit(1)))

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
			.withColumn("category", lit(category))

		relatedAnswerCommentsDf = relatedAnswerCommentsDf
			.withColumn("RELAC_NORM_ID", toNormId(relatedAnswerCommentsDf("_RELAC_ID"), lit(3)))
			.withColumn("RELA_NORM_ID", toNormId(relatedAnswerCommentsDf("_RELAC_ID"), lit(2)))

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
			.withColumn("category", lit(category))

		relatedQuestionCommentsDf = relatedQuestionCommentsDf
			.withColumn("RELC_NORM_ID", toNormId(relatedQuestionCommentsDf("_RELC_ID"), lit(2)))
			.withColumn("RELQ_NORM_ID", toNormId(relatedQuestionCommentsDf("_RELC_ID"), lit(1)))

		println("Related question comments schema:")
		relatedQuestionCommentsDf.printSchema()
		relatedQuestionCommentsDf.show()

		var threads = df.select("Thread._THREAD_SEQUENCE")
			.filter((r: Row) => Option(r.getAs("_THREAD_SEQUENCE")).isDefined)
			.withColumn("id", monotonically_increasing_id())

		threads = threads
			.withColumn("ORG_ID", toNormId(threads("_THREAD_SEQUENCE"), lit(0)))
			.withColumn("REL_ID", toNormId(threads("_THREAD_SEQUENCE"), lit(1)))
			.withColumn("category", lit(category))
			.drop("_THREAD_SEQUENCE")
		println("Threads ids")
		threads.printSchema()
		threads.show()

		val (prop: Properties, url: String) = getDBCredentials

		originalQuestionsDf.write.mode(SaveMode.Append).jdbc(url, "OrgQuestions", prop)
		relatedQuestionsDf.write.mode(SaveMode.Append).jdbc(url, "RelQuestions", prop)
		relatedAnswersDf.write.mode(SaveMode.Append).jdbc(url, "RelAnswers", prop)
		relatedAnswerCommentsDf.write.mode(SaveMode.Append).jdbc(url, "RelAnswerComments", prop)
		relatedQuestionCommentsDf.write.mode(SaveMode.Append).jdbc(url, "RelQuestionComments", prop)
		threads.write.mode(SaveMode.Append).jdbc(url, "Threads", prop)
	}

	def processUsersXmlFile(sqlContext: SQLContext, tag: String, filePath: String, category: String) = {
		val df = (new XmlReader()).withRowTag(tag).xmlFile(sqlContext, filePath)
			.selectExpr("explode(User) as e").select("e.*")
			.distinct()
			.withColumn("id", monotonically_increasing_id())
			.withColumn("category", lit(category))

		df.printSchema()
		df.show()

		val (prop: Properties, url: String) = getDBCredentials

		df.write.mode(SaveMode.Append).jdbc(url, "Users", prop)
	}

	private def getDBCredentials = {
		val stream: InputStream = getClass.getResourceAsStream("credentials/db.txt")
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
		val url = "jdbc:mysql://" + host + "/" + dbName

		(prop, url)
	}
}

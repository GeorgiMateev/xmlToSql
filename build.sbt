lazy val root = (project in file(".")).
	settings(
		name := "XmlToSql",
		scalaVersion := "2.11.8",
		libraryDependencies ++= Seq(
			"com.databricks" % "spark-xml_2.11" % "0.4.1",
			"mysql" % "mysql-connector-java" % "5.1.12",
			"org.apache.spark" % "spark-core_2.11" % "2.0.2" % "provided",
			"org.apache.spark" % "spark-sql_2.11" % "2.0.2" % "provided"
		),
		mainClass in assembly := Some("XmlToSql")
	)
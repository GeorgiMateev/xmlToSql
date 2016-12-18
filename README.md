# xmlToSql
Spark app for converting large deeply nested xml files to normalized sql tables

## Run locally

```
spark-submit --class XmlToSql --master local[4] --driver-memory 4G --executor-memory 4G C:\Su\SemEval\SemEvalXmlToSql\target\scala-2.11\XmlToSql-assembly-0.1-SNAPSHOT.jar
```

## Debug locally
- run: 
```
export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
```
- Add simple Remote debug configuration in IntelliJ listening on port 5005
- Start the spark-submit script
- Attach debuger now! will appear, you have 10 sec. to attach debugger - run the Remote debug
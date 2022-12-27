name := "Wikimedia_Streaming_Project"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.2"
val kafkaVersion = "3.3.1"
val okHttpVersion = "4.10.0"
val okHttpEventSource = "2.5.0"


resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  //spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  //spark streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

  //low level integration
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Test,
 // "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion,

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  //ok-http  and OkHttp EventSource
  "com.squareup.okhttp3" % "okhttp" % okHttpVersion,
  "com.launchdarkly" % "okhttp-eventsource" % okHttpEventSource

)
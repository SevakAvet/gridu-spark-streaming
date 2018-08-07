name := "spark"

version := "0.1"

scalaVersion := "2.11.0"

//val sparkVersion = "2.1.0"
val sparkVersion = "2.3.1"
val kafkaVersion = "2.1.0"
val cassandraVersion = "2.3.1"

resolvers += "Bintray Maven Repository" at "https://dl.bintray.com/spark-packages/maven"
lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
lazy val kafkaClients = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion excludeAll excludeJpountz // add more exclusions here

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  kafkaClients,
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraVersion,
  "net.liftweb" %% "lift-json" % "2.6.2",
  "net.debasishg" %% "redisclient" % "3.7",
  "com.typesafe" % "config" % "1.2.1",
  "RedisLabs" % "spark-redis" % "0.3.2",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion
)
import com.spotright.sbt._
//import com.atlassian.labs.gitstamp.GitStampPlugin._

// SpotRight deps
val commonSiVer     = "4.13.0"
val xcassVer        = "1.4.1"
val modelsVer       = "4.1.1"
val searchVer       = "3.1.0"

//3rd party versions
val awsVer            = "1.7.4"
val awsHadoopVer      = "2.7.2"

lazy val trickshot =
  (project in file(".")).
    settings(spotright.settingsLog4J: _*).
    settings(
      name := "trickshot",
      organization := "com.spotright",
      version := "1.0",
      scalaVersion := "2.11.8",

      libraryDependencies ++= Seq(
        "com.spotright.xcass" %% "xcass-spark" % xcassVer
          exclude("com.github.rholder", "snowball-stemmer")
          excludeAll(ExclusionRule(organization = "org.slf4j"))
          excludeAll(ExclusionRule(organization = "ch.qos.logback"))
          excludeAll(ExclusionRule("io.netty")),
        "com.spotright.common" %% "common-config" % commonSiVer,
        "com.spotright.common" %% "common-core" % commonSiVer,
        "com.spotright.models" %% "models-asty" % modelsVer,
        "com.spotright.search" %% "search-client" % searchVer
          exclude("javax.xml.stream", "stax-api")
          exclude("org.apache.zookeeper", "zookeeper")
          exclude("com.typesafe.akka", "akka-actor_2.11"),
        tgav("spotright-button"),

        // 3rd Party

        "org.apache.hadoop" % "hadoop-aws" % awsHadoopVer
          exclude("stax", "stax-api")
          exclude("javax.xml.stream", "stax-api")
          exclude("javax.servlet", "servlet-api")
          exclude("org.objectweb", "asm")
          exclude("asm", "asm")
          //          excludeAll(ExclusionRule(organization = "commons-collections"))
          excludeAll(ExclusionRule(organization = "commons-beanutils")),
        "org.scalaj" %% "scalaj-http" % "2.3.0",
        gav("opencsv"),
        pgav("spark-streaming"),
        pgav("spark-sql")
          exclude("com.google.guava","guava")
          exclude("com.esotericsoftware.minlog","minlog")
          exclude("com.esotericsoftware.kryo","kryo")
          exclude("org.apache.spark","spark-network-common_2.11"),
        "org.apache.spark" %% "spark-mllib" % v("spark-core") % "provided"
      ).map( _.exclude("commons-logging", "commons-logging"))
    )
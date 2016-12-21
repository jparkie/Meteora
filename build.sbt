import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

/**
  * Organization:
  */
organization     := "com.github.jparkie"
organizationName := "jparkie"

/**
  * Library Meta:
  */
name     := "Meteora"
licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

/**
  * Scala:
  */
scalaVersion       := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

/**
  * Library Dependencies:
  */
// Versions:
val AkkaVersion      = "2.4.10"
val ChillAkkaVersion = "0.8.1"
val GuavaVersion     = "19.0"
val RocksDBVersion   = "4.11.2"
val ScalaTestVersion = "3.0.0"

// Dependencies:
val akkaActor         = "com.typesafe.akka" %% "akka-actor"                        % AkkaVersion
val akkaCluster       = "com.typesafe.akka" %% "akka-cluster"                      % AkkaVersion
val akkaClusterTools  = "com.typesafe.akka" %% "akka-cluster-tools"                % AkkaVersion
val akkaHttp          = "com.typesafe.akka" %% "akka-http-experimental"            % AkkaVersion
val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json-experimental" % AkkaVersion
val akkaRemote        = "com.typesafe.akka" %% "akka-remote"                       % AkkaVersion
val akkaStream        = "com.typesafe.akka" %% "akka-stream"                       % AkkaVersion
val akkaTestKit       = "com.typesafe.akka" %% "akka-testkit"                      % AkkaVersion % "test"
val chillAkka         = "com.twitter"       %% "chill-akka"                        % ChillAkkaVersion
val guava             = "com.google.guava"  %  "guava"                             % GuavaVersion
val rocksDB           = "org.rocksdb"       %  "rocksdbjni"                        % RocksDBVersion
val scalaTest         = "org.scalatest"     %% "scalatest"                         % ScalaTestVersion % "test"

libraryDependencies ++= Seq(
  akkaActor,
  akkaCluster,
  akkaClusterTools,
  akkaHttp,
  akkaHttpSprayJson,
  akkaRemote,
  akkaStream,
  akkaTestKit,
  chillAkka,
  guava,
  rocksDB,
  scalaTest)

/**
  * Tests:
  */
parallelExecution in Test := false

/**
  * Scalariform:
  */
SbtScalariform.scalariformSettings
ScalariformKeys.preferences := FormattingPreferences()
  .setPreference(RewriteArrowSymbols, false)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(SpacesAroundMultiImports, true)
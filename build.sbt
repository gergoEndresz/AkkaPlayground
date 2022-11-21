ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val akkaVersion = "2.6.18"
val akkaHttpVersion = "10.2.7"
val scalatestVersion = "3.2.10"

lazy val akkaDependencies = Seq(
  "akka-actor",
  "akka-stream",
  "akka-slf4j",
  "akka-discovery"
).map("com.typesafe.akka" %% _ % akkaVersion)

lazy val akkaHttpDependencies = Seq(
  "akka-http-xml",
  "akka-http-spray-json",
  "akka-parsing",
  "akka-http2-support",
  "akka-http-core",
  "akka-http"
).map("com.typesafe.akka" %% _ % akkaHttpVersion)

lazy val testDependencies = Seq("org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.14.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "AkkaPlayground",
    libraryDependencies ++= akkaDependencies ++ akkaHttpDependencies ++ testDependencies
  )

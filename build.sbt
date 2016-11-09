name := "ccp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  // other dependencies here
  "org.scalanlp" %% "breeze" % "0.12",
  "org.typelevel" %% "cats" % "0.8.0",
  "com.typesafe.akka" %% "akka-actor" % "2.4.12",
  "com.typesafe" % "config" % "1.3.1",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
)

//resolvers ++= Seq(
//  // other resolvers here
//  // if you want to use snapshot builds (currently 0.12-SNAPSHOT), use this.
//  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
//  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
//)

name := "ccp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  // other dependencies here
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",
  "org.scalaz" %% "scalaz-core" % "7.2.8",
  "com.typesafe.akka" %% "akka-actor" % "2.4.14",
  "com.typesafe" % "config" % "1.3.1",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.4",
  "org.spire-math" %% "spire" % "0.11.0"
)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)


//resolvers ++= Seq(
//  // other resolvers here
//  // if you want to use snapshot builds (currently 0.12-SNAPSHOT), use this.
//  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
//  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
//)

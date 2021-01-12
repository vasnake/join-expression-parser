import sbt._

object Dependencies {

  val sparkVersion = "2.4.5"

  lazy val sparkModules: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core",
    "org.apache.spark" %% "spark-sql"
  ).map(_ % sparkVersion)

  lazy val testModules: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % "3.2.2",
    "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.0.0"
  )

  lazy val libs: Seq[ModuleID] = Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
  )

}

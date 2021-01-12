import Dependencies._

ThisBuild / scalaVersion          := "2.12.12"
ThisBuild / version               := "0.1.0-SNAPSHOT"
ThisBuild / organization          := "github.com.vasnake"
ThisBuild / organizationHomepage  := Some(url("https://github.com/vasnake/join-expression-parser"))

lazy val root = (project in file("."))
  .settings(
    name := "sql-join-expression",
    libraryDependencies ++= libs ++ (sparkModules ++ testModules).map(_ % Test)
  )

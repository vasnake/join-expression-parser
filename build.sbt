import Dependencies._

ThisBuild / scalaVersion          := "2.13.4"
ThisBuild / version               := "0.1.0-SNAPSHOT"
ThisBuild / organization          := "github.com.vasnake"
ThisBuild / organizationHomepage  := Some(url("https://github.com/vasnake/join-expression-parser"))

lazy val root = (project in file("."))
  .settings(
    name := "sql-join-expression",
    libraryDependencies += scalaTest % Test
  )

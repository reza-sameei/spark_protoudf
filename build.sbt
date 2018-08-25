
val commonSettings = Seq(
    organization := "xyz.sigmalab.xtool",
    name := "spark-protoudf-project",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.11.12"
)

lazy val root = sbt.Project("root", file(".")).aggregate(macrodef).settings(commonSettings:_*)

lazy val macrodef = sbt.Project("macrodef", file("module/macrodef")).settings(commonSettings:_*)

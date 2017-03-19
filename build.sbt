import _root_.sbt.Keys._

name := "calcite-sample"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.11.0"
libraryDependencies += "org.apache.calcite" % "calcite-linq4j" % "1.11.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.6"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.6"
libraryDependencies += "junit" % "junit" % "4.12"
libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"
libraryDependencies += "commons-io" % "commons-io" % "2.4"
libraryDependencies += "org.hamcrest" % "hamcrest-core" % "1.4-atlassian-1"
libraryDependencies += "sqlline" % "sqlline" % "1.2.0"
libraryDependencies += "commons-beanutils" % "commons-beanutils" % "1.9.3"

retrieveManaged := true

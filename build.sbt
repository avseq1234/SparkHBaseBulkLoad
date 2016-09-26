name := "HBaseBulkLoad1"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.0.0"

libraryDependencies += "eu.unicredit" %% "hbase-rdd" % "0.7.1"
    
name := "Omega Simulator"

version := "0.1"

organization := "edu.berkeley.cs"

mainClass := Some("Simulation")

scalacOptions += "-deprecation"

// Add a dependency on commons-math for poisson random number generator
libraryDependencies += "org.apache.commons" % "commons-math" % "2.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.7.2" % "test"

libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.4.1"


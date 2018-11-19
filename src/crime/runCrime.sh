#!/bin/bash

# remove compiled java classes and jar file
rm *.class
rm *.jar

# compile .java files and jar
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main CrimeReducer.java
jar cf CrimeReducer.jar CrimeReducer*.class

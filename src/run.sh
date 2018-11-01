#!/bin/bash

rm *.class ||: \
&& /usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main ./WordCount.java \
&& jar cf WordCount.jar WordCount*.class \
&& /usr/local/hadoop/bin/hadoop fs -rm -R /tmp/out ||: \
&& /usr/local/hadoop/bin/hadoop jar ./WordCount.jar WordCount /tmp/data /tmp/out
#&& /usr/local/hadoop/bin/hadoop fs -cat /tmp/out/*

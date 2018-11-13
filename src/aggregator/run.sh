#!/bin/bash

rm *.class ||: \
&& /usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main ./CrimeWeatherAgg.java \
&& jar cf CrimeWeatherAgg.jar CrimeWeatherAgg*.class \
&& /usr/local/hadoop/bin/hadoop fs -rm -R /tmp/out/aggOut ||: \
&& /usr/local/hadoop/bin/hadoop jar ./CrimeWeatherAgg.jar CrimeWeatherAgg /tmp/out/aggOut /tmp/data/weather /tmp/data/crime
#&& /usr/local/hadoop/bin/hadoop fs -cat /tmp/out/aggOut/*

#!/bin/bash

rm *.class ||: \
&& /usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main ./WeatherReducer.java \
&& jar cf WeatherReducer.jar WeatherReducer*.class \
&& /usr/local/hadoop/bin/hadoop fs -rm -R /tmp/out/weather ||: \
&& /usr/local/hadoop/bin/hadoop jar ./WeatherReducer.jar WeatherReducer /tmp/data/weather /tmp/out/weather
#&& /usr/local/hadoop/bin/hadoop fs -cat /tmp/out/weather/*

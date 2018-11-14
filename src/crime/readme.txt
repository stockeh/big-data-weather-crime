Usage-

Run:

hadoop jar CrimeReducer.jar CrimeReducer <district number (int)> <hdfs file path to data file> <hdfs file path to out folder>

Where hadoop is alias for:
$HADOOP_HOME/bin/hadoop

Assume csv file Crimes2001-PChicago.csv has been stripped of it's header line. 
I used this command to accomplish this:

sed -i 1d file.csv 

With my settings, my run command would like like this for district 7:

hadoop jar CrimeReducer.jar CrimeReducer 7 /testFolder/Crimes2001-PChicago.csv /testFolder/out


Each district takes about a minute or less to run on yarn.  See job conf for file path to final folder.  NumReducers for job2 is set to 1.

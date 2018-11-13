import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.StringJoiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


/**
 * The CrimeWeatherAgg class will join the proccessed crime and
 * weather datasets by date (key). Date format output needs to be set to MM/DD/YYYY
 * in each mapper to group by date.
 */

public class CrimeWeatherAgg {

  /**
   * Two mappers, one for each Crime and Weather input file.
   * Output of each mapper is in the form <Date, {wX or CrimeSet}>
   * Wx = {Date,Wx1,Wx2,Wx3,Wx4}
   * CrimeSet = {Date,District,C1,C2,C3,C4,C5,C6,C7,C8}
   */

  public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {

    private Text date = new Text();
    private Text deltaWx = new Text();

    public void map(Object key, Text result, Context context) throws IOException, InterruptedException {

      String[] inputSplit = result.toString().split(",");
      StringJoiner Wx = new StringJoiner(",");

      if(inputSplit.length > 4) {

        // add all wX's comma seperated
        for(int i = 1; i < 5; i++) {
          Wx.add(inputSplit[i]);
        }

        // format date from YYYY-MM-DD to
        String[] tempDate = inputSplit[0].split("-");
        String dateOut = tempDate[1] + "/" + tempDate[2] + "/" + tempDate[0];

        date.set(dateOut);
        deltaWx.set("W" + Wx);

        // Output <date, deltaWx>
        context.write(date, deltaWx);
      }
    }
  }

  public static class CrimeMapper extends Mapper<Object, Text, Text, Text> {

    private Text date = new Text();
    private Text crimeSet = new Text();

    public void map(Object key, Text result, Context context) throws IOException, InterruptedException {

      String[] inputSplit = result.toString().split(",");
      StringJoiner crime = new StringJoiner(",");

      if(inputSplit.length > 9) {

        // add all wX's comma seperated
        for(int i = 1; i < 10; i++) {
          crime.add(inputSplit[i]);
        }

        date.set(inputSplit[0]);
        crimeSet.set("C" + crime);

        // Output <date, deltaWx>
        context.write(date, crimeSet);
      }
    }
  }


  /**
   * Sort-Shuffle phase will return a list of values(Wx or crimeSet) corresponding to each key(Date)
   * The reducer is recieveing input in this format: {date, [("W" + wX, "C" + crimeSet)]}
   * Our output should be {Date,Wx1,Wx2,Wx3,Wx4,District,C1,C2,C3,C4,C5,C6,C7,C8}.
   */
  public static class Reducer1 extends Reducer<Text, Text, Text, NullWritable> {

    // Used to context write out
    Text finalOut = new Text();
    String lineOut = "";
    String crimeLine = "";
    String weatherLine = "";
    String date = "";

    public void reduce(Text key, Iterable<Text> result, Context context) throws IOException, InterruptedException {

      // arrayList used to copy result
      ArrayList<String> aggList = new ArrayList<String>();
      ArrayList<String> weatherList = new ArrayList<String>();
      ArrayList<String> crimeList = new ArrayList<String>();

//      // Copy iterable into new arrayList
//      for (Text res : result) {
//        resList.add(res.toString());
//      }

      // for each crime or weather line
      for (Text r : result) {

        String line = r.toString();

        // check if string is wX
        if (line.charAt(0) == 'W') {

          // Remove the "W"
          weatherLine = line.substring(1);
        }

        // else its a crimeSet
        else {

            // Remove the "C"
            crimeLine = line.substring(1);
        }
      }

      if(crimeLine != "" && weatherLine != "") {

        lineOut = key + "," + weatherLine + "," + crimeLine;

//      System.out.println("FINAL OUT: " + key + " " + finalOut);

        finalOut.set(lineOut);
        context.write(finalOut, NullWritable.get());
      }
    }
  }

  /**
   * Driver method for the CrimeWeatherAgg class.
   */
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CrimeWeatherAgg");

    MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, CrimeMapper.class);

    MultipleInputs.addInputPath(job, new Path(args[2]),
            TextInputFormat.class, WeatherMapper.class);

    job.setJarByClass(CrimeWeatherAgg.class);
//    job.setPartitionerClass(job1Partitioner.class);
    job.setNumReduceTasks(1);
    job.setReducerClass(Reducer1.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}

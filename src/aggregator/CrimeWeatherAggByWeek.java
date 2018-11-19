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
import org.apache.hadoop.mapreduce.Counter;
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

  private static int weekDays = 0;
  private static int weekNum = 1;

  /**
   * Two mappers, one for each Crime and Weather input file.
   * Output of each mapper is in the form <Date, {wX or CrimeSet}>
   * Wx = {Date,Wx1,Wx2,Wx3,Wx4}
   * CrimeSet = {Date,District,C1,C2,C3,C4,C5,C6,C7,C8}
   *
   * Addition Nov. 17, 2018:
   * Changed to aggregate data based on week, not day. This attempts to get rid of some days where there's
   * lots of zeroes. some days were: {0, 0, 0, 0, 0, 0, 1, 0}
   *
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
        String dateOut = tempDate[0] + "/" + tempDate[1] + "/" + tempDate[2];

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

        // format date from MM-DD-YYYY to YYYY-MM-DD
        String[] tempDate = inputSplit[0].split("/");
        String dateOut = tempDate[2] + "/" + tempDate[0] + "/" + tempDate[1];

        date.set(dateOut);
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

    public void reduce(Text key, Iterable<Text> result, Context context) throws IOException, InterruptedException {

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

        weatherLine = "";
        crimeLine = "";
        finalOut.clear();
      }
    }
  }

  /**
   * Mapper2 assigns every 7 days to a week number starting from 1.
   */
  public static class WeatherCrimeMapper2 extends Mapper<Object, Text, Text, Text> {

    Text lineOut = new Text();
    Text weekOut = new Text();

    public void map(Object key, Text result, Context context) throws IOException, InterruptedException {

      StringJoiner Wx = new StringJoiner(",");

      if(weekDays < 7)
      {
//        context.getCounter(DocumentsCount.WEEKDAYS).increment(1);
        String[] inputSplit = result.toString().split(",");

        for(int i = 1; i < 14; i++){
          Wx.add(inputSplit[i]);
        }
        weekDays++;
      }

      else
      {
        weekDays = 0;
        weekNum += 1;

        String[] inputSplit = result.toString().split(",");

        for(int i = 1; i < 14; i++){
          Wx.add(inputSplit[i]);
        }
      }

        // format date from YYYY-MM-DD to
        String dateOut = Integer.toString(weekNum);

        weekOut.set(dateOut);
        lineOut.set("" + Wx);

        // Output <date, deltaWx>
        context.write(weekOut, lineOut);
      }
    }

  /**
   * Reducer gets a key, value pair which is <WeekNum, {List of weather and crimes for the WeekNum}>>
   */

  // { WeekNum,Wx1,Wx2,Wx3,Wx4,District,C1,C2,C3,C4,C5,C6,C7,C8 }.
public static class Reducer2 extends Reducer<Text, Text, Text, NullWritable> {

  public void reduce(Text key, Iterable<Text> result, Context context) throws IOException, InterruptedException {

    // Used to context write out
    Text finalOut = new Text();
    double wx1 = 0.0;
    double wx2 = 0.0;
    double wx3 = 0.0;
    double wx4 = 0.0;
    int district = 0;
    int c1 = 0;
    int c2 = 0;
    int c3 = 0;
    int c4 = 0;
    int c5 = 0;
    int c6 = 0;
    int c7 = 0;
    int c8 = 0;
    String lineOut = "";
    String crimeLine = "";
    String weatherLine = "";

    // for each crime or weather line
    for (Text r : result) {

      String[] inputSplit = r.toString().split(",");
      wx1 += Double.parseDouble(inputSplit[0]);
      wx2 += Double.parseDouble(inputSplit[1]);
      wx3 += Double.parseDouble(inputSplit[2]);
      wx4 += Double.parseDouble(inputSplit[3]);
      district = Integer.parseInt(inputSplit[4]);
      c1 += Integer.parseInt(inputSplit[5]);
      c2 += Integer.parseInt(inputSplit[6]);
      c3 += Integer.parseInt(inputSplit[7]);
      c4 += Integer.parseInt(inputSplit[8]);
      c5 += Integer.parseInt(inputSplit[9]);
      c6 += Integer.parseInt(inputSplit[10]);
      c7 += Integer.parseInt(inputSplit[11]);
      c8 += Integer.parseInt(inputSplit[12]);
    }

    double wx1Avg = wx1/7;
    double wx2Avg = wx2/7;
    double wx3Avg = wx3/7;
    double wx4Avg = wx4/7;

    lineOut = Double.toString(wx1Avg) + "," + Double.toString(wx2Avg) + "," + Double.toString(wx3Avg)
            + "," + Double.toString(wx4Avg) + "," + Integer.toString(district) + "," + Integer.toString(c1) + "," +
            Integer.toString(c2) + "," + Integer.toString(c3) + "," + Integer.toString(c4) + "," + Integer.toString(c5) + "," +
            Integer.toString(c6) + "," + Integer.toString(c7) + "," + Integer.toString(c8);


      lineOut = key + "," + lineOut;

//      System.out.println("FINAL OUT: " + key + " " + finalOut);

      finalOut.set(lineOut);
      context.write(finalOut, NullWritable.get());

      lineOut = "";
      crimeLine = "";
      finalOut.clear();

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

    job.setNumReduceTasks(1);
    job.setReducerClass(Reducer1.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[0]+ "/tempCompare/"));

    job.waitForCompletion(true);



    Job job2 = Job.getInstance(conf, "job2");
    job2.setJarByClass(CrimeWeatherAgg.class);

    job2.setMapperClass(WeatherCrimeMapper2.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setNumReduceTasks(1);
    job2.setReducerClass(Reducer2.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job2, new Path(args[0]+ "/tempCompare/"));
    FileOutputFormat.setOutputPath(job2, new Path(args[0]+"/final/"));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}

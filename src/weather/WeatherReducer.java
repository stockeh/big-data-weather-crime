import java.io.IOException;
import java.util.StringTokenizer;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The WeatherReducer class aggregates weather data
 * across multiple stations over a multi-yaer period.
 */

public class WeatherReducer {

  /**
   * Mapper class reads each input line by line to organize desired
   * weather attibutes per date.  These attributes in the dataset are:
   *
   *  - HOURLYDRYBULBTEMPF (10)
   *  - HOURLYDewPointTempF (14)
   *  - HOURLYWindSpeed (17)
   *  - HOURLYRelativeHumidity (16)
   *
   * Missing values or improperly formatted values will be labeled "?"
   * Expected output of the form <DATE  Wx1,Wx2,Wx3,Wx3>
   */
  public static class ParceWeather
       extends Mapper<LongWritable, Text, Text, Text>{

    private Text date = new Text();
    private Text weather = new Text();
    private final Integer[] indicies = {10, 14, 17, 16};

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String val = value.toString();
      // Skip header of .csv file.
      if (key.get() == 0 && val.contains("STATION_NAME")) {
        return;
      }
      else {
        String[] line = val.split(",");
        StringJoiner Wx = new StringJoiner(",");
        String item;

        for (int i : indicies) {
          item = line[i].replaceAll("[^-\\d.]", "");
          Wx.add(item.equals("") ? "?" : item);
        }
        date.set(line[5].split("\\s+")[0]);

        weather.set(Wx.toString());
        context.write(date, weather);
      }
    }
  }

  /**
   * Reduce class organizes data by date in the form YYYY-MM-DD.
   * The four weather attributes are averaged by day for all properly
   * formatted values.  Thus, disregarding missing values.
   *
   * The average is computed by summing valid values and incrimenting
   * by the number of summations for that feature.
   *
   * Expected output of the form <DATE,DeltaWx1,DeltaWx2,DeltaWx3,DeltaWx4>
   */
  public static class ReduceWeather
       extends Reducer<Text, Text, Text, NullWritable> {

    private Text composite = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {

      StringJoiner out = new StringJoiner(",");
      out.add(key.toString());

      Integer[] counts = {0, 0 , 0, 0};
      Double[] deltaWx = {0.0, 0.0, 0.0, 0.0};
      String[] temp = new String[4];
      int errors = 0;

      for (Text val : values) {
        temp = val.toString().split(",");

        for (int i = 0; i < 4; ++i) {
          if (!temp[i].equals("?")) {
            deltaWx[i] += Double.parseDouble(temp[i]);
            ++counts[i];
          }
          else {
            ++errors;
          }
        }
      }
      // System.out.println("ERROR: " + errors + ", on " + key.toString());
      for (int i = 0; i < 4; ++i) {
        out.add(Double.toString(deltaWx[i] / counts[i]));
      }
      composite.set(out.toString());
      context.write(composite, NullWritable.get());
    }
  }

  /**
   * Driver method for the WeatherReducer class.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("USAGE: <Inputdirectory> <Outputlocation>");
      System.exit(0);
	  }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Weather Reducer");
    job.setJarByClass(WeatherReducer.class);

    job.setMapperClass(ParceWeather.class);
    job.setReducerClass(ReduceWeather.class);

    // Reducer output key, value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // Reducer output key, value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    // Recursively map each input in directory to reducer.
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

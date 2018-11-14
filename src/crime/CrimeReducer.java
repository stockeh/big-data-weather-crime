import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.*;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeReducer {


  /*Mapper 1
  * Writes out <Text, IntWritable>
  * example out <'10/02/2018,THEFT'  1> */
  public static class CategoryMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final Set<Integer> one = new HashSet<Integer>(Arrays.asList(1,2,3,4,9,10,11,12,13,14,76,77));
    private final Set<Integer> two = new HashSet<Integer>(Arrays.asList(5,6,7,21,22));
    private final Set<Integer> three = new HashSet<Integer>(Arrays.asList(15,16,17,18,19,20));
    private final Set<Integer> four = new HashSet<Integer>(Arrays.asList(8,32,33));
    private final Set<Integer> five = new HashSet<Integer>(Arrays.asList(23,24,25,26,27,28,29,30,31));
    private final Set<Integer> six = new HashSet<Integer>(Arrays.asList(56,57,58,59,61,62,63,64,65,66,67,68));
    private final Set<Integer> seven = new HashSet<Integer>(Arrays.asList(34,35,36,37,38,39,40,41,42,43,60,69));
    private final Set<Integer> eight = new HashSet<Integer>(Arrays.asList(70,71,72,73,74,75));
    private final Set<Integer> nine = new HashSet<Integer>(Arrays.asList(44,45,46,47,48,49,50,51,52,53,54,55));

    private final Set<String> primary = new HashSet<String>(Arrays.asList("HOMICIDE","ROBBERY","BATTERY","ASSAULT","BURGLARY","THEFT","MOTOR VEHICLE THEFT","WEAPONS VIOLATION"));

    private final static IntWritable count = new IntWritable(1);
    private Text word = new Text();
    private String date;
    private String category;
    private String community;
    private String out;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      //Grab the district number passed via the command line
      Configuration conf = context.getConfiguration();
      int district = Integer.parseInt(conf.get("district"));

      //Create HashSet variable and set to reference to appropriate set for community numbers.
      //This is has been tested and compared to calling sets directly.
      Set<Integer> district_set = new HashSet<>();
      if (district == 1){
        district_set = one;
      }
      else if (district == 2){
        district_set = two;
      }
      else if (district == 3){
        district_set = three;
      }
      else if (district == 4){
        district_set = four;
      }
      else if (district == 5){
        district_set = five;
      }
      else if (district == 6){
        district_set = six;
      }
      else if (district == 7){
        district_set = seven;
      }
      else if (district == 8){
        district_set = eight;
      }
      else {
        district_set = nine;
      }

      String[] row = value.toString().split(",");
      if (row.length > 14){ //ensure valid row size

        date = row[2].split(" ")[0]; //index 0 contains date
        category = row[5].trim(); //index 5 contains crime category string
        community = row[13]; //index 13 contains community id

        //check size to ensure no missing information, also ensure that community is a digit
        if (community.length()>0 && category.length()>0 && community.matches("\\d+")) {

          //if category and community are located in the relevant set, then we want to map and write that out
          if (primary.contains(category) && district_set.contains(Integer.parseInt(community))) {
            out = date + "," + category;
            word.set(out);
            context.write(word, count);
          }
        }
      }
    }
  }

  /* Reducer1
  *  Input: <Text, IntWritable> exp: <'10/02/2018,THEFT' 1, 1, 1>
  *  Output: <Text, Text> exp: <'10/02/2018' 'THEFT,3'>   */
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,Text> {
    private IntWritable result = new IntWritable();
    private Text keyout = new Text();
    private Text valueout = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
      }
      String[] temp = key.toString().split(",");

      String keyoutstring = temp[0];
      keyout.set(keyoutstring);

      String valueoutstring = temp[1] + "," + sum;
      System.out.println(valueoutstring);
      valueout.set(valueoutstring);
      context.write(keyout, valueout);
    }
  }

  //NEXT STEP FOR CRIME OUTOUT Date,District,C1,C2,C3,C4,C5,C6,C7,C8

  //Input: <'10/02/2018' 'THEFT,3'>
  //Output: <'10/02/2018' ['THEFT,3', 'ROBBERY,1', 'HOMOCIDE,2']>
  // Mapper2

  public static class CategorySumMapper extends Mapper<Object, Text, Text, Text>{

    private Text category_list = new Text();
    private Text date = new Text();
    private String date_temp;
    private String category_rate;


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] row = value.toString().split("\t");

      date_temp = row[0];
      category_rate = row[1];

      date.set(date_temp);
      category_list.set(category_rate);

      context.write(date, category_list);

    }
  }
  /* Reducer2
    Input: <'10/02/2018' ['THEFT,3', 'ROBBERY,1', 'HOMOCIDE,2']>
    Output: Date,District,C1,C2,C3,C4,C5,C6,C7,C8
    */
  public static class FinalReducer extends Reducer<Text,Text,Text,NullWritable> {

    private String category;
    private int total;
    private String result;
    private Text out = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      //Grab the district number passed to command line
      Configuration conf = context.getConfiguration();
      int district = Integer.parseInt(conf.get("district"));

      //Initialize an array to keep track of category sums
      int [] C = {0,0,0,0,0,0,0,0};
      String [] val_arr;

      for (Text val : values) {
        val_arr = val.toString().trim().split(",");
        category = val_arr[0];
        System.out.println("value in array at 0: " + val_arr[0]);
        System.out.println("value in array at 1: " + val_arr[1]);
        total = Integer.parseInt(val_arr[1]);

        //For each value check the category and add to the array
        //the total crime rate in the appropriate index.
        if (category.equals("HOMICIDE")){
          C[0] += total;
        }
        else if (category.equals("ROBBERY")){
          C[1] += total;
        }
        else if (category.equals("BATTERY")){
          C[2] += total;
        }
        else if (category.equals("ASSAULT")){
          C[3] += total;
        }
        else if (category.equals("BURGLARY")){
          C[4] += total;
        }
        else if (category.equals("THEFT")){
          C[5] += total;
        }
        else if (category.equals("MOTOR VEHICLE THEFT")){
          C[6] += total;
        }
        else {
          C[7] += total;
        }
      }

      //build result string and write
      result = key.toString() + "," + district + ",";
      for (int i=0; i<8; i++){
        if (i != 7){
          result += C[i] + ",";
        }
        else {
          result += C[i];
        }
      }

      out.set(result);
      context.write(out, NullWritable.get());

    }
  }


  public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  conf.set("district", args[0]);

  Job job = Job.getInstance(conf, "Crime Counter");
  job.setJarByClass(CrimeReducer.class);
  job.setMapperClass(CategoryMapper.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(IntWritable.class);
  job.setReducerClass(IntSumReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  job.setNumReduceTasks(10);
  FileInputFormat.addInputPath(job, new Path(args[1]));
  FileOutputFormat.setOutputPath(job, new Path(args[2] + "/intermediate"));
  job.waitForCompletion(true);

  Job job2 = Job.getInstance(conf, "Crime Aggregate");
  job2.setJarByClass(CrimeReducer.class);
  job2.setMapperClass(CategorySumMapper.class);
  job2.setMapOutputKeyClass(Text.class);
  job2.setMapOutputValueClass(Text.class);
  job2.setReducerClass(FinalReducer.class);
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(NullWritable.class);
  //job2.setNumReduceTasks(10);
  FileInputFormat.addInputPath(job2, new Path(args[2] + "/intermediate"));
  FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));
  System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}

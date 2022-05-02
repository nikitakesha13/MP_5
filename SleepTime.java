import java.io.IOException;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class SleepTime {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public String get_time(String time_given){

      String[] time = time_given.split(":");

      if (time.length == 3){
        String output = "";
        String start_time = time[0] + ":00";

        SimpleDateFormat format = new SimpleDateFormat("HH:mm");
        try {

          Calendar c = Calendar.getInstance();
          c.setTime(format.parse(start_time));
          c.add(Calendar.HOUR, 1);
          c.add(Calendar.MINUTE, -1);
          String end_time = format.format(c.getTime());

          output = start_time + " - " + end_time;
        } catch (ParseException e) {
          return "";
        }
        return output;
      }

      return "";
      
    }

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Vector<String> data = new Vector<String>(Arrays.asList(value.toString().split("\\s+")));

      Integer pos_time = data.indexOf("T");
      Integer pos_url = data.indexOf("U");
      Integer pos_text = data.indexOf("W");
      if ((pos_url != -1) && (pos_text != -1) && (pos_time != -1)){
        boolean found_sleep = false;
        if (pos_text + 1 < data.size()){
          for (int i = pos_text + 1; i < data.size(); ++i){
            if (data.get(i).contains("sleep")){
              found_sleep = true;
              break;
            }
          }
        }
        if (found_sleep){
          if (pos_time + 2 < data.size()){
            String time = get_time(data.get(pos_time+2));
            if (!time.isEmpty()){
              word.set(time);
              context.write(word, one);
            }
          }
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("textinputformat.record.delimiter", "\n\n");
    Job job = Job.getInstance(conf, "sleep time count");
    job.setJarByClass(SleepTime.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

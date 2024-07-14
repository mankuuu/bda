import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class maxtemperature {
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "maxtemperature");
job.setJarByClass(maxtemperature.class);
// TODO: specify a mapper
job.setMapperClass(MaxTempMapper.class);
// TODO: specify a reducer
job.setReducerClass(MaxTempReducer.class);
// TODO: specify output types
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
// TODO: specify input and output DIRECTORIES (not files)
FileInputFormat.setInputPaths(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
if (!job.waitForCompletion(true))
return;
}
}

//Mapper

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
public class MaxTempMapper extends Mapper<LongWritable, Text, Text,
IntWritable > {
public void map(LongWritable key, Text value, Context context) {
  throws IOException, InterruptedException {
String line=value.toString();
String year=line.substring(15,19);
int airtemp;
if(line.charAt(87)== '+')
{
airtemp=Integer.parseInt(line.substring(88,92));
}
else
airtemp=Integer.parseInt(line.substring(87,92));
String q=line.substring(92,93);
if(airtemp!=9999&&q.matches("[01459]"))
{
context.write(new Text(year),new IntWritable(airtemp));
}
}
}
}

//Reducer

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class MaxTempReducer extends Reducer<Text, IntWritable, Text,
IntWritable>
{
public void reduce(Text key, Iterable<IntWritable> values, Context
context)
throws IOException, InterruptedException
{
int maxvalue=Integer.MIN_VALUE;
for (IntWritable value : values)
  {
maxvalue=Math.max(maxvalue, value.get());
}
context.write(key, new IntWritable(maxvalue));
}
}

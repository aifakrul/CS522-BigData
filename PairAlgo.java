import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairAlgo extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String custOrder=value.toString();
			String [] items=custOrder.split("\\s+");
			
			for(int i=0;i<items.length-1;i++)
			{
				String order=items[i].trim();
				for(int j=i+1;j<items.length;j++)
				{
					if(order.equalsIgnoreCase(items[j].trim()))
						break;
					
					word.set(order+","+items[j].trim());
					context.write(word, one);
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new PairAlgo(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "PairAlgo");
		job.setJarByClass(PairAlgo.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(2);
		
		Configuration conf = new Configuration();
		
		Path output = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		
		// delete existing directory
		if (hdfs.exists(output)) {
		  hdfs.delete(output, true);
		}
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}

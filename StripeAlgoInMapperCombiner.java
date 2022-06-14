import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class StripeAlgoInMapperCombiner extends Configured implements Tool
{
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StripePair>
	{
		private HashMap<String, HashMap<String, Integer>> map;
		
		@Override
		public void setup(Context context)
		{
			map=new HashMap<String,HashMap<String, Integer> >();
		}
		
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
					if(order.equalsIgnoreCase(items[j]))
						break;
				
					HashMap<String, Integer> h;
					if(!this.map.containsKey(order))
					{
						h=new HashMap<String, Integer>();
						h.put(items[j], 1);
					}
					else
					{
						h=this.map.get(order);
						if(!h.containsKey(items[j]))
						{
							h.put(items[j], 1);
						}
						else
						{
							h.put(items[j], h.get(items[j])+1);
						}
					}
					
					this.map.put(order, h);
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			Text word = new Text();
			for(Entry<String, HashMap<String,Integer>> item:this.map.entrySet())
			{
				word.set(item.getKey());
				for(Entry<String,Integer> subItem:item.getValue().entrySet())
				{
					context.write(word, new StripePair(subItem.getKey(),subItem.getValue()));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, StripePair, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<StripePair> values, Context context) throws IOException, InterruptedException
		{
			TreeMap<String, Integer> h=new TreeMap<String, Integer>(); // for sorting
			for (StripePair val : values)
			{
				if(!h.containsKey(val.getKey()))
				{
					h.put(val.getKey(), val.getValue());
				}
				else
				{
					h.put(val.getKey(),h.get(val.getKey())+ val.getValue());
				}
			}
			
			for(Entry<String,Integer> item:h.entrySet())
			{
				context.write(key,new Text(new StripePair(item.getKey(),item.getValue()).toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new StripeAlgoInMapperCombiner(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "StripeAlgo");
		job.setJarByClass(StripeAlgoInMapperCombiner.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StripePair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(1);
		
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

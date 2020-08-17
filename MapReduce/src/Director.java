import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Director {

	public static void main(String[] args) throws Exception {
		args = new String[]{"Movies_input", "Movies_Director_output"};
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args))
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("error");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Director");
		
		job.setJarByClass(Director.class);
		job.setJobName("Director");
		job.setMapperClass(Director.MyMapper.class);
		job.setReducerClass(Director.MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		
	    private Text out_key = new Text();
	    private IntWritable out_value = new IntWritable();
	    
	    @Override //肖申克的救赎	9.7	剧情/犯罪	美国	英语	1994-09-10	142	弗兰克·德拉邦特	5595	7.5	蒂姆·罗宾斯	12186	7.1	摩根·弗里曼	36318	7.2
	    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
	            throws IOException, InterruptedException {
	    	
	    	String line = value.toString();
	    	String[] items = StringUtils.split(line, '\t');
	    	
    		out_key.set(items[7]);
    		out_value.set(Integer.parseInt(items[8]));
    		context.write(out_key, out_value);
	    }
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
	    private Text out_key = new Text();
	    private IntWritable out_value = new IntWritable();
	    

	    @Override // key: type  value: score, score ....
	    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
	            throws IOException, InterruptedException {
	    	int fans_num = 0;

	    	for(IntWritable score: values) {
	    		fans_num = Integer.parseInt(score.toString());
	    	}
			out_key.set(key);  
			out_value.set(fans_num);
			context.write(out_key, out_value);
	    }
	}
}
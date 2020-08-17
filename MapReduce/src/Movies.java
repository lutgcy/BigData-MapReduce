import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Movies {

	public static void main(String[] args) throws Exception {
		args = new String[]{"Movies_input", "Movies_output"};
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args))
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("error");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Movies");
		job.setJarByClass(Movies.class);
		job.setJobName("Movies");
		job.setMapperClass(Movies.MyMapper.class);
		job.setReducerClass(Movies.MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		
	    private Text out_key = new Text();
	    private Text out_value = new Text();
	    
	    @Override //猫鼠游戏	8.9	剧情/传记/犯罪	
	    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
	            throws IOException, InterruptedException {
	    	
	    	String line = value.toString();
	    	String[] items = StringUtils.split(line, '\t');
	    	
	    	String[] types = items[2].split("/");
	    	
	    	for(String e: types) {
	    		out_key.set(e);
	    		out_value.set(items[1]);
	    		context.write(out_key, out_value);
	    	}
	    }
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
	    
	    private Text out_key = new Text();
	    private Text out_value = new Text();
	    
	    DecimalFormat df = new DecimalFormat("#.#");

	    @Override // key: type  value: score, score ....
	    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
	            throws IOException, InterruptedException {
	    	int count = 0;

	    	Double sum = 0.0d;
	    	for(Text score: values) {
	    		sum += Double.parseDouble(score.toString());
	    		count++;
	    	}
			out_key.set(key);  
			out_value.set(df.format(sum / count) + "\t" + String.valueOf(count));
			context.write(out_key, out_value);
	    }
	}
}
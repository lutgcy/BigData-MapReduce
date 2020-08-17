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

public class Long {

	public static void main(String[] args) throws Exception {
		args = new String[]{"Movies_input", "Movies_time_output"};
		Configuration conf = new Configuration();
		String[] otherArgs = (new GenericOptionsParser(conf, args))
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("error");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Long");
		job.setJarByClass(Long.class);
		job.setJobName("Long");
		job.setMapperClass(Long.MyMapper.class);
		job.setReducerClass(Long.MyReducer.class);
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
	    
	    @Override //忠犬八公的故事	9.3	剧情	美国/英国	  英语/日语	2009-06-13	93	拉斯·霍尔斯道姆	2467	6.6	理查·基尔	6928	7.0	萨拉·罗默尔	420	5.6
	    protected void map(Object key, Text line, Mapper<Object, Text, Text, Text>.Context context)
	            throws IOException, InterruptedException {
	    	
	    	String[] items = StringUtils.split(line.toString(), '\t');
	    	
	    	int time = 0;
	    	String[] list = items[6].replace(" ", "").split(":");
	    	if(list.length == 1) {
	    		time = Integer.parseInt(list[0]);
	    	} else {
	    		time = Integer.parseInt(list[1]);
	    	}
	    	
	    	if (time < 90) {
	    		out_key.set("0~90");
	    	} else if(time < 100) {
	    		out_key.set("90~100");
	    	} else if(time < 110) {
	    		out_key.set("100~110");
	    	} else if(time < 120) {
	    		out_key.set("110~120");
	    	} else if(time < 130) {
	    		out_key.set("120~130");
	    	} else if(time < 140) {
	    		out_key.set("130~140");
	    	} else if(time < 150) {
	    		out_key.set("140~150");
	    	} else {
	    		out_key.set("150~");
	    	}
    		out_value.set("1");	
    		context.write(out_key, out_value);
    		
	    }
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
	    
	    private Text out_key = new Text();
	    private Text out_value = new Text();
	    
	    @Override // key: type  value: score, score ....
	    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
	            throws IOException, InterruptedException {

	    	int sum = 0;
	    	for(Text score: values) {
	    		sum += Integer.parseInt(score.toString());
	    	}
			out_key.set(key);  
			out_value.set(String.valueOf(sum));
			context.write(out_key, out_value);
	    }
	}
}
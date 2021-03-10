import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class Poker_cardgame {
public static class PMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    public void map(LongWritable key, Text value, Context con1) throws IOException, InterruptedException
    {
        String line_rows = value.toString();
        String[] line_rowsSplit = line_rows.split(",");
        con1.write(new Text(line_rowsSplit[0]), new IntWritable(Integer.parseInt(line_rowsSplit[1])));
    }
}

public static class PReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> value, Context con1)
    throws IOException, InterruptedException {

    	ArrayList<Integer> numbers_list = new ArrayList<Integer>();

    	int summation1 = 0;
    	int current_temporary_value = 0;

    	for (IntWritable val : value) {
    		summation1+= val.get();
    		current_temporary_value = val.get();
    		numbers_list.add(current_temporary_value);
    	}

    	if(summation1 < 91){
    		for (int i = 1;i <= 13;i++){
    			if(!numbers_list.contains(i))
    				 con1.write(key, new IntWritable(i));
    		}
    	}
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = new Job(conf, "Finding the missing poker cards");
    job1.setJarByClass(Poker_cardgame.class);
    job1.setMapperClass(PMapper.class);
    job1.setReducerClass(PReducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
}
}
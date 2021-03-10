import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class Poker_cardgame {
    public static class PMapper extends Mapper < LongWritable, Text, Text, IntWritable > {
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException
        {
            String line_rows = value.toString();
            String[] line_rowsSplit = line_rows.split(",");
            context.write(new Text(line_rowsSplit[0]), new IntWritable(Integer.parseInt(line_rowsSplit[1])));
        }

    }

public static class PReducer extends TableReducer < Text, IntWritable, ImmutableBytesWritable >
        
        {
            int summation1 = 0;

            public void reduce(Text key, Iterable<IntWritable> value, Context con) throws IOException,
            InterruptedException {
                int i = 1, current_temporary_value = 0;
                ArrayList<Integer> suite = new ArrayList<Integer>((IntStream.range(1, 14)).boxed().collect(Collectors.toList()));
                for (IntWritable cards: value) {
                    current_temporary_value = cards.get();
                    if (suite.contains(current_temporary_value)) {
                        suite.remove(suite.indexOf(current_temporary_value));
                    }
                }
                for (i = 0; i < suite.size(); ++i) {

                    Put p = new Put(Bytes.toBytes(summation1));
                    summation1++;
                    p.addColumn(Bytes.toBytes("cards"), Bytes.toBytes("cardno"), Bytes.toBytes(suite.get(i)));
                    p.addColumn(Bytes.toBytes("cards"), Bytes.toBytes("cardtype"), Bytes.toBytes(key.toString()));
                    con.write(null, p);
                    //con.write(key, new IntWritable(suite.get(i)));
                }
            }
        }


    public static void main(String[] args) throws Exception {
        // Create the table
        Configuration conf_hbase = HBaseConfiguration.create();
        org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(conf_hbase);
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(TableName.valueOf("MissingPokerCards"));
        table.setColumnFamily(ColumnFamilyDescriptorBuilder.of("cards"));
        admin.createTable(table.build());

        Configuration conf2 = HBaseConfiguration.create();
        Job job1 = Job.getInstance(conf2, "Missing Poker Cards");
        job1.setJarByClass(Poker_cardgame.class);
        job1.setMapperClass(PMapper.class);
        //job1.setReducerClass(PReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        //job1.setOutputKeyClass(Text.class);
        //job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        TableMapReduceUtil.initTableReducerJob("MissingPokerCards", PReducer.class, job1);
        job1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "MissingPokerCards");
        job1.setOutputFormatClass(TableOutputFormat.class);

        boolean b = job1.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error");
        }
    }
                
}            
                
                
                
package org.cloudburst.etl.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;


public class HBaseService {
    private static final String TABLE_NAME = "tweets";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String outputKey = fields[1]+ "_" + fields[2];
            String outputValue = fields[0] + "_" + fields[3] + "_" + fields[4];
            context.write(new Text(outputKey), new Text(outputValue));
            value.clear();
        }
    }

    public static class Reduce extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {
        private ImmutableBytesWritable hkey = new ImmutableBytesWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String outputValue = null;
            // TODO: get the list of keyValues for each key, sort them by tweet id and
            // TODO: create a final outputValue that separates different entries with \n

            hkey.set(key.getBytes());
            KeyValue kv = new KeyValue(hkey.get(), Bytes.toBytes("tweetInfo"), Bytes.toBytes("data"),
                    Bytes.toBytes(outputValue));
            context.write(hkey, kv);
            hkey = new ImmutableBytesWritable();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.table.name", TABLE_NAME);

        Job job = new Job(conf, "hbaseloading");

        job.setJarByClass(HBaseService.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);

        HTable hTable = new HTable(conf, TABLE_NAME);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}


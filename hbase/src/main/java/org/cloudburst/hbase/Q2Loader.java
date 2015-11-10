package org.cloudburst.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
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

public class Q2Loader {
    private static final String TABLE_NAME = "tweets";
    private static final String TAB = "\t";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(TAB);

            /* Key { userId_createdAtDate } */
            String outputKey = fields[2] + "_" + fields[3];

            String outputValue = null;
            try {
                /* Value { tweetId : sentimentScore : text } */
                outputValue = fields[0] + ":" + fields[5] + ":"
                        + new String(Hex.decodeHex(fields[6].toCharArray()), "UTF-8");
            } catch (DecoderException e) {
                e.printStackTrace();
            }
            context.write(new Text(outputKey), new Text(outputValue));
            value.clear();
        }
    }

    public static class Reduce extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {
        private ImmutableBytesWritable hkey;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            hkey = new ImmutableBytesWritable();
            StringBuilder outputValue = new StringBuilder();
            HashMap<String, String> tweetMap = new HashMap<>();
            for (Text value : values) {
                String[] fields = value.toString().split(":");
                tweetMap.put(fields[0], value.toString());
            }
            // sort by tweet ID and then append to output with "\n"
            Object[] keys = tweetMap.keySet().toArray();
            Arrays.sort(keys);
            for (Object k : keys) {
                outputValue.append(tweetMap.get(k));
                outputValue.append("\n");
            }
            // write key value pairs to HFile
            hkey.set(key.getBytes());
            KeyValue kv = new KeyValue(hkey.get(), Bytes.toBytes("tweetInfo"), Bytes.toBytes("data"),
                    Bytes.toBytes(outputValue.toString()));
            context.write(hkey, kv);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.table.name", TABLE_NAME);

        Job job = new Job(conf);

        job.setJarByClass(Q2Loader.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

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

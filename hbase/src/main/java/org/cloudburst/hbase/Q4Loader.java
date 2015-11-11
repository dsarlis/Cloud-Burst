package org.cloudburst.hbase;

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
import org.cloudburst.util.Q4Object;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class Q4Loader {
    private static final String TABLE_NAME = "hashtags";
    private static final String TAB = "\t";
    private static final String COLON = ":";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /* Format of a line: hashtag \t date \t count \t userList \t earliest_tweet*/
            String line = value.toString();
            String[] fields = line.split(TAB);

            String outputKey = null;
            try {
                outputKey = new String(Hex.decodeHex(fields[0].toCharArray()), "UTF-8");
            } catch (DecoderException e) {
                e.printStackTrace();
            }
            String outputValue = fields[1] + COLON + fields[2] + COLON + fields[3] + COLON + fields[4];
            context.write(new Text(outputKey), new Text(outputValue));
            value.clear();
        }
    }

    public static class Reduce extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {
        private ImmutableBytesWritable hkey;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Q4Object> q4 = new ArrayList<>();
            for (Text value: values) {
                String[] parts = value.toString().split(COLON);
                q4.add(new Q4Object(parts[0], parts[1], parts[2], parts[3]));
            }

            Collections.sort(q4);

            int id = 0;
            for (Q4Object q: q4) {
                hkey = new ImmutableBytesWritable();
                // write key value pairs to HFile
                    String outputKey =  key.toString();
                    hkey.set(outputKey.getBytes());
                    String outputValue = q.getDate() + COLON + q.getCount() + COLON + q.getUserList() + COLON +
                            q.getText();
                    KeyValue kv = new KeyValue(hkey.get(), Bytes.toBytes("data"), Bytes.toBytes(id),
                            Bytes.toBytes(outputValue));
                    context.write(hkey, kv);
                    id++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.table.name", TABLE_NAME);

        Job job = new Job(conf);

        job.setJarByClass(Q4Loader.class);
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


package org.cloudburst.hbase;

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
import java.util.ArrayList;

public class Q3Loader {
    private static final String TABLE_NAME = "impact";
    private static final String TAB = "\t";
    private static final String UNDERSCORE = "_";
    private static final String COMMA = ",";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /* Format of a line: tweetId\tuserId\tcreationTime\tfollowers\tscore\ttext*/
            String line = value.toString();
            String[] fields = line.split(TAB);

            /* Key {userId_date} */
            String outputKey = fields[0] + UNDERSCORE + fields[1];
            /* Value {date,impactScore,tweetId,text(in hex)} */
            String outputValue = fields[1] + COMMA + fields[2] + COMMA + fields[3] + COMMA + fields[4];
            context.write(new Text(outputKey), new Text(outputValue));
            value.clear();
        }
    }

    public static class Reduce extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {
        private ImmutableBytesWritable hkey;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            hkey = new ImmutableBytesWritable();
            ArrayList<String> posImpacts = new ArrayList<>();
            ArrayList<String> negImpacts = new ArrayList<>();

            /* Put each value to the positive or negative tweets list according to impactScore */
            for (Text t: values) {
                String value = t.toString();
                String[] parts = value.split(COMMA);
                int impactScore = Integer.parseInt(parts[1]);
                if (impactScore > 0) {
                    posImpacts.add(value);
                } else {
                    negImpacts.add(value);
                }
            }

            StringBuilder posOutputValue = new StringBuilder();
            StringBuilder negOutputValue = new StringBuilder();

            for (String p: posImpacts) {
                posOutputValue.append(p).append(TAB);
            }

            for (String n: negImpacts) {
                negOutputValue.append(n).append(TAB);
            }
            /* write key value pairs to HFile */
            /* key {userId_date} */
            hkey.set(key.getBytes());
            KeyValue kv = new KeyValue(hkey.get(), Bytes.toBytes("data"), Bytes.toBytes("pos"),
                    Bytes.toBytes(posOutputValue.toString()));
            /* value {date_impactScore_tweetId_text} */
            context.write(hkey, kv);
            /* use a second write for the second column family */
            kv = new KeyValue(hkey.get(), Bytes.toBytes("data"), Bytes.toBytes("neg"),
                    Bytes.toBytes(negOutputValue.toString()));
            context.write(hkey, kv);
        }
    }

    // driver method for the Map Reduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.table.name", TABLE_NAME);

        Job job = new Job(conf);

        job.setJarByClass(Q3Loader.class);
        // set mapper and reducer keys and values
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);

        // set the output of the job to be in HFile format
        HTable hTable = new HTable(conf, TABLE_NAME);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}

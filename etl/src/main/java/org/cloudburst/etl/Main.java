package org.cloudburst.etl;

import java.io.IOException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
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
import org.cloudburst.etl.util.StringUtil;
import org.cloudburst.etl.util.TextCensor;

import com.google.gson.JsonSyntaxException;

/**
 * Main class to process Q2 files.
 */
public class Main {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * Mapper method that keeps TweetId as the KEY, and all other required fields as
         * the VALUE.
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();

                String[] tokens = line.split("\t");
                StringBuilder builder = new StringBuilder();
                String tweetId = tokens[0];

                builder.append(",");
                builder.append(tokens[3]);
                builder.append(",");
                builder.append(tokens[4]);
                builder.append(",");
                builder.append(tokens[6]);
                builder.append(",");
                String censored = TextCensor.censorBannedWords(new String(Hex.decodeHex(tokens[7].toCharArray())));
                builder.append(StringUtil.bytesToHex(censored.getBytes()));

                context.write(new Text(tweetId), new Text(builder.toString()));

            } catch (JsonSyntaxException e) {
			} catch (DecoderException e) {}
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        /**
         * The Reducer method simply print one of the value with same tweetId.
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
                break;
            }
        }
    }

    /**
     * The MAIN method serves as a driver of the custom job.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "etl");

        job.setJarByClass(Main.class);
        /* Set output keys for mapper and reducer */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Set input and output file formats */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

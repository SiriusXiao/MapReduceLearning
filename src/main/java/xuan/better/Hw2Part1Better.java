package xuan.better;

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

import java.io.IOException;
import java.util.Scanner;

public class Hw2Part1Better {

    public static class LineMapper extends Mapper<Object, Text, Line, DoubleWritable> {

        private Line mLine;
        private DoubleWritable mDistance;
        private Scanner mScanner;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mLine = new Line();
            mDistance = new DoubleWritable();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || value.getLength() == 0) {
                return;
            }
            mScanner = new Scanner(value.toString());

            String from, to;
            double distance;
            try {
                from = mScanner.next();
                to = mScanner.next();
                distance = mScanner.nextDouble();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("data is invalid.");
                return;
            }

            mLine.setFrom(from);
            mLine.setTo(to);
            mDistance.set(distance);

            context.write(mLine, mDistance);
        }
    }


    public static class LineReducer extends Reducer<Line, DoubleWritable, Text, Text> {

        private static final String BLOCK_KEY = "%-20s";
        private static final String BLOCK_VALUE = "%-10s";
        private Text mOutKey;
        private Text mOutValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mOutKey = new Text();
            mOutValue = new Text();
        }

        @Override
        protected void reduce(Line key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0f;
            for (DoubleWritable val : values) {
                count++;
                sum += val.get();
            }
            String from = key.getFrom();
            String to = key.getTo();
            String distance = String.format("%.2f", sum);
            mOutKey.set(String.format(BLOCK_KEY, "From:" + from) + String.format(BLOCK_KEY, "To:" + to));
            mOutValue.set(String.format(BLOCK_VALUE, "Count:" + count) + String.format(BLOCK_KEY, "Distance:" + distance));
            context.write(mOutKey, mOutValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "line job");

        job.setJarByClass(Hw2Part1Better.class);

        job.setMapperClass(LineMapper.class);
        job.setReducerClass(LineReducer.class);

        job.setMapOutputKeyClass(Line.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

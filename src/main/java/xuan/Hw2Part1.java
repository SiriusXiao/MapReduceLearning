package xuan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Scanner;

@Deprecated
public class Hw2Part1 {

    public static class LineMapper extends Mapper<Object, Text, Line, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scan = new Scanner(value.toString());
            Line line = new Line(scan.next(), scan.next());
            context.write(line, new DoubleWritable(scan.nextDouble()));
        }
    }

    public static class LineCombiner extends Reducer<Line, DoubleWritable, Line, DoubleWritable> {

        private DoubleWritable mCount;
        private DoubleWritable mSum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mCount = new DoubleWritable();
            mSum = new DoubleWritable();
        }

        @Override
        protected void reduce(Line key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            mSum.set(sum);
            mCount.set(count);
            context.write(key, mSum);
            context.write(key, mCount);
        }
    }

    public static class LineReducer extends Reducer<Line, DoubleWritable, Line, Text> {
        private Line mResultKey;
        private Text mResultValue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            mResultKey = new Line();
            mResultValue = new Text();
        }

        @Override
        protected void reduce(Line key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            double sum = 0, count = 0;
            for (DoubleWritable val : values) {
                if (i == 0) {
                    sum = val.get();
                } else {
                    count = val.get();
                }
            }
            mResultKey.set(key);
            mResultValue.set("sum=" + String.format(".2%f", sum) + ", count=" + String.format(".0%f", count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(LineMapper.class);
        job.setCombinerClass(LineCombiner.class);
        job.setReducerClass(LineReducer.class);

        job.setMapOutputKeyClass(Line.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Line.class);
        job.setOutputValueClass(Text.class);

        // add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // add the output path as given by the command line
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static class Line extends ObjectWritable implements Comparable {
        private String from;
        private String to;

        public Line() {
            this.from = "";
            this.to = "";
        }

        public Line(String from, String to) {
            this.from = from;
            this.to = to;
        }

        public int compareTo(Object o) {
            if (!(o instanceof Line)) {
                return -1;
            }
            if (this.from.equals(((Line) o).from)) {
                return this.to.compareTo(((Line) o).to);
            } else {
                return this.from.compareTo(((Line) o).from);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Line)) {
                return false;
            }
            Line tar = (Line) obj;
            return (from.equals(tar.from) && to.equals(tar.to))
                    || (from.equals(tar.to) && to.equals(tar.from));
        }
    }
}

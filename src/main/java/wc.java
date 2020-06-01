import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 * This Hadoop program is to implement counting the frequency
 * of words in a text file which is stored in HDFS.
 */

public class wc {

    private final static String rootPath = "";

    //map each word to a value one
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    //reduce values by a unique word
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //this program accepts two parameters by default;
        //if there is a third paramter, it is treated as the number of the reducers
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: wc.jar <input file> <output file> or");
            System.out.println("wc.jar <input file> <output file> <reduce number>");
            System.exit(1);
        }

        //set up Hadoop configuration
        Configuration conf = new Configuration();


        //create a Hadoop job
        Job job = Job.getInstance(conf, "WordCount for " + args[0]);
        job.setJarByClass(wc.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //set the number of reducers. By default, No. of reducers = 1
        if (args.length == 3) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
        }
        FileInputFormat.addInputPath(job, new Path(rootPath + args[0]));
        FileOutputFormat.setOutputPath(job, new Path(rootPath + args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
public class prank {
    // utility attributes
    public static NumberFormat NF = new DecimalFormat("00");
    public static String LINKS_SEPARATOR = "|";
    // configuration values
    public static Double DAMPING = 0.85;
    public static int ITERATIONS = 1;
    public static String INPUT_PATH = "";
    public static String OUTPUT_PATH = "";
    // A map task of the first job: transfer each line to a map pair
    public static class FetchNeighborsMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //skip the comment line with #
            if (value.charAt(0) != '#') {
                int tabIndex = value.find("\t");
                String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
                String nodeB = Text.decode(value.getBytes(), tabIndex + 1,
                        value.getLength() - (tabIndex + 1));
                context.write(new Text(nodeA), new Text(nodeB));
            }
        }
    }
    // A reduce task of the first job: fetch the neighbor's links
    // and set the initial value of fromLinkId 1.0
    public static class FetchNeighborsReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean first = true;
            String links = "1.0\t";
            int count = 0;
            for (Text value : values) {
                if (!first)
                    links += ",";
                links += value.toString();
                first = false;
                count++;
            }
            context.write(key, new Text(links));
        }
    }
    // A map task of the second job:
    public static class CalculateRankMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            int tIdx1 = value.find("\t");
            int tIdx2 = value.find("\t", tIdx1 + 1);
            // extract tokens from the current line
            String page = Text.decode(value.getBytes(), 0, tIdx1);
            String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2
                    - (tIdx1 + 1));
            // Skip pages with no links.
            if (tIdx2 == -1)
                return;
            String links = Text.decode(value.getBytes(), tIdx2 + 1,
                    value.getLength() - (tIdx2 + 1));
            String[] allOtherPages = links.split(",");
            for (String otherPage : allOtherPages) {
                Text pageRankWithTotalLinks = new Text(pageRank + "\t"
                        + allOtherPages.length);
                context.write(new Text(otherPage), pageRankWithTotalLinks);
            }
            // put the original links so the reducer is able to produce the
            // correct output
            context.write(new Text(page), new Text(prank.LINKS_SEPARATOR + links));
        }
    }
    public static class CalculateRankReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String links = "";
            double sumShareOtherPageRanks = 0.0;
            for (Text value : values) {
                String content = value.toString();
                //check if a linke has an appending 'links' string
                if (content.startsWith(prank.LINKS_SEPARATOR)) {
                    links += content.substring(prank.LINKS_SEPARATOR
                            .length());
                } else {
                    String[] split = content.split("\\t");
                    // extract tokens
                    double pageRank = Double.parseDouble(split[0]);
                    if (split[1] != null && !split[1].equals("null")) {
                        int totalLinks = Integer.parseInt(split[1]);
                        // calculate the contribution of each outgoing link
                        // of the current link
                        sumShareOtherPageRanks += (pageRank / totalLinks);
                    }
                }
            }
            //get the final page rank of the current link
            double newRank = prank.DAMPING * sumShareOtherPageRanks
                    + (1 - prank.DAMPING);
            //ignore the link which has no outgoing links
            if (newRank > 0.15000000000000002
                    && !key.toString().trim().equals(""))
                context.write(key, new Text(newRank + "\t" + links));
        }
    }
    // A map task of the third job for sorting
    public static class SortRankMapper extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            int tIdx1 = value.find("\t");
            int tIdx2 = value.find("\t", tIdx1 + 1);
            // extract tokens from the current line
            String page = Text.decode(value.getBytes(), 0, tIdx1);
            double pageRank = Double.parseDouble(Text.decode(value.getBytes(),
                    tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
            context.write(new Text(page), new DoubleWritable(pageRank));
        }
    }
    public static void main(String[] args) throws Exception {
        long start=System.currentTimeMillis();
        if (args.length >2) {
            //set the iteration numbers
                prank.ITERATIONS = Math.max(Integer.parseInt(args[0]), 1);
            //set input path
                prank.INPUT_PATH = prank.INPUT_PATH + args[1];
            //set output path
                prank.OUTPUT_PATH = prank.OUTPUT_PATH + args[2];
        } else {
            printHelp();
        }
        String inPath = null;
        String lastOutPath = null;
        prank pagerank = new prank();
        System.out.println("Start to fetch neighbor links ...");

        boolean isCompleted = pagerank.job("fetchNeighborLinks",
                FetchNeighborsMapper.class, FetchNeighborsReducer.class,
                INPUT_PATH, OUTPUT_PATH + "/iter00");
        if (!isCompleted) {
            System.exit(1);
        }
        for (int runs = 0; runs < ITERATIONS; runs++) {
            inPath = OUTPUT_PATH + "/iter" + NF.format(runs);
            lastOutPath = OUTPUT_PATH + "/iter" + NF.format(runs + 1);
            System.out.println("Start to calculate rank [" + (runs + 1) + "/"
                    + pagerank.ITERATIONS + "] ...");
            isCompleted = pagerank.job("jobOfCalculatingRanks",
                    CalculateRankMapper.class, CalculateRankReducer.class,
                    inPath, lastOutPath);
            if (!isCompleted) {
                System.exit(1);
            }
        }
        System.out.println("Start to sort ranks ...");
        isCompleted = pagerank.job("jobOfSortingRanks", SortRankMapper.class,
                SortRankMapper.class, lastOutPath, OUTPUT_PATH + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        double time=(System.currentTimeMillis()-start)/1000;
        System.out.println("All jobs done in: "+time);
        System.out.println("All jobs done!");
        System.exit(0);
    }
    public boolean job(String jobName, Class m, Class r, String in, String out)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(prank.class);
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        if (jobName.equals("jobOfSortingRanks")) {
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
        } else {
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
        }
        job.setMapperClass(m);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        if (jobName.equals("jobOfSortingRanks")) {
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
        } else {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setReducerClass(r);
        }
        return job.waitForCompletion(true);
    }
    //Print help message if the user does know how to run the program
    public static void printHelp() {
        System.out.println("Usage: PageRank.jar  <iterations>  <input file>  <output file> \n");
    }
}
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;

public class sort {
    public static String rootPath = "";
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // map mỗi từ với giá trị 1.
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    // tính tần suất xuất hiện của một từ duy nhất thông reduce.
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
    // construct a map with a composite key, such as ((hadoop,1),null);
    // Mapper thứ hai.
    public static class SecondaryMapper extends
            Mapper<IntWritable, Text, CompositeKey, Text> {
        private Text word = new Text();

        public void map(IntWritable value, Text key, Context context)
                throws IOException, InterruptedException {
            context.write(new CompositeKey((Text) key, value), word);
        }
    }
    // Implement một bộ so sánh để so sánh giữa hai số nguyên
    private static class IntWritableComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount for " + args[0]);
        // Lưu các kết quả trung gian tạm thời ra file.
        Path tempDir = new Path("temp_wc_" + System.currentTimeMillis());
        job.setJarByClass(sort.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(rootPath + args[0]));
        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Sắp xếp theo tần suất xuất hiện.
        if (job.waitForCompletion(true)) {
            Job job2 = new Job(conf, "sorted by frequency");
            job2.setJarByClass(sort.class);
            FileInputFormat.addInputPath(job2, tempDir);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setMapperClass(InverseMapper.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            job2.setSortComparatorClass(IntWritableComparator.class);
            FileSystem.get(conf).deleteOnExit(tempDir);
            tempDir = new Path("temp_wc_" + System.currentTimeMillis());
            FileOutputFormat.setOutputPath(job2, tempDir);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            // Sắp xếp theo thứ tự từ
            if (job2.waitForCompletion(true)) {
                Job job3 = new Job(conf, "sorted by word");
                job3.setJarByClass(sort.class);
                FileInputFormat.addInputPath(job3, tempDir);
                job3.setInputFormatClass(SequenceFileInputFormat.class);
                job3.setMapperClass(SecondaryMapper.class);
                // set parameters for the job3, such as paritioner and
                // comparator
                job3.setMapOutputKeyClass(CompositeKey.class);
//                job3.setPartitionerClass(KeyPartitioner.class);
                job3.setSortComparatorClass(CompositeKeyComparator.class);
                job3.setGroupingComparatorClass(KeyGroupingComparator.class);
                FileOutputFormat.setOutputPath(job3, new Path(rootPath + args[1]));
                job3.setOutputKeyClass(IntWritable.class);
                job3.setOutputValueClass(Text.class);
                System.exit(job3.waitForCompletion(true) ? 0 : 1);
            }
        }
        FileSystem.get(conf).deleteOnExit(tempDir);
    }
}
// Nhóm các từ lại với nhau theo thứ tự tần suất xuất hiện.
class KeyGroupingComparator extends WritableComparator {
    protected KeyGroupingComparator() {
        super(CompositeKey.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        return key2.getFrequency().compareTo(key1.getFrequency());
    }
}
// So sánh giữa các Key tổng hợp.
class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        int cmp = key2.getFrequency().compareTo(key1.getFrequency());
        if (cmp != 0)
            return cmp;
        return key1.getWord().compareTo(key2.getWord());
    }
}
// Xây dựng một lớp key tổng hợp.
class CompositeKey implements WritableComparable<CompositeKey> {
    private Text word;
    private IntWritable frequency;

    public CompositeKey() {
        set(new Text(), new IntWritable());
    }

    public CompositeKey(String word, int frequency) {

        set(new Text(word), new IntWritable(frequency));
    }

    public CompositeKey(Text w, IntWritable f) {
        set(w, f);
    }

    public void set(Text t, IntWritable n) {
        this.word = t;
        this.frequency = n;
    }

    @Override
    public String toString() {
        return (new StringBuilder()).append(frequency).append(' ').append(word)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CompositeKey) {
            CompositeKey comp = (CompositeKey) o;
            return word.equals(comp.word) && frequency.equals(comp.frequency);
        }
        return false;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        frequency.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        frequency.write(out);
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = word.compareTo(o.word);
        if (result != 0) {
            return result;
        }
        return result = frequency.compareTo(o.frequency);
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getFrequency() {
        return frequency;
    }
}

// partitioned by frequency
class KeyPartitioner extends Partitioner<CompositeKey, Text> {
    HashPartitioner<IntWritable, Text> hashPartitioner =
            new HashPartitioner<IntWritable, Text>();
    IntWritable newKey = new IntWritable();
    @Override
    public int getPartition(CompositeKey key, Text value, int numReduceTasks) {
        try {
            return hashPartitioner.getPartition(key.getFrequency(), value,
                    numReduceTasks);
        } catch (Exception e) {
            e.printStackTrace();
            return (int) (Math.random() * numReduceTasks);
        }
    }
}
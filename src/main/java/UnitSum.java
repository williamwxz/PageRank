import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // need a mapper to read file from hdfs and split
            //input format: toPage\t unitMultiplication
            //target: pass to reducer
            String[] data = value.toString().trim().split("\\t");
            context.write(new Text(data[0]), new DoubleWritable(Double.parseDouble(data[1])));
        }
    }

    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        // read current page rank
        private float beta;
        private static final float DEFAULT_BETA = 0.2f;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", DEFAULT_BETA);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] subPageRank = value.toString().trim().split("\\t");
            double val = Double.parseDouble(subPageRank[1])*beta;
            context.write(new Text(subPageRank[0]), new DoubleWritable(val));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

           //input key = toPage value = <unitMultiplication>
            //target: sum!
            double sum = 0;
            for (DoubleWritable value:values){
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // input:   transition matrix | current page rank | next page rank | beta
        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

//        job.setMapperClass(PassMapper.class);
        ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        ChainMapper.addMapper(job, BetaMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);

//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}

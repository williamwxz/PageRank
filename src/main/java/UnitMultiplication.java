import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String[] webpages = value.toString().trim().split("\\t");
            // please consider corner case, dead end webpage
            if (webpages.length<=1){
                return;
            }
            String fromPage = webpages[0];
            String[] toPages = webpages[1].split(",");
            int n = toPages.length;
            for (String to:toPages){
                String outputVal = to+"="+(double)1/n;
                context.write(new Text(fromPage), new Text(outputVal));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] pageranks = value.toString().trim().split("\\t");
            String id = pageranks[0];
            String weight = pageranks[1];
            context.write(new Text(id), new Text(weight));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            //List<String> transitionCell
            double pagerankCell=0;
            List<String> transitionCells = new ArrayList<String>();
            for (Text value:values){
                if (value.toString().contains("=")){
                    //from mapper 1
                    transitionCells.add(value.toString());
                }else{
                    pagerankCell = Double.parseDouble(value.toString());
                }
            }
            for (String cell: transitionCells){
                String[] id_probs = cell.split("=");
                String toPage = id_probs[0];
                double prob = Double.parseDouble(id_probs[1]);
                double multipliedVal = prob*pagerankCell;
                context.write(new Text(toPage), new Text(Double.toString(multipliedVal)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // take different inputs
        // configuration | input file path | input file format | mapper class
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}

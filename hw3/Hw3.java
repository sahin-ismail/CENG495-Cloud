import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "main");

        job.setJarByClass(Hw3.class);

        
        if (args[0].equals("tot")) {
            job.setMapperClass(TotalDuration.TokenizerMapper.class);
            job.setCombinerClass(TotalDuration.IntSumReducer.class);
            job.setReducerClass(TotalDuration.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);            
        }
        else if (args[0].equals("city")) {
            job.setMapperClass(TotalCity.TokenizerMapper.class);
            job.setCombinerClass(TotalCity.IntSumReducer.class);
            job.setReducerClass(TotalCity.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);            
        }
        else if (args[0].equals("avg")) {
            job.setMapperClass(TotalAvg.TokenizerMapper.class);
            job.setReducerClass(TotalAvg.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);            
        }
        else if (args[0].equals("sep")) {
            job.setNumReduceTasks(4);
            job.setMapperClass(DurationSeparate.TokenizerMapper.class);
            job.setPartitionerClass(DurationSeparate.AvgPartitioner.class);
            job.setReducerClass(DurationSeparate.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);          
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
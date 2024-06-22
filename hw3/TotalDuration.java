import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalDuration {

    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text city1 = new Text();
    private Text city2 = new Text();
    private Text result = new Text();
    private Text duration = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      

      while (itr.hasMoreTokens()) {
          city1.set(itr.nextToken());
          city2.set(itr.nextToken());
          duration.set(itr.nextToken());
          result.set("a");
          IntWritable durationWritable = new IntWritable(Integer.parseInt(duration.toString())); 
          context.write(result, durationWritable);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Text total = new Text("total_count");

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(total, result);
    }
  }
        
}
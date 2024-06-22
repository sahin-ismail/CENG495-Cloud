import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalCity {

    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable> {

    	
    private final static IntWritable inc = new IntWritable(1);
    private Text city1 = new Text();
    private Text city2 = new Text();
    private Text duration = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
          city1.set(itr.nextToken());
          city2.set(itr.nextToken());
          duration.set(itr.nextToken());
          context.write(city1, inc);
          context.write(city2, inc);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
        
}
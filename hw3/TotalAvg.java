import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalAvg {

    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable> {
        private Text city1 = new Text();
        private Text city2 = new Text();
        private Text duration = new Text();
        private Text result = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
          city1.set(itr.nextToken());
          city2.set(itr.nextToken());
          duration.set(itr.nextToken());
          String res = "";
          if(city1.toString().compareTo(city2.toString()) < 0) {
        	  res = city1.toString() +"-"+ city2.toString();
          }else {
              res = city2.toString() +"-"+ city1.toString();
          }
          
          result.set(res);
        IntWritable durationRes = new IntWritable(Integer.parseInt(duration.toString())); 
        context.write(result, durationRes);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int city_count = 0;
      for (IntWritable val : values) {
        sum += val.get();
        
        city_count++;
      }

      double average_duration = (double) sum / (double) city_count;
      result.set(average_duration);
      context.write(key, result);
    }
  }
        
}
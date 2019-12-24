package hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WindSumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	  private IntWritable windAngleAverage = new IntWritable();

      // Ű(�̼����� ����)���� ���޵� ǳ���� Ƚ���� ���� ����
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int windAngleSum = 0;
          for (IntWritable val : values) {
        	  windAngleSum += val.get();
          }
          windAngleAverage.set(windAngleSum);
          context.write(key, windAngleAverage);
      }
}

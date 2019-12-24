package hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HdfsFile {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {// ����� �Ű������� �ʿ� ������ �ٸ��ٸ� 
			System.err.println("��� ���: HdfsFile <InputFile> <OutputFile>");
			System.exit(2); // ���α׷� ����
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(HdfsFile.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(WindSumReduce.class);
		job.setReducerClass(WindSumReduce.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));  // �Է� ������ġ
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // ��� ������ġ 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
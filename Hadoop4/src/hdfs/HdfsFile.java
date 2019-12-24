package hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HdfsFile {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {// 명령형 매개변수의 필요 갯수가 다르다면 
			System.err.println("사용 방법: HdfsFile <InputFile> <OutputFile>");
			System.exit(2); // 프로그램 종료
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(HdfsFile.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(WindSumReduce.class);
		job.setReducerClass(WindSumReduce.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));  // 입력 파일위치
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 출력 파일위치 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
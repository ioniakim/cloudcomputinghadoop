package sds.hadoop.ch03.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		if(2 != args.length ){
			System.err.println("Usage: WordCount <input> <output>");
			return;
		}
		
		Job job = new Job(conf, "WordCount"); // Map Reduce Job 실행을 위해 Job 객체 생성
		job.setJarByClass(WordCount.class); // MapReduce Job에서 사용할 사용자의 라이브러리 파일을 지정
		job.setMapperClass(WordCountMapper.class); // Mapper class 지정
		job.setReducerClass(WordCountReducer.class); // Reducer class 지정
		job.setInputFormatClass(TextInputFormat.class); // 텍스트 파일을 입력 포맷 지정. key=line no, value=line, 라인단위로 읽어옴 
		job.setOutputFormatClass(TextOutputFormat.class); // 텍스트 파일을 출력 포맷 지정. key와 value를 탭으로 구분해서 라인단위로 작성 
		job.setOutputKeyClass(Text.class); // Output의 key 타입 지정
		job.setOutputValueClass(IntWritable.class); // Output의 value 타입 지정
		FileInputFormat.addInputPath(job, new Path(args[0])); // 입력 파라미터 경로 지정. 디렉토리 지정 가능
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // 출력 파라미터 경로 지정
		job.waitForCompletion(true); // Job 실행

	}

}

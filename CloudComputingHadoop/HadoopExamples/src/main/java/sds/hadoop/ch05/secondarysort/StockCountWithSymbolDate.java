package sds.hadoop.ch05.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockCountWithSymbolDate extends Configured implements Tool {
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new StockCountWithSymbolDate(),args);
		System.out.printf( String.format("### Result: ",res));

			
	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(),arg0).getRemainingArgs();
		// check path
		if(arg0.length != 2){
			System.err.println("Usage: StockRiseCount <input> <output>");
			System.exit(-1);
		}
		
		Job job = new Job(getConf(), "StockCountWithSymbolDate");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(StockCountWithSymbolDate.class);
		job.setMapperClass(StockCountMapperWithSymbolDate.class);
		job.setReducerClass(StockCountReducerWithSymbolDate.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(SymbolDateKey.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(SymbolPartitioner.class);
		job.setGroupingComparatorClass(SymbolGroupKeyComparator.class);
		job.setSortComparatorClass(SymbolDateKeyComparator.class);
		
		
		MultipleOutputs.addNamedOutput(job, "NASDAQ", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "NYSE", TextOutputFormat.class, Text.class, IntWritable.class);
		
		
		job.waitForCompletion(true);

		return 0;
	}

}

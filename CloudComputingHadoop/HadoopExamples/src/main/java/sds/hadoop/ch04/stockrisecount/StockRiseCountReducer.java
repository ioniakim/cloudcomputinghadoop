package sds.hadoop.ch04.stockrisecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockRiseCountReducer
	extends Reducer <Text, IntWritable, Text, IntWritable>{

	private IntWritable result = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context ctx)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable value : values){
			sum += value.get();
		}
		
		result.set(sum);
		ctx.write(key, result);
		
	}
}

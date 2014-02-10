package sds.hadoop.ch05.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class StockCountReducerWithSymbolDate 
	extends Reducer<SymbolDateKey, IntWritable, SymbolDateKey, IntWritable>{
	
	private MultipleOutputs<SymbolDateKey, IntWritable> mos;
	
	private SymbolDateKey outputKey = new SymbolDateKey();
	
	private IntWritable result = new IntWritable();
	
	private int callCount = 0;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<SymbolDateKey, IntWritable>(context);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
	

	@Override
	protected void reduce(SymbolDateKey key,
			Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {

		String[] columns = key.getSymbol().split(",");
		String exchange = columns[0];
		String symbol = columns[1];

		
		countBySymbolDate(key, values, exchange, symbol);
		
		
	}

	private void countBySymbolDate(SymbolDateKey key, Iterable<IntWritable> values,
			String exchange, String symbol) throws IOException,
			InterruptedException {
		Integer date = key.getDate();
		int sum = 0;
		for(IntWritable value : values){
			if(false == date.equals(key.getDate())){
				result.set(sum);
				outputKey.setSymbol(symbol);
				outputKey.setDate(date);
				mos.write(exchange, outputKey, result);
				sum = 0;
			}
			sum += value.get();
			date = key.getDate();
		}
	}

	private void countCall(SymbolDateKey key, String exchange, String symbol)
			throws IOException, InterruptedException {
		result.set(++callCount);
		outputKey.setSymbol(symbol);
		outputKey.setDate(key.getDate());
		mos.write(exchange, outputKey, result);
	}
	
}

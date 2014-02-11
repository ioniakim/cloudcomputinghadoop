package sds.hadoop.ch05.secondarysort.readfieldstest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCountMapperWithSymbolDate
	extends Mapper<LongWritable, Text, SymbolDateKey, IntWritable>{
	
	private final static IntWritable outputValue = new IntWritable(1);
	
	private SymbolDateKey outputKey = new SymbolDateKey();
	
	private String symbol;
	private Integer date;
	private float price;
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {

		if(key.get() == 0){
			return;
		}
		
		String[] columns = value.toString().split(",");
		if(columns == null || columns.length == 0){
			return;
		}
		
		try{
			parseColumns(columns);

			if(price > 0){
				outputKey.setSymbol(symbol);
				outputKey.setDate(date);
				context.write(outputKey, outputValue);
			} 

		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private void parseColumns(String[] columns) {
		
		symbol = columns[0] + "," + columns[1];

		price = Float.parseFloat(columns[6]) - Float.parseFloat(columns[3]);

		date = new Integer(columns[2].replaceAll("-", "").substring(0, 6));
	}

}

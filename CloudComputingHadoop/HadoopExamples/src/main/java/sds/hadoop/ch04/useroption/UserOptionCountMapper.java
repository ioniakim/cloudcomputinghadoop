package sds.hadoop.ch04.useroption;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserOptionCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private String workType;
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	
	/**
	 * only once executed
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		workType = context.getConfiguration().get("workType").toUpperCase();
	}
	

	/**
	 * filter in only the exchange data matching workType
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		if(key.get() == 0){
			return;
		}
		
		String[] columns = value.toString().split(",");
		if(columns != null && columns.length > 0){
			try{
				if(workType.equals(columns[0])){
					if(columns[0].equals("NASDAQ")){
						outputKey.set(columns[2].substring(0, 4));
						float risePct = Float.parseFloat(columns[6]) - Float.parseFloat(columns[3]);
						if(risePct > 0){
							context.write(outputKey, outputValue);
						}
					}
				}
			} catch (Exception e){
				e.printStackTrace();
			}
		}
	}

}

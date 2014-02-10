/**
 * 
 */
package sds.hadoop.ch03.writeread;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * @author ionia
 *
 */
public class SingleFileWriteRead {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if(args.length != 2){
			System.err.println("Usage: SingleFileWriteRead <filename> <contents>");
			System.exit(-1);
		}

		writeReadRemoteHdfs(args);
		
		
	}

	/**
	 * to access HDFS from a remote host.
	 * @param args
	 */
	private static void writeReadRemoteHdfs(final String[] args) {
		
		try{
			// log in hdfs as user 'hadoop' from a remote host.
			UserGroupInformation ugi =
					UserGroupInformation.createRemoteUser("hadoop");
			
			ugi.doAs(new PrivilegedAction<Void>() {

				@Override
				public Void run() {
					writeReadHdfs(args);
					
					return null;
				}
			});
			
		} catch(Exception e){
			
		}
		
	}

	/**
	 * write a text into a file in HDFS and read it. 
	 * @param args
	 */
	private static void writeReadHdfs(final String[] args) {
		Configuration conf = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		
			Path path = new Path(args[0]);
			if(hdfs.exists(path)){
				hdfs.delete(path, true);
			}
			
			FSDataOutputStream outStream = hdfs.create(path);
			outStream.writeUTF(args[1]);
			outStream.close();
			
			FSDataInputStream inputStream = hdfs.open(path);
			String inStr = inputStream.readUTF();
			inputStream.close();
			
			System.out.println("## inStr:" + inStr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

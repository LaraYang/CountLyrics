import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class Retrieve {
	public static void main(String[] args) {
		try {
			retrieveResult();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void retrieveResult() throws IOException {
		Writer writer = null;
		try {
		    writer = new BufferedWriter(new OutputStreamWriter(
		          new FileOutputStream("/home/lyang/Documents/FrequentWords.txt"), "utf-8"));
			writer.write("Most Frequent Words\n");
			Scan s = new Scan();
			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, "Words");
			ResultScanner sc = table.getScanner(s);
			for (Result rr = sc.next(); rr != null; rr = sc.next()) {
				byte[] row = rr.getRow();
				NavigableMap<byte[], byte[]> map = rr.getFamilyMap(Bytes.toBytes("Counts"));
				writer.write(String.format("%-20s",Bytes.toString(row)));
				for (Entry<byte[], byte[]> pair : map.entrySet()) {
					writer.write(String.format("%s: %d",Bytes.toString(pair.getKey()),
							 Bytes.toInt(pair.getValue()))+"\t");
				}
				writer.write("\n");
			}
			writer.close();
		} finally {
			
		}
	}
}

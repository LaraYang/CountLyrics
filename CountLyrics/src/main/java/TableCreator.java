import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class TableCreator {
	public static void main(String args[]) throws IOException {
		Configuration hc = HBaseConfiguration.create();
		HTableDescriptor ht = new HTableDescriptor("Words");
		ht.addFamily(new HColumnDescriptor("Counts"));
		HBaseAdmin hba = new HBaseAdmin(hc);
		System.out.println( "Creating Table" );
		hba.createTable(ht);
		System.out.println("Done...");
	}
}

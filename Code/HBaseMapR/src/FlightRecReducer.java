
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightRecReducer

extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values,

	Context context)

	throws IOException, InterruptedException {

		 Configuration config = HBaseConfiguration.create();
		 HTable table = new HTable(config, "FLIGHTREC");
		 Put p = new Put(Bytes.toBytes(key.toString()));
		// System.out.print(key.toString());
		 ArrayList<Text> s = new ArrayList<Text>();
		// Iterator i = 
		for (Text value : values) {

String line = value.toString();
System.out.print(line);
String[] ParsedLine=line.split(",");
	//	System.out.print(ParsedLine[0]+"yes");
		      p.add(Bytes.toBytes("DATE_OF_TRAVEL"), Bytes.toBytes(""),Bytes.toBytes(ParsedLine[0]));
		      p.add(Bytes.toBytes("Predicted_Ticket_Fare"), Bytes.toBytes(""),Bytes.toBytes(ParsedLine[1]));
		      p.add(Bytes.toBytes("Destin_Value"), Bytes.toBytes(""),Bytes.toBytes(ParsedLine[2]));
		      p.add(Bytes.toBytes("BaseFare"), Bytes.toBytes(""),Bytes.toBytes(ParsedLine[3]));
		      p.add(Bytes.toBytes("MONTH_OF_BOOKING"), Bytes.toBytes(""),Bytes.toBytes(ParsedLine[4]));
		      // Instantiating Get class
		     // Get g = new Get(Bytes.toBytes("5842"));
		      
		
		      table.put(p); 
//		System.out.print(s.get(4).toString());
		context.write(key, new Text(p.toString()));

	}
}
}


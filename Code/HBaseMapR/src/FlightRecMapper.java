
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;

public class FlightRecMapper extends Mapper<Object, Text, Text, Text> {

	private Text outKey =new Text();
	private Text outValue =new Text();
	Configuration config = HBaseConfiguration.create();

    // Instantiating HTable class
    
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			HTable table = new HTable(config, "t2");
			String line = value.toString();
			CSVReader R = new CSVReader(new StringReader(line));
			String[] ParsedLine = R.readNext();
		      
			R.close();
		      Put p = new Put(Bytes.toBytes(ParsedLine[0]));
			
			outKey.set(ParsedLine[0].replace(' ', '_'));
			outValue.set(ParsedLine[1].replace(' ', '_')+","+ParsedLine[2].replace(' ', '_')+","
			+ParsedLine[3].replace(' ', '_')+","+ParsedLine[4].replace(' ', '_')+","+ParsedLine[5].replace(' ', '_'));

		/*    p.add(Bytes.toBytes("pressure"), Bytes.toBytes("in"),Bytes.toBytes(ParsedLine[1]));
		      p.add(Bytes.toBytes("pressure"), Bytes.toBytes("out"),Bytes.toBytes(ParsedLine[2]));
		      p.add(Bytes.toBytes("temp"), Bytes.toBytes("in"),Bytes.toBytes(ParsedLine[3]));
		      p.add(Bytes.toBytes("temp"), Bytes.toBytes("in"),Bytes.toBytes(ParsedLine[4]));
		      p.add(Bytes.toBytes("vibration"), Bytes.toBytes("in"),Bytes.toBytes(ParsedLine[5]));
		      table.put(p); 
			*/context.write( outKey, outValue);
		} catch (Exception e) {
			System.out.println(e);
		}

	}

}

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "FLIGHTREC");
      Get g = new Get(Bytes.toBytes("100.0005815"));
     /* Put p = new Put(Bytes.toBytes("23454"));
      p.add(Bytes.toBytes("pressure"), Bytes.toBytes("in"),Bytes.toBytes("2340"));
      p.add(Bytes.toBytes("pressure"), Bytes.toBytes("out"),Bytes.toBytes("22"));
      p.add(Bytes.toBytes("temp"), Bytes.toBytes("in"),Bytes.toBytes("987"));
      p.add(Bytes.toBytes("temp"), Bytes.toBytes("in"),Bytes.toBytes("123"));
      p.add(Bytes.toBytes("vibration"), Bytes.toBytes("in"),Bytes.toBytes("987"));
      */// Instantiating Get class
   
   //   table.put(p); 
      // Reading the data
      Result result = table.get(g);
//System.out.print(p);
      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("DATE_OF_TRAVEL"),Bytes.toBytes(""));

      byte [] value1 = result.getValue(Bytes.toBytes("Predicted_Ticket_Fare"),Bytes.toBytes(""));

      // Printing the values
     String name = Bytes.toString(value);
      String city = Bytes.toString(value1);
      
      System.out.println("DATE_OF_TRAVEL: " + name + " Predicted_Ticket_Fare: " + city);
   }
}
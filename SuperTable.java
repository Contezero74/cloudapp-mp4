import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;

public class SuperTable {
   private static final Log LOG = LogFactory.getLog(SuperTable.class);

   private static final String TABLE_NAME = "powers";
   private static final String PERSONAL_CNAME = "personal";
   private static final String PERSONAL_HERO_CNAME = "hero";
   private static final String PERSONAL_POWER_CNAME = "power";
   private static final String PROFESSIONAL_CNAME = "professional";
   private static final String PROFESSIONAL_NAME_CNAME = "name";
   private static final String PROFESSIONAL_XP_CNAME = "xp";

   private static Put createHBaseEntry(final String id,
				       final String hero,
				       final String power,
				       final String name,
				       final String xp) {
	final Put r = new Put(Bytes.toBytes(id));
	r.add(Bytes.toBytes(PERSONAL_CNAME), Bytes.toBytes(PERSONAL_HERO_CNAME), Bytes.toBytes(hero));
	r.add(Bytes.toBytes(PERSONAL_CNAME), Bytes.toBytes(PERSONAL_POWER_CNAME), Bytes.toBytes(power));
	r.add(Bytes.toBytes(PROFESSIONAL_CNAME), Bytes.toBytes(PROFESSIONAL_NAME_CNAME), Bytes.toBytes(name));
	r.add(Bytes.toBytes(PROFESSIONAL_CNAME), Bytes.toBytes(PROFESSIONAL_XP_CNAME), Bytes.toBytes(xp));

	LOG.info("Row " + id + " has been created");	

	return r;
   }

   public static void main(String[] args) throws IOException {

	// Instantiate Configuration class
	final Configuration config = HBaseConfiguration.create();

	// Instaniate HBaseAdmin class
	final HBaseAdmin admin = new HBaseAdmin(config);

	// Instantiate table descriptor class
	final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

	// Add column families to table descriptor
	tableDescriptor.addFamily(new HColumnDescriptor(PERSONAL_CNAME));
	tableDescriptor.addFamily(new HColumnDescriptor(PROFESSIONAL_CNAME));

	// Execute the table through admin
	admin.createTable(tableDescriptor);
	LOG.info("Table " + TABLE_NAME + " has been created");

	// Instantiating HTable class
	final HTable powers = new HTable(config, TABLE_NAME);

	// Repeat these steps as many times as necessary
	// 1. Instantiating Put class (Hint: Accepts a row name)
	// 2. Add values using add() method (Hints: Accepts column family name, qualifier/row name ,value)
	final Put r1 = createHBaseEntry("row1", "superman", "strength", "clark", "100");
	final Put r2 = createHBaseEntry("row2", "batman", "money", "bruce", "50");
	final Put r3 = createHBaseEntry("row3", "wolverine", "healing", "logan", "75");

	// Save the table
	powers.put(r1);
	powers.put(r2);
	powers.put(r3);
	LOG.info("Table " + TABLE_NAME + " has been saved (row1, row2, and row3)");
	
	// Close table
	powers.close();

	// Instantiate the Scan class
	final HTable herosScanTable = new HTable(config, TABLE_NAME);
	final Scan herosScan  = new Scan();

	// Scan the required columns
	herosScan.addColumn(Bytes.toBytes(PERSONAL_CNAME), Bytes.toBytes(PERSONAL_HERO_CNAME));

	// Get the scan result
	final ResultScanner scanner = herosScanTable.getScanner(herosScan);

	// Read values from scan result and print scan result
	for (Result hero = scanner.next(); hero != null; hero = scanner.next()) {
		System.out.println(hero);
	}


	// Close the scanner
	scanner.close();
   
	// Htable closer
	herosScanTable.close();
   }
}


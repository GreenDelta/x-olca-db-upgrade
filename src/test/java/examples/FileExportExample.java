package examples;

import java.io.File;

import org.openlca.xdb.upgrade.FileExport;
import org.openlca.xdb.upgrade.IDatabase;
import org.openlca.xdb.upgrade.MySQLDatabase;

public class FileExportExample {

	public static void main(String[] args) {
		try {
			String oldDbUrl = "jdbc:mysql://localhost:3306/openlca4teacher_new";
			IDatabase oldDb = new MySQLDatabase(oldDbUrl, "root", "");
			File exportFile = new File("C:/Users/Besitzer/Desktop/openlca4teacher_new.zolca");
			FileExport fileExport = new FileExport(oldDb, exportFile);
			fileExport.run();
			oldDb.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

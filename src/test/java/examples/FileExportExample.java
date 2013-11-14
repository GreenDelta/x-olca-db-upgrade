package examples;

import java.io.File;

import org.openlca.xdb.upgrade.FileExport;
import org.openlca.xdb.upgrade.OldDatabase;

public class FileExportExample {

	public static void main(String[] args) {

		try {

			String oldDbUrl = "jdbc:mysql://localhost:3306/epa_db";
			OldDatabase oldDb = new OldDatabase(oldDbUrl, "root", "");
			File exportFile = new File("C:/Users/Besitzer/Desktop/epa_db.zolca");
			FileExport fileExport = new FileExport(oldDb, exportFile);
			fileExport.run();
			oldDb.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

package examples;

import org.openlca.xdb.upgrade.IDatabase;
import org.openlca.xdb.upgrade.MySQLDatabase;
import org.openlca.xdb.upgrade.Update;

public class MySqlUpdateExample {

	public static void main(String[] args) {
		try {
			String oldDbUrl = "jdbc:mysql://localhost:3306/openlca4teacher_new";
			IDatabase oldDb = new MySQLDatabase(oldDbUrl, "root", "");
			String newDbUrl = "jdbc:mysql://localhost:3306/openlca_tmp";
			IDatabase newDb = new MySQLDatabase(newDbUrl, "root", "");
			Update update = new Update(oldDb, newDb);
			update.run();
			oldDb.close();
			newDb.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

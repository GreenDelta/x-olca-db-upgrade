package examples;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.mysql.MySQLDatabase;
import org.openlca.xdb.upgrade.OldDatabase;
import org.openlca.xdb.upgrade.Update;

public class MySqlUpdateExample {

	public static void main(String[] args) {
		try {
			String oldDbUrl = "jdbc:mysql://localhost:3306/epa_db";
			OldDatabase oldDb = new OldDatabase(oldDbUrl, "root", "");
			String newDbUrl = "jdbc:mysql://localhost:3306/epa_db_new";
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

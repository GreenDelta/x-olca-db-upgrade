package org.openlca.xdb.upgrade;

import org.openlca.core.database.IDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Update implements Runnable {

	private Logger log = LoggerFactory.getLogger(getClass());
	private OldDatabase oldDb;
	private IDatabase newDb;

	public Update(OldDatabase oldDb, IDatabase newDb) {
		this.oldDb = oldDb;
		this.newDb = newDb;
	}

	@Override
	public void run() {
		try {
			log.trace("run update");
			Sequence index = new Sequence();
			Category.map(oldDb, newDb, index);
			Unit.map(oldDb, newDb, index);
			UnitGroup.map(oldDb, newDb, index);
			FlowProperty.map(oldDb, newDb, index);
			Location.map(oldDb, newDb, index);
			index.write(newDb);
		} catch (Exception e) {
			log.error("update failed", e);
			throw new RuntimeException("Update failed", e);
		}
	}

}

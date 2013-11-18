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
			Sequence seq = new Sequence();
			Category.map(oldDb, newDb, seq);
			Unit.map(oldDb, newDb, seq);
			UnitGroup.map(oldDb, newDb, seq);
			FlowProperty.map(oldDb, newDb, seq);
			Location.map(oldDb, newDb, seq);
			Flow.map(oldDb, newDb, seq);
			FlowPropertyFactor.map(oldDb, newDb, seq);
			Actor.map(oldDb, newDb, seq);
			Source.map(oldDb, newDb, seq);
			Exchange.map(oldDb, newDb, seq);
			Process.map(oldDb, newDb, seq);
			ProcessDoc.map(oldDb, newDb, seq);
			ProductSystem.map(oldDb, newDb, seq);
			ProcessLink.map(oldDb, newDb, seq);
			ProductSystemProcess.map(oldDb, newDb, seq);
			ImpactMethod.map(oldDb, newDb, seq);
			ImpactCategory.map(oldDb, newDb, seq);
			ImpactFactor.map(oldDb, newDb, seq);
			Parameter.map(oldDb, newDb, seq);
			seq.write(newDb);
		} catch (Exception e) {
			log.error("update failed", e);
			throw new RuntimeException("Update failed", e);
		}
	}

}

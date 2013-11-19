package org.openlca.xdb.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.EventBus;

public class Update implements Runnable {

	private Logger log = LoggerFactory.getLogger(getClass());
	private OldDatabase oldDb;
	private IDatabase newDb;
	private EventBus eventBus;

	public Update(OldDatabase oldDb, IDatabase newDb) {
		this.oldDb = oldDb;
		this.newDb = newDb;
	}

	public void setEventBus(EventBus eventBus) {
		this.eventBus = eventBus;
	}

	@Override
	public void run() {
		try {

			log.trace("run update");
			Sequence seq = new Sequence();

			log(0, "Map categories");
			Category.map(oldDb, newDb, seq);

			log(5, "Map unit groups");
			Unit.map(oldDb, newDb, seq);
			UnitGroup.map(oldDb, newDb, seq);

			log(10, "Map flow properties");
			FlowProperty.map(oldDb, newDb, seq);

			log(15, "Map locations");
			Location.map(oldDb, newDb, seq);

			log(20, "Map flows");
			Flow.map(oldDb, newDb, seq);
			FlowPropertyFactor.map(oldDb, newDb, seq);

			log(30, "Map actors and sources");
			Actor.map(oldDb, newDb, seq);
			Source.map(oldDb, newDb, seq);

			log(35, "Map processes");
			Exchange.map(oldDb, newDb, seq);
			Process.map(oldDb, newDb, seq);
			ProcessDoc.map(oldDb, newDb, seq);
			ProcessSource.map(oldDb, newDb, seq);

			log(55, "Map product systems");
			ProductSystem.map(oldDb, newDb, seq);
			ProcessLink.map(oldDb, newDb, seq);
			ProductSystemProcess.map(oldDb, newDb, seq);

			log(65, "Map LCIA methods");
			ImpactMethod.map(oldDb, newDb, seq);
			ImpactCategory.map(oldDb, newDb, seq);
			ImpactFactor.map(oldDb, newDb, seq);
			NormalizationWeightingSet.map(oldDb, newDb, seq);
			NormalizationWeightingFactor.map(oldDb, newDb, seq);

			log(75, "Map parameters");
			Parameter.map(oldDb, newDb, seq);

			log(80, "Map projects");
			Project.map(oldDb, newDb, seq);
			ProjectVariant.map(oldDb, newDb, seq);

			log(85, "Finish import");
			seq.write(newDb);
		} catch (Exception e) {
			log.error("update failed", e);
			throw new RuntimeException("Update failed", e);
		}
	}

	private void log(int finishedWork, String nextTask) {
		log.trace("{}% done; nextTask={}", finishedWork, nextTask);
		if (eventBus != null)
			eventBus.post(new ProgressEvent(nextTask, finishedWork));
	}

}

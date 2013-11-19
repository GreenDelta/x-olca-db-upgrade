package org.openlca.xdb.upgrade;

public class ProgressEvent {

	private String nextTask;
	private int finishedWork;

	public ProgressEvent(String nextTask, int finishedWork) {
		this.nextTask = nextTask;
		this.finishedWork = finishedWork;
	}

	/** The name of the next task */
	public String getNextTask() {
		return nextTask;
	}

	/**
	 * The amount of total work that is done which is a value between 0 and 100.
	 */
	public int getFinishedWork() {
		return finishedWork;
	}
}

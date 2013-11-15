package org.openlca.xdb.upgrade;

import java.util.Arrays;
import java.util.HashMap;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class Sequence {

	public static final int CATEGORY = 0;
	public static final int UNIT = 1;
	public static final int UNIT_GROUP = 2;
	public static final int FLOW_PROPERTY = 3;
	public static final int LOCATION = 4;
	public static final int FLOW = 5;
	public static final int FLOW_PROPERTY_FACTOR = 6;
	public static final int ACTOR = 7;
	public static final int SOURCE = 8;
	public static final int EXCHANGE = 9;
	public static final int PROCESS = 10;
	public static final int PRODUCT_SYSTEM = 11;

	private final HashMap<String, Integer>[] sequences;
	private int seqCount = 0;

	@SuppressWarnings("unchecked")
	public Sequence() {
		sequences = new HashMap[12];
		for (int i = 0; i < sequences.length; i++)
			sequences[i] = new HashMap<>();
	}

	/**
	 * Get the allocated integer id for the given reference ID. If there is no
	 * such ID a new one is allocated.
	 */
	public int get(int type, String refId) {
		if (refId == null)
			return next();
		HashMap<String, Integer> map = sequences[type];
		Integer i = map.get(refId);
		if (i != null)
			return i;
		seqCount++;
		map.put(refId, seqCount);
		return seqCount;
	}

	public int next() {
		return ++seqCount;
	}

	public void write(IDatabase database) throws Exception {
		String sql = "UPDATE sequence SET SEQ_COUNT = " + next();
		NativeSql.on(database).batchUpdate(Arrays.asList(sql));
	}

}

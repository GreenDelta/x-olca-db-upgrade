package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.xdb.upgrade.NativeSql.BatchInsertHandler;

abstract class AbstractInsertHandler<T> implements BatchInsertHandler {

	private List<T> elements;
	protected Sequence seq;

	public AbstractInsertHandler(List<T> elements, Sequence sequence) {
		this.elements = elements;
		this.seq = sequence;
	}

	@Override
	public boolean addBatch(int i, PreparedStatement stmt) throws SQLException {
		T element = elements.get(i);
		map(element, stmt);
		return true;
	}

	protected abstract void map(T element, PreparedStatement stmt)
			throws SQLException;

}

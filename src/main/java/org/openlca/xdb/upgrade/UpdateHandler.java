package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.xdb.upgrade.NativeSql.BatchInsertHandler;

abstract class UpdateHandler<T> implements BatchInsertHandler {

	private List<T> elements;
	protected Sequence seq;

	public UpdateHandler(Sequence sequence) {
		this.seq = sequence;
	}

	public abstract String getStatement();

	public void nextBatch(List<T> elements) {
		this.elements = elements;
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

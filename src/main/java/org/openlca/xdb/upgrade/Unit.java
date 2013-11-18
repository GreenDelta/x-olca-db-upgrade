package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.xdb.upgrade.NativeSql.BatchInsertHandler;

class Unit {

	@DbField("id")
	private String refId;

	@DbField("name")
	private String name;

	@DbField("description")
	private String description;

	@DbField("conversionfactor")
	private double conversionFactor;

	@DbField("synonyms")
	private String synonyms;

	@DbField("f_unitgroup")
	private String unitGroupId;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_units";
		Mapper<Unit> mapper = new Mapper<>(Unit.class);
		List<Unit> units = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_units(id, ref_id, conversion_factor, "
				+ "description, name, synonyms, f_unit_group) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
		InsertHandler handler = new InsertHandler(units, index);
		NativeSql.on(newDb).batchInsert(insertStmt, units.size(), handler);
	}

	private static class InsertHandler implements BatchInsertHandler {

		private List<Unit> units;
		private Sequence index;

		public InsertHandler(List<Unit> units, Sequence index) {
			this.units = units;
			this.index = index;
		}

		@Override
		public boolean addBatch(int i, PreparedStatement stmt)
				throws SQLException {
			Unit unit = units.get(i);
			stmt.setInt(1, index.get(Sequence.UNIT, unit.refId));
			stmt.setString(2, unit.refId);
			stmt.setDouble(3, unit.conversionFactor);
			stmt.setString(4, unit.description);
			stmt.setString(5, unit.name);
			stmt.setString(6, unit.synonyms);
			stmt.setInt(7, index.get(Sequence.UNIT_GROUP, unit.unitGroupId));
			return true;
		}
	}
}

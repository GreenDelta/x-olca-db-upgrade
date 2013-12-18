package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_units";
		Mapper<Unit> mapper = new Mapper<>(Unit.class, oldDb, newDb);
		InsertHandler handler = new InsertHandler(index);
		mapper.mapAll(query, handler);
	}

	private static class InsertHandler extends UpdateHandler<Unit> {

		public InsertHandler(Sequence index) {
			super(index);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_units(id, ref_id, conversion_factor, "
					+ "description, name, synonyms, f_unit_group) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(Unit unit, PreparedStatement stmt) throws
				SQLException {
			stmt.setInt(1, seq.get(Sequence.UNIT, unit.refId));
			stmt.setString(2, unit.refId);
			stmt.setDouble(3, unit.conversionFactor);
			stmt.setString(4, unit.description);
			stmt.setString(5, unit.name);
			stmt.setString(6, unit.synonyms);
			stmt.setInt(7, seq.get(Sequence.UNIT_GROUP, unit.unitGroupId));
		}
	}
}

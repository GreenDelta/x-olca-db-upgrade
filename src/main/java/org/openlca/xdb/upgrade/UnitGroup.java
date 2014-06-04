package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

class UnitGroup {

	@DbField("id")
	private String refId;

	@DbField("description")
	private String description;

	@DbField("categoryid")
	private String categoryId;

	@DbField("name")
	private String name;

	@DbField("f_referenceunit")
	private String refUnitId;

	@DbField("f_defaultflowproperty")
	private String defaultPropId;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_unitgroups";
		Mapper<UnitGroup> mapper = new Mapper<>(UnitGroup.class, oldDb, newDb);
		InsertHandler handler = new InsertHandler(index);
		mapper.mapAll(query, handler);
	}

	private static class InsertHandler extends UpdateHandler<UnitGroup> {

		public InsertHandler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_unit_groups(id, ref_id, name, "
					+ "f_category, description, f_reference_unit, "
					+ "f_default_flow_property, last_change, version) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(UnitGroup group, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.UNIT_GROUP, group.refId));
			stmt.setString(2, group.refId);
			stmt.setString(3, group.name);
			if (Category.isNull(group.categoryId))
				stmt.setNull(4, Types.INTEGER);
			else
				stmt.setInt(4, seq.get(Sequence.CATEGORY, group.categoryId));
			stmt.setString(5, group.description);
			stmt.setInt(6, seq.get(Sequence.UNIT, group.refUnitId));
			if (group.defaultPropId == null)
				stmt.setNull(7, Types.INTEGER);
			else
				stmt.setInt(7,
						seq.get(Sequence.FLOW_PROPERTY, group.defaultPropId));
			stmt.setLong(8, System.currentTimeMillis());
			stmt.setLong(9, 4294967296L);
		}

	}

}

package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

class FlowProperty {

	@DbField("id")
	private String refId;

	@DbField("flowpropertytype")
	private int type;

	@DbField("description")
	private String description;

	@DbField("unitgroupid")
	private String unitGroupId;

	@DbField("categoryid")
	private String categoryId;

	@DbField("name")
	private String name;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_flowproperties";
		Mapper<FlowProperty> mapper = new Mapper<>(FlowProperty.class, oldDb,
				newDb);
		Handler handler = new Handler(index);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<FlowProperty> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_flow_properties(id, ref_id, name, "
					+ "f_category, description, flow_property_type, f_unit_group,"
					+ "last_change, version) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(FlowProperty prop, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.FLOW_PROPERTY, prop.refId));
			stmt.setString(2, prop.refId);
			stmt.setString(3, prop.name);
			if (Category.isNull(prop.categoryId))
				stmt.setNull(4, Types.INTEGER);
			else
				stmt.setInt(4, seq.get(Sequence.CATEGORY, prop.categoryId));
			stmt.setString(5, prop.description);
			stmt.setInt(6, prop.type);
			stmt.setInt(7, seq.get(Sequence.UNIT_GROUP, prop.unitGroupId));
			stmt.setLong(8, System.currentTimeMillis());
			stmt.setLong(9, 4294967296L);
		}
	}

}

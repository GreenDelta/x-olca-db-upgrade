package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

class Flow {

	@DbField("id")
	private String refId;

	@DbField("flowtype")
	private int flowType;

	@DbField("description")
	private String description;

	@DbField("categoryid")
	private String categoryId;

	@DbField("name")
	private String name;

	@DbField("infrastructure_flow")
	private boolean infrastructureFlow;

	@DbField("cas_number")
	private String casNumber;

	@DbField("formula")
	private String formula;

	@DbField("f_reference_flow_property")
	private String refPropertyId;

	@DbField("f_location")
	private String locationId;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_flows";
		Mapper<Flow> mapper = new Mapper<>(Flow.class, oldDb, newDb);
		Handler handler = new Handler(index);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<Flow> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_flows(id, ref_id, name, f_category, "
					+ "description, flow_type, infrastructure_flow, cas_number, "
					+ "formula, f_reference_flow_property, f_location) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(Flow flow, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.FLOW, flow.refId));
			stmt.setString(2, flow.refId);
			stmt.setString(3, flow.name);
			if (Category.isNull(flow.categoryId))
				stmt.setNull(4, Types.INTEGER);
			else
				stmt.setInt(4, seq.get(Sequence.CATEGORY, flow.categoryId));
			stmt.setString(5, flow.description);
			stmt.setString(6, mapType(flow.flowType));
			stmt.setBoolean(7, flow.infrastructureFlow);
			stmt.setString(8, flow.casNumber);
			stmt.setString(9, flow.formula);
			stmt.setInt(10, seq.get(Sequence.FLOW_PROPERTY, flow.refPropertyId));
			if (flow.locationId == null)
				stmt.setNull(11, Types.INTEGER);
			else
				stmt.setInt(11, seq.get(Sequence.LOCATION, flow.locationId));
		}

		private String mapType(int flowType) {
			switch (flowType) {
				case 0:
					return "ELEMENTARY_FLOW";
				case 1:
					return "PRODUCT_FLOW";
				case 2:
					return "WASTE_FLOW";
				default:
					return "PRODUCT_FLOW";
			}
		}
	}

}

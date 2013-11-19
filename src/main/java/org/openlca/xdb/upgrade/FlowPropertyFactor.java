package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class FlowPropertyFactor {

	@DbField("id")
	private String id;

	@DbField("conversionfactor")
	private double conversionFactor;

	@DbField("f_flowproperty")
	private String propertyId;

	@DbField("f_flowinformation")
	private String flowId;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_flowpropertyfactors";
		Mapper<FlowPropertyFactor> mapper = new Mapper<>(
				FlowPropertyFactor.class);
		List<FlowPropertyFactor> props = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_flow_property_factors(id, "
				+ "conversion_factor, f_flow, f_flow_property) "
				+ "VALUES (?, ?, ?, ?)";
		Handler handler = new Handler(props, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, props.size(), handler);
	}

	private static class Handler extends
			AbstractInsertHandler<FlowPropertyFactor> {

		public Handler(List<FlowPropertyFactor> props, Sequence seq) {
			super(props, seq);
		}

		@Override
		protected void map(FlowPropertyFactor prop, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.FLOW_PROPERTY_FACTOR, prop.id));
			stmt.setDouble(2, prop.conversionFactor);
			stmt.setInt(3, seq.get(Sequence.FLOW, prop.flowId));
			stmt.setInt(4, seq.get(Sequence.FLOW_PROPERTY, prop.propertyId));
		}
	}

}

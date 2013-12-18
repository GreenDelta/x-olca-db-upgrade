package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
				FlowPropertyFactor.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends
			UpdateHandler<FlowPropertyFactor> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_flow_property_factors(id, "
					+ "conversion_factor, f_flow, f_flow_property) "
					+ "VALUES (?, ?, ?, ?)";
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

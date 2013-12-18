package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class AllocationFactor {

	@DbField("value")
	private double value;

	@DbField("f_owner")
	private String f_owner;

	@DbField("f_flow")
	private String f_flow;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT distinct a.value, e.f_owner, e.f_flow FROM "
				+ "tbl_allocationfactors a join tbl_exchanges e on a.productid = e.id";
		Mapper<AllocationFactor> mapper = new Mapper<>(
				AllocationFactor.class, oldDb, newDb);
		Handler ecoHandler = new Handler(seq, "ECONOMIC");
		Handler physHandler = new Handler(seq, "PHYSICAL");
		mapper.mapAll(query, ecoHandler);
		mapper.mapAll(query, physHandler);
	}

	private static class Handler extends
			UpdateHandler<AllocationFactor> {

		private String method;

		public Handler(Sequence seq, String method) {
			super(seq);
			this.method = method;
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_allocation_factors(id, "
					+ "allocation_type, value, f_process, f_product) "
					+ "VALUES (?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(AllocationFactor factor, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// allocation_type
			stmt.setString(2, method);
			// value
			stmt.setDouble(3, factor.value);
			// f_process
			stmt.setInt(4, seq.get(Sequence.PROCESS, factor.f_owner));
			// f_product
			stmt.setInt(5, seq.get(Sequence.FLOW, factor.f_flow));
		}
	}
}
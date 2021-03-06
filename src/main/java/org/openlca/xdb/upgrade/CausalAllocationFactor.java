package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class CausalAllocationFactor {

	@DbField("value")
	private double value;

	@DbField("f_exchange")
	private String f_exchange;

	@DbField("f_owner")
	private String f_owner;

	@DbField("f_flow")
	private String f_flow;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT a.value, a.f_exchange, e.f_owner, e.f_flow FROM "
				+ "tbl_allocationfactors a join tbl_exchanges e "
				+ "on a.productid = e.id";
		Mapper<CausalAllocationFactor> mapper = new Mapper<>(
				CausalAllocationFactor.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends
			UpdateHandler<CausalAllocationFactor> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_allocation_factors(id, "
					+ "allocation_type, value, f_process, f_product, f_exchange) "
					+ "VALUES (?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(CausalAllocationFactor factor, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// allocation_type
			stmt.setString(2, "CAUSAL");
			// value
			stmt.setDouble(3, factor.value);
			// f_process
			stmt.setInt(4, seq.get(Sequence.PROCESS, factor.f_owner));
			// f_product
			stmt.setInt(5, seq.get(Sequence.FLOW, factor.f_flow));
			// f_exchange
			stmt.setInt(6, seq.get(Sequence.EXCHANGE, factor.f_exchange));
		}
	}
}

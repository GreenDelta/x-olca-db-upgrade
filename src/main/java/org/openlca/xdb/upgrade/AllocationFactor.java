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
		Mapper<AllocationFactor> mapper = new Mapper<>(AllocationFactor.class);
		List<AllocationFactor> factors = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_allocation_factors(id, "
				+ "allocation_type, value, f_process, f_product) "
				+ "VALUES (?, ?, ?, ?, ?)";
		Handler ecoHandler = new Handler(factors, seq, "ECONOMIC");
		Handler physHandler = new Handler(factors, seq, "PHYSICAL");
		NativeSql.on(newDb).batchInsert(insertStmt, factors.size(), ecoHandler);
		NativeSql.on(newDb)
				.batchInsert(insertStmt, factors.size(), physHandler);
	}

	private static class Handler extends
			AbstractInsertHandler<AllocationFactor> {

		private String method;

		public Handler(List<AllocationFactor> factors, Sequence seq,
				String method) {
			super(factors, seq);
			this.method = method;
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
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class ProcessCostEntry {

	@DbField("id")
	private String id;

	@DbField("f_process")
	private String f_process;

	@DbField("f_exchange")
	private String f_exchange;

	@DbField("f_cost_category")
	private String f_cost_category;

	@DbField("amount")
	private double amount;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_product_cost_entries";
		Mapper<ProcessCostEntry> mapper = new Mapper<>(ProcessCostEntry
				.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends
			UpdateHandler<ProcessCostEntry> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_process_cost_entries(id, f_process, "
					+ "f_exchange, f_cost_category, amount) "
					+ "VALUES (?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(ProcessCostEntry cost, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// f_process
			stmt.setInt(2, seq.get(Sequence.PROCESS, cost.f_process));
			// f_exchange
			stmt.setInt(3, seq.get(Sequence.EXCHANGE, cost.f_exchange));
			// f_cost_category
			stmt.setInt(4,
					seq.get(Sequence.COST_CATEGORY, cost.f_cost_category));
			// amount
			stmt.setDouble(5, cost.amount);
		}
	}
}
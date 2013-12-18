package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class CostCategory {

	@DbField("id")
	private String id;

	@DbField("name")
	private String name;

	@DbField("description")
	private String description;

	@DbField("fix")
	private boolean fix;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_cost_categories";
		Mapper<CostCategory> mapper = new Mapper<>(CostCategory.class, oldDb,
				newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<CostCategory> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_cost_categories(id, name, "
					+ "description, fix) " + "VALUES (?, ?, ?, ?)";
		}

		@Override
		protected void map(CostCategory costCat, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.COST_CATEGORY, costCat.id));
			// name
			stmt.setString(2, costCat.name);
			// description
			stmt.setString(3, costCat.description);
			// fix
			stmt.setBoolean(4, costCat.fix);
		}
	}
}

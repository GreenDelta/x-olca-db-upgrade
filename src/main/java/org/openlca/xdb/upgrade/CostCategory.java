package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

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
		Mapper<CostCategory> mapper = new Mapper<>(CostCategory.class);
		List<CostCategory> costCats = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_cost_categories(id, name, "
				+ "description, fix) " + "VALUES (?, ?, ?, ?)";
		Handler handler = new Handler(costCats, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, costCats.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<CostCategory> {

		public Handler(List<CostCategory> costCats, Sequence seq) {
			super(costCats, seq);
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

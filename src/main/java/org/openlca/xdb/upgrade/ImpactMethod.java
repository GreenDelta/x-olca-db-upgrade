package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class ImpactMethod {

	@DbField("id")
	private String id;

	@DbField("description")
	private String description;

	@DbField("categoryid")
	private String categoryid;

	@DbField("name")
	private String name;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_lciamethods";
		Mapper<ImpactMethod> mapper = new Mapper<>(ImpactMethod.class);
		List<ImpactMethod> methods = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_impact_methods(id, ref_id, "
				+ "description, f_category, name) " + "VALUES (?, ?, ?, ?, ?)";
		Handler handler = new Handler(methods, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, methods.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<ImpactMethod> {

		public Handler(List<ImpactMethod> methods, Sequence seq) {
			super(methods, seq);
		}

		@Override
		protected void map(ImpactMethod method, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.IMPACT_METHOD, method.id));
			// ref_id
			stmt.setString(2, method.id);
			// description
			stmt.setString(3, method.description);
			// f_category
			if (Category.isNull(method.categoryid))
				stmt.setNull(4, java.sql.Types.INTEGER);
			else
				stmt.setInt(4, seq.get(Sequence.CATEGORY, method.categoryid));
			// name
			stmt.setString(5, method.name);
		}
	}
}

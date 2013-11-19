package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class ImpactCategory {

	@DbField("id")
	private String id;

	@DbField("description")
	private String description;

	@DbField("name")
	private String name;

	@DbField("referenceunit")
	private String referenceunit;

	@DbField("f_lciamethod")
	private String f_lciamethod;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_lciacategories";
		Mapper<ImpactCategory> mapper = new Mapper<>(ImpactCategory.class);
		List<ImpactCategory> cats = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_impact_categories(id, ref_id, "
				+ "description, name, reference_unit, f_impact_method) "
				+ "VALUES (?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(cats, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, cats.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<ImpactCategory> {

		public Handler(List<ImpactCategory> cats, Sequence seq) {
			super(cats, seq);
		}

		@Override
		protected void map(ImpactCategory cat, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.IMPACT_CATEGORY, cat.id));
			// ref_id
			stmt.setString(2, cat.id);
			// description
			stmt.setString(3, cat.description);
			// name
			stmt.setString(4, cat.name);
			// reference_unit
			stmt.setString(5, cat.referenceunit);
			// f_impact_method
			stmt.setInt(6, seq.get(Sequence.IMPACT_METHOD, cat.f_lciamethod));
		}
	}
}

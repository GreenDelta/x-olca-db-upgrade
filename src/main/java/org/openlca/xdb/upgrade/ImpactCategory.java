package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
		Mapper<ImpactCategory> mapper = new Mapper<>(ImpactCategory.class,
				oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<ImpactCategory> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_impact_categories(id, ref_id, "
					+ "description, name, reference_unit, f_impact_method) "
					+ "VALUES (?, ?, ?, ?, ?, ?)";
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

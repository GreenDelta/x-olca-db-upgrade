package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class Source {

	@DbField("id")
	private String id;

	@DbField("description")
	private String description;

	@DbField("categoryid")
	private String categoryid;

	@DbField("name")
	private String name;

	@DbField("year")
	private int year;

	@DbField("textreference")
	private String textreference;

	@DbField("doi")
	private String doi;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_sources";
		Mapper<Source> mapper = new Mapper<>(Source.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<Source> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_sources(id, ref_id, description, "
					+ "f_category, name, source_year, text_reference, doi) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(Source source, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.SOURCE, source.id));
			stmt.setString(2, source.id);
			stmt.setString(3, source.description);
			if (Category.isNull(source.categoryid))
				stmt.setNull(4, java.sql.Types.INTEGER);
			else
				stmt.setInt(4, seq.get(Sequence.CATEGORY, source.categoryid));
			stmt.setString(5, source.name);
			stmt.setInt(6, source.year);
			stmt.setString(7, source.textreference);
			stmt.setString(8, source.doi);
		}
	}
}

package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class ProcessSource {

	@DbField("f_modelingandvalidation")
	private String f_modelingandvalidation;

	@DbField("f_source")
	private String f_source;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_modelingandvalidation_source";
		Mapper<ProcessSource> mapper = new Mapper<>(ProcessSource.class,
				oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<ProcessSource> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_process_sources(f_process_doc, "
					+ "f_source) " + "VALUES (?, ?)";
		}

		@Override
		protected void map(ProcessSource source, PreparedStatement stmt)
				throws SQLException {
			// f_process_doc
			stmt.setInt(1,
					seq.get(Sequence.PROCESS, source.f_modelingandvalidation));
			// f_source
			stmt.setInt(2, seq.get(Sequence.SOURCE, source.f_source));
		}
	}
}
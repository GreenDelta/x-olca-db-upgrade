package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class ProcessSource {

	@DbField("f_modelingandvalidation")
	private String f_modelingandvalidation;

	@DbField("f_source")
	private String f_source;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_modelingandvalidation_source";
		Mapper<ProcessSource> mapper = new Mapper<>(ProcessSource.class);
		List<ProcessSource> sources = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_process_sources(f_process_doc, "
				+ "f_source) " + "VALUES (?, ?)";
		Handler handler = new Handler(sources, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, sources.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<ProcessSource> {

		public Handler(List<ProcessSource> sources, Sequence seq) {
			super(sources, seq);
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
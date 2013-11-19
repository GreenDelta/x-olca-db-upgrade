package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** Note that this is an update on an existing process-doc table. */
class Technology {

	@DbField("id")
	private String id;

	@DbField("description")
	private String description;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_technologies";
		Mapper<Technology> mapper = new Mapper<>(Technology.class);
		List<Technology> techs = mapper.mapAll(oldDb, query);
		String insertStmt = "UPDATE tbl_process_docs SET technology = ? "
				+ " WHERE ID = ?";
		Handler handler = new Handler(techs, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, techs.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<Technology> {

		public Handler(List<Technology> techs, Sequence seq) {
			super(techs, seq);
		}

		@Override
		protected void map(Technology tech, PreparedStatement stmt)
				throws SQLException {
			stmt.setString(1, tech.description);
			stmt.setInt(2, seq.get(Sequence.PROCESS, tech.id));
		}
	}
}
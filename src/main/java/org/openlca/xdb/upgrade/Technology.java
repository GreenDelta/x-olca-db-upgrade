package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Note that this is an update on an existing process-doc table.
 */
class Technology {

	@DbField("id")
	private String id;

	@DbField("description")
	private String description;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_technologies";
		Mapper<Technology> mapper = new Mapper<>(Technology.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<Technology> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "UPDATE tbl_process_docs SET technology = ? "
					+ " WHERE ID = ?";
		}

		@Override
		protected void map(Technology tech, PreparedStatement stmt)
				throws SQLException {
			stmt.setString(1, tech.description);
			stmt.setInt(2, seq.get(Sequence.PROCESS, tech.id));
		}
	}
}
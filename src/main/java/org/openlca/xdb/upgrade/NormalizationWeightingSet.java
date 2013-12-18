package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class NormalizationWeightingSet {

	@DbField("id")
	private String id;

	@DbField("referencesystem")
	private String referencesystem;

	@DbField("f_lciamethod")
	private String f_lciamethod;

	@DbField("unit")
	private String unit;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_normalizationweightingsets";
		Mapper<NormalizationWeightingSet> mapper = new Mapper<>(
				NormalizationWeightingSet.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends
			UpdateHandler<NormalizationWeightingSet> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_normalisation_weighting_sets(id, "
					+ "reference_system, f_impact_method, unit) "
					+ "VALUES (?, ?, ?, ?)";
		}

		@Override
		protected void map(NormalizationWeightingSet nwSet,
		                   PreparedStatement stmt) throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.NW_SET, nwSet.id));
			// reference_system
			stmt.setString(2, nwSet.referencesystem);
			// f_impact_method
			stmt.setInt(3, seq.get(Sequence.IMPACT_METHOD, nwSet.f_lciamethod));
			// unit
			stmt.setString(4, nwSet.unit);
		}
	}
}
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class NormalizationWeightingFactor {

	@DbField("id")
	private String id;

	@DbField("weightingfactor")
	private Double weightingfactor;

	@DbField("normalizationfactor")
	private Double normalizationfactor;

	@DbField("f_lciacategory")
	private String f_lciacategory;

	@DbField("f_normalizationweightingset")
	private String f_normalizationweightingset;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_normalizationweightingfactors";
		Mapper<NormalizationWeightingFactor> mapper = new Mapper<>(
				NormalizationWeightingFactor.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends
			UpdateHandler<NormalizationWeightingFactor> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_nw_factors(id, "
					+ "weighting_factor, normalisation_factor, f_impact_category, "
					+ "f_nw_set) " + "VALUES (?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(NormalizationWeightingFactor factor,
				PreparedStatement stmt) throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// weighting_factor
			if (factor.weightingfactor == null)
				stmt.setNull(2, java.sql.Types.DOUBLE);
			else
				stmt.setDouble(2, factor.weightingfactor);
			// normalisation_factor
			if (factor.normalizationfactor == null)
				stmt.setNull(3, java.sql.Types.DOUBLE);
			else
				stmt.setDouble(3, factor.normalizationfactor);
			// f_impact_category
			stmt.setInt(4,
					seq.get(Sequence.IMPACT_CATEGORY, factor.f_lciacategory));
			// f_normalisation_weighting_set
			stmt.setInt(5, seq.get(Sequence.NW_SET,
					factor.f_normalizationweightingset));
		}
	}
}

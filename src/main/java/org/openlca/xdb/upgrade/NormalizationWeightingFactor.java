package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

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
				NormalizationWeightingFactor.class);
		List<NormalizationWeightingFactor> factors = mapper
				.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_normalisation_weighting_factors(id, "
				+ "weighting_factor, normalisation_factor, f_impact_category, "
				+ "f_normalisation_weighting_set) " + "VALUES (?, ?, ?, ?, ?)";
		Handler handler = new Handler(factors, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, factors.size(), handler);
	}

	private static class Handler extends
			AbstractInsertHandler<NormalizationWeightingFactor> {

		public Handler(List<NormalizationWeightingFactor> factors, Sequence seq) {
			super(factors, seq);
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
			stmt.setInt(5, seq.get(Sequence.IMPACT_CATEGORY,
					factor.f_normalizationweightingset));
		}
	}
}

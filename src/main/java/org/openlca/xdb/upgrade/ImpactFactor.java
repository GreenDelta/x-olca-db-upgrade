package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class ImpactFactor {

	@DbField("id")
	private String id;

	@DbField("f_flowpropertyfactor")
	private String f_flowpropertyfactor;

	@DbField("f_flow")
	private String f_flow;

	@DbField("f_unit")
	private String f_unit;

	@DbField("value")
	private double value;

	@DbField("f_lciacategory")
	private String f_lciacategory;

	@DbField("uncertainy_type")
	private String uncertainy_type;

	@DbField("uncertainty_parameter_1")
	private Double uncertainty_parameter_1;

	@DbField("uncertainty_parameter_2")
	private Double uncertainty_parameter_2;

	@DbField("uncertainty_parameter_3")
	private Double uncertainty_parameter_3;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_lciafactors";
		Mapper<ImpactFactor> mapper = new Mapper<>(ImpactFactor.class);
		List<ImpactFactor> factors = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_impact_factors(id, f_impact_category, "
				+ "f_flow, f_flow_property_factor, f_unit, value, distribution_type, "
				+ "parameter1_value, parameter1_formula, parameter2_value, "
				+ "parameter2_formula, parameter3_value, parameter3_formula) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(factors, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, factors.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<ImpactFactor> {

		public Handler(List<ImpactFactor> factors, Sequence seq) {
			super(factors, seq);
		}

		@Override
		protected void map(ImpactFactor factor, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// f_impact_category
			stmt.setInt(2,
					seq.get(Sequence.IMPACT_CATEGORY, factor.f_lciacategory));
			// f_flow
			stmt.setInt(3, seq.get(Sequence.FLOW, factor.f_flow));
			// f_flow_property_factor
			stmt.setInt(4, seq.get(Sequence.FLOW_PROPERTY_FACTOR,
					factor.f_flowpropertyfactor));
			// f_unit
			stmt.setInt(5, seq.get(Sequence.UNIT, factor.f_unit));
			// value
			stmt.setDouble(6, factor.value);
			// distribution_type
			stmt.setString(7, factor.uncertainy_type);
			// parameter1_value
			if (factor.uncertainty_parameter_1 == null)
				stmt.setNull(8, java.sql.Types.DOUBLE);
			else
				stmt.setDouble(8, factor.uncertainty_parameter_1);
			// parameter1_formula
			stmt.setString(9, null);
			// parameter2_value
			if (factor.uncertainty_parameter_2 == null)
				stmt.setNull(10, java.sql.Types.DOUBLE);
			else
				stmt.setDouble(10, factor.uncertainty_parameter_2);
			// parameter2_formula
			stmt.setString(11, null);
			// parameter3_value
			if (factor.uncertainty_parameter_3 == null)
				stmt.setNull(12, java.sql.Types.DOUBLE);
			else
				stmt.setDouble(12, factor.uncertainty_parameter_3);
			// parameter3_formula
			stmt.setString(13, null);
		}
	}
}
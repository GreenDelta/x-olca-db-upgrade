package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

class Exchange {

	@DbField("id")
	private String id;

	@DbField("avoidedproduct")
	private boolean avoidedproduct;

	@DbField("distributionType")
	private Integer distributionType;

	@DbField("input")
	private boolean input;

	@DbField("f_flowpropertyfactor")
	private String propertyFactorId;

	@DbField("f_unit")
	private String unitId;

	@DbField("f_flow")
	private String flowId;

	@DbField("parametrized")
	private boolean parametrized;

	@DbField("resultingamount_value")
	private double resultingamountValue;

	@DbField("resultingamount_formula")
	private String resultingamountFormula;

	@DbField("parameter1_value")
	private Double parameter1Value;

	@DbField("parameter1_formula")
	private String parameter1Formula;

	@DbField("parameter2_value")
	private Double parameter2Value;

	@DbField("parameter2_formula")
	private String parameter2Formula;

	@DbField("parameter3_value")
	private Double parameter3Value;

	@DbField("parameter3_formula")
	private String parameter3Formula;

	@DbField("f_owner")
	private String processId;

	@DbField("pedigree_uncertainty")
	private String pedigreeUncertainty;

	@DbField("base_uncertainty")
	private Double baseUncertainty;

	@DbField("f_default_provider")
	private String defaultProviderId;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_exchanges";
		Mapper<Exchange> mapper = new Mapper<>(Exchange.class);
		List<Exchange> exchanges = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_exchanges(id, f_owner, f_flow, "
				+ "f_unit, is_input, f_flow_property_factor, resulting_amount_value, "
				+ "resulting_amount_formula, avoided_product, f_default_provider, "
				+ "distribution_type, parameter1_value, parameter1_formula, "
				+ "parameter2_value, parameter2_formula, parameter3_value, "
				+ "parameter3_formula, pedigree_uncertainty, base_uncertainty) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(exchanges, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, exchanges.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<Exchange> {

		public Handler(List<Exchange> exchanges, Sequence seq) {
			super(exchanges, seq);
		}

		@Override
		protected void map(Exchange exchange, PreparedStatement stmt)
				throws SQLException {
			checkFormulas(exchange);
			// id
			stmt.setInt(1, seq.get(Sequence.EXCHANGE, exchange.id));
			// f_owner
			stmt.setInt(2, seq.get(Sequence.PROCESS, exchange.processId));
			// f_flow
			stmt.setInt(3, seq.get(Sequence.FLOW, exchange.flowId));
			// f_unit
			stmt.setInt(4, seq.get(Sequence.UNIT, exchange.unitId));
			// is_input
			stmt.setBoolean(5, exchange.input);
			// f_flow_property_factor
			stmt.setInt(6, seq.get(Sequence.FLOW_PROPERTY_FACTOR,
					exchange.propertyFactorId));
			// resulting_amount_value
			stmt.setDouble(7, exchange.resultingamountValue);
			// resulting_amount_formula
			stmt.setString(8, exchange.resultingamountFormula);
			// avoided_product
			stmt.setBoolean(9, exchange.avoidedproduct);
			// f_default_provider
			if (exchange.defaultProviderId == null)
				stmt.setNull(10, Types.INTEGER);
			else
				stmt.setInt(10,
						seq.get(Sequence.PROCESS, exchange.defaultProviderId));
			// distribution_type
			if (exchange.distributionType == null)
				stmt.setNull(11, Types.INTEGER);
			else
				stmt.setInt(11, exchange.distributionType);
			// parameter1_value
			if (exchange.parameter1Value == null)
				stmt.setNull(12, Types.DOUBLE);
			else
				stmt.setDouble(12, exchange.parameter1Value);
			// parameter1_formula
			stmt.setString(13, exchange.parameter1Formula);
			// parameter2_value
			if (exchange.parameter2Value == null)
				stmt.setNull(14, Types.DOUBLE);
			else
				stmt.setDouble(14, exchange.parameter2Value);
			// parameter2_formula
			stmt.setString(15, exchange.parameter2Formula);
			// parameter3_value
			if (exchange.parameter3Value == null)
				stmt.setNull(16, Types.DOUBLE);
			else
				stmt.setDouble(16, exchange.parameter3Value);
			// parameter3_formula
			stmt.setString(17, exchange.parameter3Formula);
			// pedigree_uncertainty
			stmt.setString(18, exchange.pedigreeUncertainty);
			// base_uncertainty
			if (exchange.baseUncertainty == null)
				stmt.setNull(19, Types.DOUBLE);
			else
				stmt.setDouble(19, exchange.baseUncertainty);
		}

		private void checkFormulas(Exchange e) {
			if (shouldDeleteFormula(e.parameter1Value, e.parameter1Formula))
				e.parameter1Formula = null;
			if (shouldDeleteFormula(e.parameter2Value, e.parameter2Formula))
				e.parameter2Formula = null;
			if (shouldDeleteFormula(e.parameter3Value, e.parameter3Formula))
				e.parameter3Formula = null;
		}

		private boolean shouldDeleteFormula(Double val, String formula) {
			if (formula == null)
				return false;
			if (val == null)
				return true;
			try {
				Double.parseDouble(formula);
				return true;
			} catch (Exception e) {
				return false;
			}
		}
	}
}

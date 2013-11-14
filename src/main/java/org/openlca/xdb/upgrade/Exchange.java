package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class Exchange {

	@DbField("id")
	private String id;

	@DbField("avoidedproduct")
	private boolean avoidedproduct;

	@DbField("distributionType")
	private int distributionType;

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
			// parameter1_value
			// parameter1_formula
			// parameter2_value
			// parameter2_formula
			// parameter3_value
			// parameter3_formula
			// pedigree_uncertainty
			// base_uncertainty

			stmt.setString(3, exchange.avoidedproduct);
			stmt.setString(4, exchange.distributionType);
			stmt.setString(5, exchange.input);
			stmt.setString(6, exchange.propertyFactorId);
			stmt.setString(7, exchange.unitId);
			stmt.setString(8, exchange.flowId);
			stmt.setString(9, exchange.parametrized);
			stmt.setString(10, exchange.resultingamountValue);

			stmt.setString(12, exchange.parameter1Value);
			stmt.setString(13, exchange.parameter1Formula);
			stmt.setString(14, exchange.parameter2Value);
			stmt.setString(15, exchange.parameter2Formula);
			stmt.setString(16, exchange.parameter3Value);
			stmt.setString(17, exchange.parameter3Formula);
			stmt.setString(18, exchange.processId);
			stmt.setString(19, exchange.pedigreeUncertainty);
			stmt.setString(20, exchange.baseUncertainty);
			stmt.setString(21, exchange.defaultProviderId);

		}
	}
}

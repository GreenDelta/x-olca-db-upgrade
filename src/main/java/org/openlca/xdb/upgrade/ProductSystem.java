package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class ProductSystem {

	@DbField("id")
	String id;

	@DbField("name")
	String name;

	@DbField("description")
	String description;

	@DbField("categoryid")
	String categoryid;

	@DbField("targetamount")
	double targetamount;

	@DbField("f_referenceprocess")
	String f_referenceprocess;

	@DbField("f_referenceexchange")
	String f_referenceexchange;

	@DbField("f_targetflowpropertyfactor")
	String f_targetflowpropertyfactor;

	@DbField("f_targetunit")
	String f_targetunit;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_productsystems";
		Mapper<ProductSystem> mapper = new Mapper<>(ProductSystem.class,
				oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<ProductSystem> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_product_systems(id, ref_id, name, "
					+ "description, f_category, target_amount, f_reference_process, "
					+ "f_reference_exchange, f_target_flow_property_factor, f_target_unit) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(ProductSystem system, PreparedStatement stmt)
				throws SQLException {

			// id
			stmt.setInt(1, seq.get(Sequence.PRODUCT_SYSTEM, system.id));
			// ref_id
			stmt.setString(2, system.id);
			// name
			stmt.setString(3, system.name);
			// description
			stmt.setString(4, system.description);
			// f_category
			if (Category.isNull(system.categoryid))
				stmt.setNull(5, java.sql.Types.INTEGER);
			else
				stmt.setInt(5, seq.get(Sequence.CATEGORY, system.categoryid));
			// target_amount
			stmt.setDouble(6, system.targetamount);
			// f_reference_process
			stmt.setInt(7, seq.get(Sequence.PROCESS, system.f_referenceprocess));
			// f_reference_exchange
			stmt.setInt(8,
					seq.get(Sequence.EXCHANGE, system.f_referenceexchange));
			// f_target_flow_property_factor
			stmt.setInt(9, seq.get(Sequence.FLOW_PROPERTY_FACTOR,
					system.f_targetflowpropertyfactor));
			// f_target_unit
			stmt.setInt(10, seq.get(Sequence.UNIT, system.f_targetunit));
		}
	}
}
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class ProductSystemProcess {

	@DbField("f_productsystem")
	private String f_productsystem;

	@DbField("f_process")
	private String f_process;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_productsystem_process";
		Mapper<ProductSystemProcess> mapper = new Mapper<>(
				ProductSystemProcess.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends
			UpdateHandler<ProductSystemProcess> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_product_system_processes("
					+ "f_product_system, f_process) " + "VALUES (?, ?)";
		}

		@Override
		protected void map(ProductSystemProcess proc, PreparedStatement stmt)
				throws SQLException {
			// f_product_system
			stmt.setInt(1,
					seq.get(Sequence.PRODUCT_SYSTEM, proc.f_productsystem));
			// f_process
			stmt.setInt(2, seq.get(Sequence.PROCESS, proc.f_process));
		}
	}
}
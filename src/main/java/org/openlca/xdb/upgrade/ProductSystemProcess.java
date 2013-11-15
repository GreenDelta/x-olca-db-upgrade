package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class ProductSystemProcess {

	@DbField("f_productsystem")
	private String f_productsystem;

	@DbField("f_process")
	private String f_process;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_productsystem_process";
		Mapper<ProductSystemProcess> mapper = new Mapper<>(
				ProductSystemProcess.class);
		List<ProductSystemProcess> procs = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_product_system_processes("
				+ "f_product_system, f_process) " + "VALUES (?, ?)";
		Handler handler = new Handler(procs, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, procs.size(), handler);
	}

	private static class Handler extends
			AbstractInsertHandler<ProductSystemProcess> {

		public Handler(List<ProductSystemProcess> procs, Sequence seq) {
			super(procs, seq);
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
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class ProcessLink {

	@DbField("id")
	private String id;

	@DbField("f_recipientprocess")
	private String f_recipientprocess;

	@DbField("f_recipientinput")
	private String f_recipientinput;

	@DbField("f_providerprocess")
	private String f_providerprocess;

	@DbField("f_provideroutput")
	private String f_provideroutput;

	@DbField("f_productsystem")
	private String f_productsystem;

	@DbField("f_flow")
	private String f_flow;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT l.*, e.f_flow FROM tbl_processlinks l "
				+ "join tbl_exchanges e on l.f_provideroutput = e.id";
		Mapper<ProcessLink> mapper = new Mapper<>(ProcessLink.class);
		List<ProcessLink> linkss = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_process_links(f_product_system, "
				+ "f_provider, f_recipient, f_flow) " + "VALUES (?, ?, ?, ?)";
		Handler handler = new Handler(linkss, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, linkss.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<ProcessLink> {

		public Handler(List<ProcessLink> linkss, Sequence seq) {
			super(linkss, seq);
		}

		@Override
		protected void map(ProcessLink links, PreparedStatement stmt)
				throws SQLException {
			// f_product_system
			stmt.setInt(1,
					seq.get(Sequence.PRODUCT_SYSTEM, links.f_productsystem));
			// f_provider
			stmt.setInt(2, seq.get(Sequence.PROCESS, links.f_providerprocess));
			// f_recipient
			stmt.setInt(3, seq.get(Sequence.PROCESS, links.f_recipientprocess));
			// f_flow
			stmt.setInt(4, seq.get(Sequence.FLOW, links.f_flow));
		}
	}
}
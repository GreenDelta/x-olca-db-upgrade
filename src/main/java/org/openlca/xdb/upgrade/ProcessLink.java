package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT l.*, e.f_flow FROM tbl_processlinks l "
				+ "join tbl_exchanges e on l.f_provideroutput = e.id";
		Mapper<ProcessLink> mapper = new Mapper<>(ProcessLink.class, oldDb,
				newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<ProcessLink> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_process_links(f_product_system, "
					+ "f_provider, f_recipient, f_flow) " + "VALUES (?, ?, ?, ?)";
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
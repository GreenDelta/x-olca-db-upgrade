package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class Process {

	@DbField("id")
	private String id;

	@DbField("processtype")
	private int processType;

	@DbField("allocationmethod")
	private Integer allocationMethod;

	@DbField("infrastructureprocess")
	private boolean infrastructureProcess;

	@DbField("geographycomment")
	private String geographyComment;

	@DbField("description")
	private String description;

	@DbField("name")
	private String name;

	@DbField("categoryid")
	private String categoryId;

	@DbField("f_quantitativereference")
	private String quantitativeReference;

	@DbField("f_location")
	private String locationId;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_processes";
		Mapper<Process> mapper = new Mapper<>(Process.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<Process> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_processes(id, ref_id, name, "
					+ "f_category, description, process_type, "
					+ "default_allocation_method, infrastructure_process, "
					+ "f_quantitative_reference, f_location, f_process_doc, "
					+ "last_change, version) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(Process proc, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.PROCESS, proc.id));
			// ref_id
			stmt.setString(2, proc.id);
			// name
			stmt.setString(3, proc.name);
			// f_category
			if (Category.isNull(proc.categoryId))
				stmt.setNull(4, java.sql.Types.INTEGER);
			else
				stmt.setInt(4, seq.get(Sequence.CATEGORY, proc.categoryId));
			// description
			stmt.setString(5, proc.description);
			// process_type
			stmt.setString(6, proc.processType == 0 ? "LCI_RESULT"
					: "UNIT_PROCESS");
			// default_allocation_method
			stmt.setString(7, mapAllocationMethod(proc.allocationMethod));
			// infrastructure_process
			stmt.setBoolean(8, proc.infrastructureProcess);
			// f_quantitative_reference
			stmt.setInt(9,
					seq.get(Sequence.EXCHANGE, proc.quantitativeReference));
			// f_location
			if (proc.locationId == null)
				stmt.setNull(10, java.sql.Types.INTEGER);
			else
				stmt.setInt(10, seq.get(Sequence.LOCATION, proc.locationId));
			// f_process_doc
			stmt.setInt(11, seq.get(Sequence.PROCESS, proc.id));
			stmt.setLong(12, System.currentTimeMillis());
			stmt.setLong(13, 4294967296L);
		}

		private String mapAllocationMethod(Integer allocationMethod) {
			if (allocationMethod == null)
				return null;
			switch (allocationMethod) {
			case 0:
				return "CAUSAL";
			case 1:
				return "ECONOMIC";
			case 2:
				return "NONE";
			case 3:
				return "PHYSICAL";
			default:
				return "NONE";
			}
		}
	}
}
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;
import org.openlca.core.model.AllocationMethod;
import org.openlca.core.model.ProcessType;

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

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_processes";
		Mapper<Process> mapper = new Mapper<>(Process.class);
		List<Process> procs = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_processes(id, ref_id, name, "
				+ "f_category, description, process_type, "
				+ "default_allocation_method, infrastructure_process, "
				+ "f_quantitative_reference, f_location, f_process_doc) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(procs, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, procs.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<Process> {

		public Handler(List<Process> procs, Sequence seq) {
			super(procs, seq);
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
			stmt.setString(6,
					proc.processType == 0 ? ProcessType.LCI_RESULT.name()
							: ProcessType.UNIT_PROCESS.name());
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
		}

		private String mapAllocationMethod(Integer allocationMethod) {
			if (allocationMethod == null)
				return null;
			switch (allocationMethod) {
			case 0:
				return AllocationMethod.CAUSAL.name();
			case 1:
				return AllocationMethod.ECONOMIC.name();
			case 2:
				return AllocationMethod.NONE.name();
			case 3:
				return AllocationMethod.PHYSICAL.name();
			default:
				return AllocationMethod.NONE.name();
			}
		}
	}
}
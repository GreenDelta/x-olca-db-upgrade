package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class ProjectVariant {

	@DbField("id")
	private String id;

	@DbField("productsystems")
	private String productsystems;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_projects";
		Mapper<ProjectVariant> mapper = new Mapper<>(ProjectVariant.class,
				oldDb, newDb);
		List<ProjectVariant> projects = mapper.getAll(oldDb, query);
		for (ProjectVariant oldProject : projects) {
			String oldSystemIds = oldProject.productsystems;
			if (oldSystemIds == null || oldSystemIds.trim().isEmpty())
				continue;
			StringBuilder idList = new StringBuilder();
			String[] ids = oldSystemIds.split(";");
			for (int i = 0; i < ids.length; i++) {
				idList.append("'").append(ids[i]).append("'");
				if (i < (ids.length - 1))
					idList.append(",");
			}
			String sysQuery = "SELECT * FROM tbl_productsystems WHERE id IN ("
					+ idList.toString() + ")";
			Mapper<ProductSystem> sysMapper = new Mapper<>(
					ProductSystem.class, oldDb, newDb);
			Handler handler = new Handler(seq, oldProject.id);
			sysMapper.mapAll(sysQuery, handler);
		}
	}

	private static class Handler extends UpdateHandler<ProductSystem> {

		private String oldProjectId;

		public Handler(Sequence seq, String oldProjectId) {
			super(seq);
			this.oldProjectId = oldProjectId;
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_project_variants(id, f_project, "
					+ "name, f_product_system, f_unit, "
					+ "f_flow_property_factor, amount) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(ProductSystem system, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// f_project
			stmt.setInt(2, seq.get(Sequence.PROJECT, oldProjectId));
			// name
			stmt.setString(3, system.name);
			// f_product_system
			stmt.setInt(4, seq.get(Sequence.PRODUCT_SYSTEM, system.id));
			// f_unit
			stmt.setInt(5, seq.get(Sequence.UNIT, system.f_targetunit));
			// f_flow_property_factor
			stmt.setInt(6, seq.get(Sequence.FLOW_PROPERTY_FACTOR,
					system.f_targetflowpropertyfactor));
			// amount
			stmt.setDouble(7, system.targetamount);
		}
	}
}

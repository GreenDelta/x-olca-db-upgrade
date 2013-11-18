package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class ProjectVariant {

	@DbField("id")
	private String id;

	@DbField("productsystems")
	private String productsystems;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_projects";
		Mapper<ProjectVariant> mapper = new Mapper<>(ProjectVariant.class);
		List<ProjectVariant> variants = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_project_variants(id, f_project, "
				+ "name, f_product_system) " + "VALUES (?, ?, ?, ?)";
		Handler handler = new Handler(NewVariant.create(variants, seq), seq);
		NativeSql.on(newDb).batchInsert(insertStmt, variants.size(), handler);
	}

	private static class NewVariant {

		private int id;
		private int projectId;
		private String name;
		private int systemId;

		static List<NewVariant> create(List<ProjectVariant> oldProjects,
				Sequence seq) {
			List<NewVariant> newVariants = new ArrayList<>();
			for (ProjectVariant oldProject : oldProjects) {
				String oldSystemIds = oldProject.productsystems;
				if (oldSystemIds == null || oldSystemIds.isEmpty())
					continue;
				int i = 0;
				for (String oldSystemId : oldSystemIds.split(";")) {
					NewVariant newVariant = new NewVariant();
					newVariant.id = seq.next();
					newVariant.projectId = seq.get(Sequence.PROJECT,
							oldProject.id);
					newVariant.name = "Variant " + (++i);
					newVariant.systemId = seq.get(Sequence.PRODUCT_SYSTEM,
							oldSystemId);
					newVariants.add(newVariant);
				}
			}
			return newVariants;
		}
	}

	private static class Handler extends AbstractInsertHandler<NewVariant> {

		public Handler(List<NewVariant> variants, Sequence seq) {
			super(variants, seq);
		}

		@Override
		protected void map(NewVariant variant, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, variant.id);
			// f_project
			stmt.setInt(2, variant.projectId);
			// name
			stmt.setString(3, variant.name);
			// f_product_system
			stmt.setInt(4, variant.systemId);
		}
	}
}

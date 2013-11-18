package org.openlca.xdb.upgrade;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class Project {

	@DbField("id")
	private String id;

	@DbField("productsystems")
	private String productsystems;

	@DbField("creationdate")
	private Date creationdate;

	@DbField("description")
	private String description;

	@DbField("categoryid")
	private String categoryid;

	@DbField("functionalunit")
	private String functionalunit;

	@DbField("name")
	private String name;

	@DbField("lastmodificationdate")
	private Date lastmodificationdate;

	@DbField("goal")
	private String goal;

	@DbField("f_author")
	private String f_author;

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_projects";
		Mapper<Project> mapper = new Mapper<>(Project.class);
		List<Project> projects = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_projects(id, ref_id, name, "
				+ "description, f_category, creation_date, functional_unit, "
				+ "last_modification_date, goal, f_author, f_impact_method, "
				+ "f_nwset) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(projects, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, projects.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<Project> {

		public Handler(List<Project> projects, Sequence seq) {
			super(projects, seq);
		}

		@Override
		protected void map(Project project, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.PROJECT, project.id));
			// ref_id
			stmt.setString(2, project.id);
			// name
			stmt.setString(3, project.name);
			// description
			stmt.setString(4, project.description);
			// f_category
			if (Category.isNull(project.categoryid))
				stmt.setNull(5, java.sql.Types.INTEGER);
			else
				stmt.setInt(5, seq.get(Sequence.CATEGORY, project.categoryid));
			// creation_date
			stmt.setDate(6, project.creationdate);
			// functional_unit
			stmt.setString(7, project.functionalunit);
			// last_modification_date
			stmt.setDate(8, project.lastmodificationdate);
			// goal
			stmt.setString(9, project.goal);
			// f_author
			if (project.f_author == null)
				stmt.setNull(10, java.sql.Types.INTEGER);
			else
				stmt.setInt(10, seq.get(Sequence.ACTOR, project.f_author));
			// f_impact_method
			stmt.setNull(11, java.sql.Types.INTEGER);
			// f_nwset
			stmt.setNull(12, java.sql.Types.INTEGER);
		}
	}
}
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class Actor {

	@DbField("id")
	private String id;

	@DbField("telefax")
	private String telefax;

	@DbField("website")
	private String website;

	@DbField("address")
	private String address;

	@DbField("description")
	private String description;

	@DbField("zipcode")
	private String zipcode;

	@DbField("name")
	private String name;

	@DbField("categoryid")
	private String categoryid;

	@DbField("email")
	private String email;

	@DbField("telephone")
	private String telephone;

	@DbField("country")
	private String country;

	@DbField("city")
	private String city;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_actors";
		Mapper<Actor> mapper = new Mapper<>(Actor.class);
		List<Actor> actors = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_actors(id, ref_id, telefax, "
				+ "website, address, description, zip_code, name, f_category, "
				+ "email, telephone, country, city) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(actors, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, actors.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<Actor> {

		public Handler(List<Actor> actors, Sequence seq) {
			super(actors, seq);
		}

		@Override
		protected void map(Actor actor, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.ACTOR, actor.id));
			stmt.setString(2, actor.id);
			stmt.setString(3, actor.telefax);
			stmt.setString(4, actor.website);
			stmt.setString(5, actor.address);
			stmt.setString(6, actor.description);
			stmt.setString(7, actor.zipcode);
			stmt.setString(8, actor.name);
			if (Category.isNull(actor.categoryid))
				stmt.setNull(9, java.sql.Types.INTEGER);
			else
				stmt.setInt(9, seq.get(Sequence.CATEGORY, actor.categoryid));
			stmt.setString(10, actor.email);
			stmt.setString(11, actor.telephone);
			stmt.setString(12, actor.country);
			stmt.setString(13, actor.city);
		}
	}
}

package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
		Mapper<Actor> mapper = new Mapper<>(Actor.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<Actor> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_actors(id, ref_id, telefax, "
					+ "website, address, description, zip_code, name, f_category, "
					+ "email, telephone, country, city, last_change, version) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
			stmt.setLong(14, System.currentTimeMillis());
			stmt.setLong(15, 4294967296L);
		}
	}
}

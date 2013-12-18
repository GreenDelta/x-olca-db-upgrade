package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class Location {

	@DbField("id")
	private String refId;

	@DbField("description")
	private String description;

	@DbField("name")
	private String name;

	@DbField("longitude")
	private double longitude;

	@DbField("code")
	private String code;

	@DbField("latitude")
	private double latitude;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_locations";
		Mapper<Location> mapper = new Mapper<>(Location.class, oldDb, newDb);
		Handler handler = new Handler(index);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<Location> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_locations(id, ref_id, description, "
					+ "name, longitude, latitude, code) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(Location loc, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.LOCATION, loc.refId));
			stmt.setString(2, loc.refId);
			stmt.setString(3, loc.description);
			stmt.setString(4, loc.name);
			stmt.setDouble(5, loc.longitude);
			stmt.setDouble(6, loc.latitude);
			stmt.setString(7, loc.code);
		}
	}

}

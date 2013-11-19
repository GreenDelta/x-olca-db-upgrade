package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

class Parameter {

	@DbField("id")
	private String id;

	@DbField("description")
	private String description;

	@DbField("name")
	private String name;

	@DbField("f_owner")
	private String f_owner;

	@DbField("type")
	private int type;

	@DbField("expression_parametrized")
	private boolean expression_parametrized;

	@DbField("expression_value")
	private double expression_value;

	@DbField("expression_formula")
	private String expression_formula;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * FROM tbl_parameters where type <> 1";
		Mapper<Parameter> mapper = new Mapper<>(Parameter.class);
		List<Parameter> parameters = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_parameters(id, name, description, "
				+ "is_input_param, f_owner, scope, value, formula) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(parameters, seq);
		NativeSql.on(newDb).batchInsert(insertStmt, parameters.size(), handler);
	}

	private static class Handler extends AbstractInsertHandler<Parameter> {

		public Handler(List<Parameter> parameters, Sequence seq) {
			super(parameters, seq);
		}

		@Override
		protected void map(Parameter parameter, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.next());
			// name
			stmt.setString(2, parameter.name);
			// description
			stmt.setString(3, parameter.description);
			// is_input_param
			stmt.setBoolean(4, !parameter.expression_parametrized);
			// f_owner
			if (parameter.type == 0)
				stmt.setInt(5, seq.get(Sequence.PROCESS, parameter.f_owner));
			else
				stmt.setNull(5, Types.INTEGER);
			// scope
			if (parameter.type == 0)
				stmt.setString(6, "PROCESS");
			else
				stmt.setString(6, "GLOBAL");
			// value
			stmt.setDouble(7, parameter.expression_value);
			// formula
			if (parameter.expression_parametrized)
				stmt.setString(8, parameter.expression_formula);
			else
				stmt.setString(8, null);
		}
	}

}

package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;
import org.openlca.core.model.ModelType;

class Category {

	@DbField("id")
	private String refId;

	@DbField("name")
	private String name;

	@DbField("componentclass")
	private String className;

	@DbField("f_parentcategory")
	private String parentCategoryId;

	//@formatter:off
	private static List<String> nullCategories = Arrays.asList(
			"org.openlca.core.model.Actor", 
			"org.openlca.core.model.Flow",
			"org.openlca.core.model.FlowProperty",
			"org.openlca.core.model.LCIAMethod",
			"org.openlca.core.model.Process",
			"org.openlca.core.model.ProductSystem",
			"org.openlca.core.model.Project", 
			"org.openlca.core.model.Source",
			"org.openlca.core.model.UnitGroup");
	//@formatter:on

	public static void map(OldDatabase oldDb, IDatabase newDb, Sequence index)
			throws Exception {
		String query = "SELECT * FROM tbl_categories";
		Mapper<Category> mapper = new Mapper<>(Category.class);
		List<Category> categories = mapper.mapAll(oldDb, query);
		String insertStmt = "INSERT INTO tbl_categories(id, ref_id, name, "
				+ "description, model_type, f_parent_category) "
				+ "VALUES (?, ?, ?, ?, ?, ?)";
		Handler handler = new Handler(categories, index);
		NativeSql.on(newDb).batchInsert(insertStmt, categories.size(), handler);
		// TODO: remove old categories
	}

	/** Returns true if the category should be null in the new version */
	public static boolean isNull(String refId) {
		if (refId == null)
			return true;
		return nullCategories.contains(refId);
	}

	private static class Handler extends AbstractInsertHandler<Category> {

		public Handler(List<Category> categories, Sequence seq) {
			super(categories, seq);
		}

		@Override
		protected void map(Category category, PreparedStatement stmt)
				throws SQLException {
			stmt.setInt(1, seq.get(Sequence.CATEGORY, category.refId));
			String refId = category.refId;
			if (refId.length() > 36)
				refId = refId.substring(0, 36);
			stmt.setString(2, refId);
			stmt.setString(3, category.name);
			stmt.setString(4, null);
			stmt.setString(5, mapType(category.className).name());
			String parentRef = category.parentCategoryId;
			if (isNull(parentRef))
				stmt.setNull(6, java.sql.Types.INTEGER);
			else
				stmt.setInt(6, seq.get(Sequence.CATEGORY, parentRef));
		}

		private ModelType mapType(String className) {
			if (className == null)
				return ModelType.UNKNOWN;
			switch (className) {
			case "org.openlca.core.model.Actor":
				return ModelType.ACTOR;
			case "org.openlca.core.model.Flow":
				return ModelType.FLOW;
			case "org.openlca.core.model.FlowProperty":
				return ModelType.FLOW_PROPERTY;
			case "org.openlca.core.model.LCIAMethod":
				return ModelType.IMPACT_METHOD;
			case "org.openlca.core.model.Process":
				return ModelType.PROCESS;
			case "org.openlca.core.model.ProductSystem":
				return ModelType.PRODUCT_SYSTEM;
			case "org.openlca.core.model.Project":
				return ModelType.PROJECT;
			case "org.openlca.core.model.Source":
				return ModelType.SOURCE;
			case "org.openlca.core.model.UnitGroup":
				return ModelType.UNIT_GROUP;
			default:
				return ModelType.UNKNOWN;
			}
		}
	}

}

package org.openlca.xdb.upgrade;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;
import org.openlca.core.database.NativeSql.QueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mapper<T> {

	private Logger log = LoggerFactory.getLogger(getClass());
	private Class<T> clazz;
	private List<Field> dbFields = new ArrayList<>();

	public Mapper(Class<T> clazz) {
		this.clazz = clazz;
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			if (field.isAnnotationPresent(DbField.class))
				dbFields.add(field);
		}
	}

	public List<T> mapAll(IDatabase db, String query) throws Exception {
		log.trace("fetch and map from database: {}", query);
		final List<T> results = new ArrayList<>();
		NativeSql.on(db).query(query, new QueryResultHandler() {
			@Override
			public boolean nextResult(ResultSet result) throws SQLException {
				try {
					T t = map(result);
					results.add(t);
					return true;
				} catch (Exception e) {
					log.error("failed to map result", e);
					throw new RuntimeException(e);
				}
			}
		});
		log.trace("{} results fetched", results.size());
		return results;
	}

	public T map(ResultSet set) throws Exception {
		T instance = clazz.newInstance();
		for (Field field : dbFields) {
			field.setAccessible(true);
			String name = field.getAnnotation(DbField.class).value();
			if (isString(field)) {
				String value = set.getString(name);
				field.set(instance, value);
			} else if (isDouble(field)) {
				double value = set.getDouble(name);
				field.setDouble(instance, value);
			} else if (isInt(field)) {
				int value = set.getInt(name);
				field.setInt(instance, value);
			} else if (isBool(field)) {
				boolean value = set.getBoolean(name);
				field.setBoolean(instance, value);
			}
		}
		return instance;
	}

	private boolean isString(Field field) {
		return Objects.equals(field.getType(), String.class);
	}

	private boolean isDouble(Field field) {
		return Objects.equals(field.getType(), double.class);
	}

	private boolean isInt(Field field) {
		return Objects.equals(field.getType(), int.class);
	}

	private boolean isBool(Field field) {
		return Objects.equals(field.getType(), boolean.class);
	}

}

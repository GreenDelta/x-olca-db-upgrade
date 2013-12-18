package org.openlca.xdb.upgrade;

import org.openlca.xdb.upgrade.NativeSql.QueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class Mapper<T> {

	private Logger log = LoggerFactory.getLogger(getClass());
	private Class<T> clazz;
	private IDatabase oldDatabase;
	private IDatabase newDatabase;
	private List<Field> dbFields = new ArrayList<>();

	public Mapper(Class<T> clazz, IDatabase oldDatabase,
	              IDatabase newDatabase) {
		this.clazz = clazz;
		this.oldDatabase = oldDatabase;
		this.newDatabase = newDatabase;
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			if (field.isAnnotationPresent(DbField.class))
				dbFields.add(field);
		}
	}

	public void mapAll(String query, final UpdateHandler<T> handler)
			throws Exception {
		log.trace("fetch and map from database: {}", query);
		final int BATCH_SIZE = 5000;
		final List<T> results = new ArrayList<>();
		NativeSql.on(oldDatabase).query(query, new QueryResultHandler() {
			@Override
			public boolean nextResult(ResultSet result) throws SQLException {
				try {
					T t = map(result);
					results.add(t);
					if (results.size() >= BATCH_SIZE)
						flushMappings(results, handler);
					return true;
				} catch (Exception e) {
					log.error("failed to map result", e);
					throw new RuntimeException(e);
				}
			}
		});
		flushMappings(results, handler);
	}

	public List<T> getAll(IDatabase db, String query) throws Exception {
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

	private void flushMappings(List<T> results, UpdateHandler<T> handler) {
		if (results.isEmpty())
			return;
		handler.nextBatch(results);
		log.trace("insert next batch with {} elements", results.size());
		try {
			NativeSql.on(newDatabase).batchInsert(handler.getStatement(),
					results.size(), handler);
		} catch (Exception e) {
			log.error("failed to insert batch: " + handler.getStatement(), e);
		} finally {
			results.clear();
		}
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
			} else if (isDoubleObj(field)) {
				double d = set.getDouble(name);
				Double value = set.wasNull() ? null : d;
				field.set(instance, value);
			} else if (isInt(field)) {
				int value = set.getInt(name);
				field.setInt(instance, value);
			} else if (isIntObj(field)) {
				int i = set.getInt(name);
				Integer value = set.wasNull() ? null : i;
				field.set(instance, value);
			} else if (isBool(field)) {
				boolean value = set.getBoolean(name);
				field.setBoolean(instance, value);
			} else if (isDate(field)) {
				Date value = set.getDate(name);
				field.set(instance, value);
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

	private boolean isDoubleObj(Field field) {
		return Objects.equals(field.getType(), Double.class);
	}

	private boolean isInt(Field field) {
		return Objects.equals(field.getType(), int.class);
	}

	private boolean isIntObj(Field field) {
		return Objects.equals(field.getType(), Integer.class);
	}

	private boolean isBool(Field field) {
		return Objects.equals(field.getType(), boolean.class);
	}

	private boolean isDate(Field field) {
		return Objects.equals(field.getType(), Date.class);
	}

}

package org.openlca.xdb.upgrade;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import javax.persistence.EntityManagerFactory;

import org.openlca.core.database.BaseDao;
import org.openlca.core.database.DatabaseException;
import org.openlca.core.database.IDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * An old openLCA database. Implements only the parts of the database interface
 * that are used for the update.
 */
public class OldDatabase implements IDatabase {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	private String url;
	private String user;
	private String password;
	private BoneCP connectionPool;

	public OldDatabase(String url, String user, String password) {
		this.url = url;
		if (!this.url.contains("rewriteBatchedStatements")
				&& this.url.contains("useServerPrepStmts")) {
			this.url += "&rewriteBatchedStatements=true"
					+ "&useServerPrepStmts=false";
			log.trace("modified URL optimized for batch updates: {}", this.url);
		}
		this.user = user;
		this.password = password;
		initConnectionPool();
	}

	private void initConnectionPool() {
		try {
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(url);
			config.setUser(user);
			config.setPassword(password);
			connectionPool = new BoneCP(config);
		} catch (Exception e) {
			log.error("failed to initialize connection pool", e);
			throw new DatabaseException("Could not create a connection", e);
		}
	}

	@Override
	public void close() throws IOException {
		log.trace("close database mysql: {} @ {}", user, url);
		try {
			if (connectionPool != null)
				connectionPool.shutdown();
		} catch (Exception e) {
			log.error("failed to close database", e);
		} finally {
			connectionPool = null;
		}
	}

	@Override
	public Connection createConnection() {
		log.trace("create connection mysql: {} @ {}", user, url);
		try {
			if (connectionPool != null) {
				Connection con = connectionPool.getConnection();
				con.setAutoCommit(false);
				return con;
			} else {
				log.warn("no connection pool set up for {}", url);
				return DriverManager.getConnection(url, user, password);
			}
		} catch (Exception e) {
			log.error("Failed to create database connection", e);
			return null;
		}
	}

	@Override
	public EntityManagerFactory getEntityFactory() {
		return null;
	}

	@Override
	public <T> BaseDao<T> createDao(Class<T> clazz) {
		return null;
	}

	@Override
	public String getName() {
		if (url == null)
			return null;
		String[] parts = url.split("/");
		if (parts.length < 2)
			return null;
		return parts[parts.length - 1].trim();
	}

}

package org.openlca.xdb.upgrade;

import java.sql.Connection;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * IDatabase implementation for MySQL database. The URL schema is
 * "jdbc:mysql://" [host] ":" [port] "/" [database].
 */
public class MySQLDatabase implements IDatabase {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	private String url;
	private String user;
	private String password;
	private BoneCP connectionPool;
	private final String persistenceUnit;

	public MySQLDatabase(String url, String user, String password) {
		this(url, user, password, "openLCA");
	}

	public MySQLDatabase(String url, String user, String password,
			String persistenceUnit) {
		this.persistenceUnit = persistenceUnit;
		this.url = url;
		if (!this.url.contains("rewriteBatchedStatements")
				&& this.url.contains("useServerPrepStmts")) {
			this.url += "&rewriteBatchedStatements=true"
					+ "&useServerPrepStmts=false";
			log.trace("modified URL optimized for batch updates: {}", this.url);
		}
		this.user = user;
		this.password = password;
		connect();
	}

	private void connect() {
		log.trace("Connect to database mysql: {} @ {}", user, url);
		try {
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(url);
			config.setUser(user);
			config.setPassword(password);
			connectionPool = new BoneCP(config);
		} catch (Exception e) {
			log.error("failed to initialize connection pool", e);
			throw new RuntimeException("Could not create a connection", e);
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
	public void close() {
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

	public String getName() {
		if (url == null)
			return null;
		String[] parts = url.split("/");
		if (parts.length < 2)
			return null;
		return parts[parts.length - 1].trim();
	}
}

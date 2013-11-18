package org.openlca.xdb.upgrade;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class DerbyDatabase implements IDatabase {

	private Logger log = LoggerFactory.getLogger(getClass());
	private String url;
	private File folder;
	private boolean closed = false;
	private BoneCP connectionPool;

	public DerbyDatabase(File folder) {
		this.folder = folder;
		boolean create = !folder.exists();
		log.info("initialize database folder {}, create={}", folder, create);
		url = "jdbc:derby:" + folder.getAbsolutePath().replace('\\', '/');
		log.trace("database url: {}", url);
		try {
			DriverManager.registerDriver(new EmbeddedDriver());
		} catch (Exception e) {
			throw new RuntimeException("Could not load driver", e);
		}
		if (create)
			createNew(url);
		connect();
	}

	private void createNew(String url) {
		log.trace("create new database @ {}", folder);
		folder.mkdirs();
		String tempDirPath = System.getProperty("java.io.tmpdir");
		File tempDir = new File(tempDirPath);
		File tempZolca = new File(tempDir, UUID.randomUUID().toString()
				+ ".zolca");
		try {
			FileOutputStream fos = new FileOutputStream(tempZolca);
			IOUtils.copy(getClass().getResourceAsStream("empty.zolca"), fos);
			fos.flush();
			fos.close();
			ZipUtil.unpack(tempZolca, folder);
			tempZolca.delete();
		} catch (Exception e) {
			throw new RuntimeException("Could not create an empty database", e);
		}
	}

	private void connect() {
		log.trace("connect to database: {}", url);
		try {
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(url);
			connectionPool = new BoneCP(config);
		} catch (Exception e) {
			log.error("failed to initialize connection pool", e);
			throw new RuntimeException("Could not create a connection", e);
		}
	}

	@Override
	public void close() throws IOException {
		if (closed)
			return;
		log.trace("close database: {}", url);
		if (connectionPool != null)
			connectionPool.shutdown();
		try {
			// TODO: single database shutdown throws unexpected
			// error in eclipse APP - close all connections here
			// DriverManager.getConnection(url + ";shutdown=true");
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
			System.gc(); // unload embedded driver for possible restarts
			// see also
			// http://db.apache.org/derby/docs/10.4/devguide/rdevcsecure26537.html
		} catch (SQLException e) {
			// a normal shutdown of derby throws an SQL exception
			// with error code 50000 (for single database shutdown
			// 45000), otherwise an error occurred
			log.info("exception: {}", e.getErrorCode());
			if (e.getErrorCode() != 45000 && e.getErrorCode() != 50000)
				log.error(e.getMessage(), e);
			else {
				closed = true;
				log.info("database closed");
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public Connection createConnection() {
		log.trace("create connection: {}", url);
		try {
			if (connectionPool != null) {
				Connection con = connectionPool.getConnection();
				con.setAutoCommit(false);
				return con;
			} else {
				log.warn("no connection pool set up for {}", url);
				return DriverManager.getConnection(url);
			}
		} catch (Exception e) {
			log.error("Failed to create database connection", e);
			return null;
		}
	}

	@Override
	public String getName() {
		return folder.getName();
	}

	/** Closes the database and deletes the underlying folder. */
	public void delete() throws Exception {
		if (!closed)
			close();
		delete(folder);
	}

	private void delete(File folder) {
		log.trace("delete folder {}", folder);
		for (File f : folder.listFiles()) {
			if (f.isDirectory())
				delete(f);
			f.delete();
		}
		boolean b = folder.delete();
		log.trace("folder {} deleted? -> {}", folder, b);
	}

}

package org.openlca.xdb.upgrade;

import java.io.File;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

/**
 * Exports a database to a file. During the export a Derby database is created
 * in a temporary folder and then packed into the file.
 */
public class FileExport implements Runnable {

	private Logger log = LoggerFactory.getLogger(getClass());
	private OldDatabase oldDatabase;
	private File exportFile;

	private File tempDir;
	private DerbyDatabase newDatabase;

	public FileExport(OldDatabase oldDatabase, File file) {
		this.oldDatabase = oldDatabase;
		this.exportFile = file;
	}

	@Override
	public void run() {
		try {
			newDatabase = createDerbyDb();
			new Update(oldDatabase, newDatabase).run();
			newDatabase.close();
			log.trace("write database to file {}", exportFile);
			ZipUtil.pack(tempDir, exportFile);
			log.trace("delete temporary database");
			FileUtils.deleteDirectory(tempDir);
			log.trace("all done");
		} catch (Exception e) {
			log.error("Database export failed", e);
		}
	}

	private DerbyDatabase createDerbyDb() throws Exception {
		File temp = new File(System.getProperty("java.io.tmpdir"));
		tempDir = new File(temp, UUID.randomUUID().toString());
		log.trace("Create a temporary database @ {}", tempDir);
		DerbyDatabase db = new DerbyDatabase(tempDir);
		db.createConnection().close(); // test connection
		log.trace("database created");
		return db;
	}

}

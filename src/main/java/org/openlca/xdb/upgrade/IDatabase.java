package org.openlca.xdb.upgrade;

import java.io.Closeable;
import java.sql.Connection;

/**
 * The common interface for openLCA databases.
 */
public interface IDatabase extends Closeable {

	/**
	 * Creates a native SQL connection to the underlying database. The
	 * connection should be closed from the respective client.
	 */
	Connection createConnection();

	/**
	 * Returns the database name.
	 */
	public String getName();

}

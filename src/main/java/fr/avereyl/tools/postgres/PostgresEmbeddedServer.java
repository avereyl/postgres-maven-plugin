/**
 * 
 */
package fr.avereyl.tools.postgres;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileLock;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.postgresql.ds.PGSimpleDataSource;

import fr.avereyl.tools.EmbeddedServer;
import fr.avereyl.tools.traits.OperatingSystemAware;
import lombok.extern.slf4j.Slf4j;

/**
 * @author guillaume
 *
 */
@Slf4j
public class PostgresEmbeddedServer implements EmbeddedServer {

	private static final String JDBC_FORMAT = "jdbc:postgresql://localhost:%s/%s?user=%s";

	private static final String PG_STOP_MODE = "fast";
	private static final String PG_STOP_WAIT_S = "5";
	private static final String PG_SUPERUSER = "postgres";

	private static final Duration DEFAULT_PG_STARTUP_WAIT = Duration.ofSeconds(10);
	private static final String LOCK_FILE_NAME = "epg-lock";

	private final UUID instanceId = UUID.randomUUID();

	/**
	 * Folder where POSTGRES binaries are installed. by default:
	 * java.io.tmp/embedded-pg
	 */
	private final File postgresDirectory;
	/**
	 * 
	 */
	private Optional<File> overriddenWorkingDirectory = Optional.empty();

	/**
	 * Strategy to resolve POSTGRES binaries.
	 */
	private PostgresBinaryResolver postgresBinaryResolver;
	private final PostgresBinaryPreparer postgresBinaryPreparer = new PostgresBinaryPreparer();
	private final PostgresController postgresController = new PostgresController();

	private File dataDirectory;

	private Duration pgStartupWait;
	private int port;

	private final AtomicBoolean started = new AtomicBoolean();
	private final AtomicBoolean closed = new AtomicBoolean();

	private final Map<String, String> postgresConfig = new HashMap<>();
	private final Map<String, String> localeConfig = new HashMap<>();

	private File lockFile;

	private volatile FileOutputStream lockStream;
	private volatile FileLock lock;

	private boolean cleanDataDirectoryAfterClosing;
	private boolean cleanDataDirectoryBeforeStarting;

	private PostgresEmbeddedServer(PostgresEmbeddedServer.Builder builder) {
		// set fields from builder operations
		builder.operations.forEach(op -> op.accept(this));

		// check for missing mandatory parameters

		//
		lockFile = new File(this.dataDirectory, LOCK_FILE_NAME);

		// set fields with default values (if needed)
		this.postgresBinaryResolver = this.postgresBinaryResolver == null ? new BundledPostgresBinaryResolver()
				: this.postgresBinaryResolver;

		// prepare POSTGRES binaries (if needed)
		this.postgresDirectory = postgresBinaryPreparer.prepare(this.postgresBinaryResolver,
				overriddenWorkingDirectory);

		// clean data directories (if needed)
		if (isCleaningDataDirectoryBeforeStartRequired()) {
			cleanDataDirectory(this.dataDirectory);
		}

		// initialize the database if no postgresql.conf file found
		if (!new File(dataDirectory, "postgresql.conf").exists()) {
			initDatabase();
		}

	}

	public static PostgresEmbeddedServer.Builder builder() {
		return new Builder();
	}

	/**
	 * 
	 * @author guillaume
	 *
	 */
	public static final class Builder {

		private final List<Consumer<PostgresEmbeddedServer>> operations = new ArrayList<>();

		private Builder() {
		}

		public PostgresEmbeddedServer build() {
			return new PostgresEmbeddedServer(this);
		}
	}

	public EmbeddedServer start(Map<String, String> connectionConfig) throws IOException {
		// clean data (if needed) TODO integrate property access in builder
		if (cleanDataDirectoryBeforeStarting && System.getProperty("pmp.no-cleanup") == null) {
			try {
				FileUtils.deleteDirectory(dataDirectory);
			} catch (IOException e) {
				log.error("Could not clean up directory {}", dataDirectory.getAbsolutePath());
			}
		} else {
			log.info("Did not clean up directory {}", dataDirectory.getAbsolutePath());
		}

		// lock !!
		lock();

		// start postgres server with the given/computed configuration
		final StopWatch watch = new StopWatch();
		watch.start();
		if (started.getAndSet(true)) {
			throw new IllegalStateException("Postmaster already started");
		}

		final List<String> options = new ArrayList<>();
		options.add("-o");
		options.addAll(OperatingSystemAware.createInitOptions(port, postgresConfig));

		Process postmaster = postgresController.pgCtl(postgresDirectory, dataDirectory, "start", options);
		log.info("{} postmaster started as {} on port {}.  Waiting up to {} for server startup to finish.", instanceId,
				postmaster.toString(), port, pgStartupWait);

		// add shutdown hook
		final Thread closeThread = new Thread(() -> {
			try {
				PostgresEmbeddedServer.this.close();
			} catch (IOException ex) {
				log.error("Unexpected IOException from Closeables.close", ex);
			}
		});
		closeThread.setName("postgres-" + instanceId + "-closer");
		Runtime.getRuntime().addShutdownHook(closeThread);
		waitForServerStartup(watch, connectionConfig);
		return this;
	}

	public void waitForServerStartup(StopWatch watch, Map<String, String> connectConfig) throws IOException {
		Throwable lastCause = null;
		final long start = System.nanoTime();
		final long maxWaitNs = TimeUnit.NANOSECONDS.convert(pgStartupWait.toMillis(), TimeUnit.MILLISECONDS);
		while (System.nanoTime() - start < maxWaitNs) {
			try {
				verifyReady(connectConfig);
				log.info("{} postmaster startup finished in {}", instanceId, watch);
				return;
			} catch (final SQLException e) {
				lastCause = e;
				log.trace("While waiting for server startup", e);
			}

			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		throw new IOException("Gave up waiting for server to start after " + pgStartupWait.toMillis() + "ms",
				lastCause);
	}

	public void verifyReady(Map<String, String> connectConfig) throws SQLException {
		final InetAddress localhost = InetAddress.getLoopbackAddress();
		try (Socket sock = new Socket()) {
			sock.setSoTimeout((int) Duration.ofMillis(500).toMillis());
			sock.connect(new InetSocketAddress(localhost, port), (int) Duration.ofMillis(500).toMillis());
		} catch (final IOException e) {
			throw new SQLException("connect failed", e);
		}
		try (Connection c = getPostgresDatabase(connectConfig).getConnection();
				Statement s = c.createStatement();
				ResultSet rs = s.executeQuery("SELECT 1")) {
			if (!rs.next()) {
				throw new IllegalStateException("expecting single row");
			}
			if (1 != rs.getInt(1)) {
				throw new IllegalStateException("expecting 1");
			}
			if (rs.next()) {
				throw new IllegalStateException("expecting single row");
			}
		}
	}

	public String getJdbcUrl(String userName, String dbName) {
		return String.format(JDBC_FORMAT, port, dbName, userName);
	}

	public DataSource getPostgresDatabase() {
		return getDatabase(PG_SUPERUSER, "postgres");
	}

	public DataSource getPostgresDatabase(Map<String, String> properties) {
		return getDatabase(PG_SUPERUSER, "postgres", properties);
	}

	public DataSource getDatabase(String userName, String dbName) {
		return getDatabase(userName, dbName, Collections.emptyMap());
	}

	public DataSource getDatabase(String userName, String dbName, Map<String, String> properties) {
		final PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerName("localhost");
		ds.setPortNumber(port);
		ds.setDatabaseName(dbName);
		ds.setUser(userName);

		properties.forEach((propertyKey, propertyValue) -> {
			try {
				ds.setProperty(propertyKey, propertyValue);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		return ds;
	}

	private void cleanDataDirectory(File parentDirectory) {
		// TODO Auto-generated method stub

	}

	private void initDatabase() {
		final StopWatch watch = new StopWatch();
		watch.start();
		List<String> options = new ArrayList<>();
		options.addAll(Arrays.asList("-A", "trust", "-U", PG_SUPERUSER, "-E", "UTF-8"));
		options.addAll(OperatingSystemAware.createLocaleOptions(this.localeConfig));
		postgresController.initDb(postgresDirectory, dataDirectory, options);
		log.info("{} initdb completed in {}", instanceId, watch);
	}

	@Override
	public boolean isCleaningDataDirectoryBeforeStartRequired() {
		return cleanDataDirectoryBeforeStarting;
	}
	@Override
	public boolean isCleaningDataDirectoryAfterStopRequired() {
		return cleanDataDirectoryAfterClosing;
	}

	/**
	 * 
	 * @throws IOException
	 */
	private void lock() throws IOException {
		lockStream = new FileOutputStream(lockFile);
		if ((lock = lockStream.getChannel().tryLock()) == null) {
			throw new IllegalStateException("could not lock " + lockFile);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {

		// stop POSTGRES
		if (closed.getAndSet(true)) {
			log.warn("Server already stopped");
			return;
		}
		final StopWatch watch = new StopWatch();
		watch.start();
		try {
			postgresController.pgCtl(postgresDirectory, dataDirectory, "stop",
					Arrays.asList("-m", PG_STOP_MODE, "-t", PG_STOP_WAIT_S, "-w"));
			log.info("{} shut down postmaster in {}", instanceId, watch);
		} catch (final Exception e) {
			log.error("Could not stop postmaster " + instanceId, e);
		}
		// unlock
		if (lock != null) {
			lock.release();
		}
		try {
			lockStream.close();
		} catch (IOException e) {
			log.error("while closing lockStream", e);
		}

		// clean data (if needed) TODO integrate property access in builder
		if (cleanDataDirectoryAfterClosing && System.getProperty("pmp.no-cleanup") == null) {
			try {
				FileUtils.deleteDirectory(dataDirectory);
			} catch (IOException e) {
				log.error("Could not clean up directory {}", dataDirectory.getAbsolutePath());
			}
		} else {
			log.info("Did not clean up directory {}", dataDirectory.getAbsolutePath());
		}
	}

}

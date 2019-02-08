/*******************************************************************************
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package fr.avereyl.tools.postgres;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.postgresql.ds.PGSimpleDataSource;

import fr.avereyl.tools.EmbeddedServer;
import fr.avereyl.tools.traits.FileSystemAware;
import fr.avereyl.tools.traits.OperatingSystemAware;
import lombok.extern.slf4j.Slf4j;

/**
 * @author guillaume
 *
 */
@Slf4j
public class PostgresEmbeddedServer implements EmbeddedServer, OperatingSystemAware, FileSystemAware {

	private static final String JDBC_FORMAT = "jdbc:postgresql://localhost:%s/%s?user=%s";

	private static final String PG_STOP_MODE = "fast";
	private static final String PG_STOP_WAIT_S = "5";
	private static final String PG_SUPERUSER = "postgres";

	private static final Duration DEFAULT_PG_STARTUP_WAIT = Duration.ofSeconds(10);

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

	private File dataDirectory;

	private Duration pgStartupWait;
	private int port;

	private final AtomicBoolean started = new AtomicBoolean();
	private final AtomicBoolean closed = new AtomicBoolean();

	private final Map<String, String> postgresConfig = new HashMap<>();
	private final Map<String, String> localeConfig = new HashMap<>();

	private boolean cleanDataDirectoryAfterClosing;
	private boolean cleanDataDirectoryBeforeStarting;

	private PostgresEmbeddedServer(final PostgresEmbeddedServer.Builder builder) throws IOException {
		// set fields from builder operations
		builder.operations.forEach(op -> op.accept(this));

		// check for missing mandatory parameters
		this.port = this.port <= 0 ? this.detectPort() : this.port;
		this.pgStartupWait = this.pgStartupWait == null ? DEFAULT_PG_STARTUP_WAIT : this.pgStartupWait;

		// set fields with default values (if needed)
		this.postgresBinaryResolver = this.postgresBinaryResolver == null ? new BundledPostgresBinaryResolver()
				: this.postgresBinaryResolver;

		// prepare POSTGRES binaries (if needed)
		this.postgresDirectory = this.postgresBinaryPreparer.prepare(this.postgresBinaryResolver,
				this.overriddenWorkingDirectory);

		// clean data directories (if needed)
		if (this.isCleaningDataDirectoryBeforeStartRequired()) {
			this.cleanDataDirectory(this.dataDirectory);
		}

		// initialize the database if no postgresql.conf file found
		if (!new File(this.dataDirectory, "postgresql.conf").exists()) {
			this.initDatabase();
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
			this.config("timezone", "UTC");
			this.config("synchronous_commit", "off");
			this.config("max_connections", "300");
		}

		public PostgresEmbeddedServer build() throws IOException {
			return new PostgresEmbeddedServer(this);
		}

		public Builder port(final Integer port) {
			this.operations.add(server -> server.port = port);
			return this;
		}

		public Builder pgdir(final String pgdir) {
			this.operations.add(server -> server.overriddenWorkingDirectory = Optional.of(Paths.get(pgdir).toFile()));
			return this;
		}

		public Builder datadir(final String datadir) {
			this.operations.add(server -> server.dataDirectory = Paths.get(datadir).toFile());
			return this;
		}

		public Builder config(final String key, final String value) {
			this.operations.add(server -> server.postgresConfig.put(key, value));
			return null;
		}

	}

	@Override
	public EmbeddedServer start(final Map<String, String> connectionConfig) throws IOException {
		Objects.requireNonNull(this.pgStartupWait, "Wait time cannot be null");
		// clean data (if needed) TODO integrate property access in builder
		if (this.cleanDataDirectoryBeforeStarting && System.getProperty("pmp.no-cleanup") == null) {
			try {
				FileUtils.deleteDirectory(this.dataDirectory);
			} catch (final IOException e) {
				log.error("Could not clean up directory {}", this.dataDirectory.getAbsolutePath());
			}
		} else {
			log.info("Did not clean up directory {}", this.dataDirectory.getAbsolutePath());
		}

		// start postgres server with the given/computed configuration
		final StopWatch watch = new StopWatch();
		watch.start();
		if (this.started.getAndSet(true)) {
			throw new IllegalStateException("Postmaster already started");
		}

		// ---------------------
		final String binaryPath = this.getBinaryPath(this.postgresDirectory.getPath(), "pg_ctl");
		final String options = this.createInitOptions(this.port, this.postgresConfig).stream()
				.collect(Collectors.joining(" "));
		final String[] commands = { binaryPath, "-D", this.dataDirectory.getPath(), "-o", options, "start" };

		final Process postmaster = this.system(log, commands);

		log.info("{} postmaster started as {} on port {}.  Waiting up to {} for server startup to finish.",
				this.instanceId, postmaster.toString(), this.port, this.pgStartupWait);

		// add shutdown hook (only if needed)
		this.addShutDownHook(false);//
		this.waitForServerStartup(watch, connectionConfig);
		return this;
	}

	private void addShutDownHook(final boolean shutdownHookNeeded) {
		if (shutdownHookNeeded) {
			final Thread closeThread = new Thread(() -> {
				try {
					PostgresEmbeddedServer.this.close();
				} catch (final IOException ex) {
					log.error("Unexpected IOException from Closeables.close", ex);
				}
			});
			closeThread.setName("postgres-" + this.instanceId + "-closer");
			Runtime.getRuntime().addShutdownHook(closeThread);
		}
	}

	public void waitForServerStartup(final StopWatch watch, final Map<String, String> connectConfig)
			throws IOException {
		Throwable lastCause = null;
		final long start = System.nanoTime();
		final long maxWaitNs = TimeUnit.NANOSECONDS.convert(this.pgStartupWait.toMillis(), TimeUnit.MILLISECONDS);
		while (System.nanoTime() - start < maxWaitNs) {
			try {
				this.verifyReady(connectConfig);
				log.info("{} postmaster startup finished in {}", this.instanceId, watch);
				return;
			} catch (final SQLException e) {
				lastCause = e;
				log.warn("While waiting for server startup", e);
			}

			try {
				Thread.sleep(100);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		throw new IOException("Gave up waiting for server to start after " + this.pgStartupWait.toMillis() + "ms",
				lastCause);
	}

	public void verifyReady(final Map<String, String> connectConfig) throws SQLException {
		final InetAddress localhost = InetAddress.getLoopbackAddress();
		try (Socket sock = new Socket()) {
			sock.setSoTimeout((int) Duration.ofMillis(500).toMillis());
			log.info("Trying to connect to {} on port {}", localhost.getHostName(), this.port);
			sock.connect(new InetSocketAddress(localhost, this.port), (int) Duration.ofMillis(500).toMillis());
		} catch (final IOException e) {
			log.error("Socket connection failed !");
			throw new SQLException("connect failed", e);
		}
		try (Connection c = this.getPostgresDatabase(connectConfig).getConnection();
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

	public String getJdbcUrl(final String userName, final String dbName) {
		return String.format(JDBC_FORMAT, this.port, dbName, userName);
	}

	public DataSource getPostgresDatabase() {
		return this.getDatabase(PG_SUPERUSER, "postgres");
	}

	public DataSource getPostgresDatabase(final Map<String, String> properties) {
		return this.getDatabase(PG_SUPERUSER, "postgres", properties);
	}

	public DataSource getDatabase(final String userName, final String dbName) {
		return this.getDatabase(userName, dbName, Collections.emptyMap());
	}

	public DataSource getDatabase(final String userName, final String dbName, final Map<String, String> properties) {
		final PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerName("localhost");
		ds.setPortNumber(this.port);
		ds.setDatabaseName(dbName);
		ds.setUser(userName);

		log.info("Trying to connect to server {} on port {} for database {} and user {}", ds.getServerName(),
				ds.getPortNumber(), ds.getDatabaseName(), ds.getUser());

		properties.forEach((propertyKey, propertyValue) -> {
			try {
				ds.setProperty(propertyKey, propertyValue);
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		});
		return ds;
	}

	private void cleanDataDirectory(final File parentDirectory) {
		// TODO Auto-generated method stub

	}

	private void initDatabase() {
		final StopWatch watch = new StopWatch();
		watch.start();

		final String binaryPath = this.getBinaryPath(this.postgresDirectory.getPath(), "initdb");
		final String localOptions = this.createLocaleOptions(this.localeConfig).stream()
				.collect(Collectors.joining(" "));
		final String[] commands = { binaryPath, "-A", "trust", "-U", PG_SUPERUSER, "-D", this.dataDirectory.getPath(),
				"-E", "UTF-8", localOptions };
		this.system(log, commands);

		log.info("{} initdb completed in {}", this.instanceId, watch);
	}

	@Override
	public boolean isCleaningDataDirectoryBeforeStartRequired() {
		return this.cleanDataDirectoryBeforeStarting;
	}

	@Override
	public boolean isCleaningDataDirectoryAfterStopRequired() {
		return this.cleanDataDirectoryAfterClosing;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {

		// stop POSTGRES
		if (this.closed.getAndSet(true)) {
			log.warn("Server already stopped");
			return;
		}
		final StopWatch watch = new StopWatch();
		watch.start();
		try {

			final String binaryPath = this.getBinaryPath(this.postgresDirectory.getPath(), "pg_ctl");
			final String[] commands = { binaryPath, "-D", this.dataDirectory.getPath(), "stop", "-m", PG_STOP_MODE,
					"-t", PG_STOP_WAIT_S, "-w" };
			this.system(log, commands);

			log.info("{} shut down postmaster in {}", this.instanceId, watch);
		} catch (final Exception e) {
			log.error("Could not stop postmaster " + this.instanceId, e);
		}

		// clean data (if needed) TODO integrate property access in builder
		if (this.cleanDataDirectoryAfterClosing && System.getProperty("pmp.no-cleanup") == null) {
			try {
				FileUtils.deleteDirectory(this.dataDirectory);
			} catch (final IOException e) {
				log.error("Could not clean up directory {}", this.dataDirectory.getAbsolutePath());
			}
		} else {
			log.info("Did not clean up directory {}", this.dataDirectory.getAbsolutePath());
		}
	}

}

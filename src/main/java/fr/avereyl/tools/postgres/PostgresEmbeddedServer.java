/**
 * 
 */
package fr.avereyl.tools.postgres;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import fr.avereyl.tools.EmbeddedServer;
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

    private File pgDir;

    private Duration pgStartupWait;
    private File dataDirectory;
    private File lockFile;
    private final UUID instanceId = UUID.randomUUID();
    private int port;
    
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Map<String, String> postgresConfig = new HashMap<>();
    private final Map<String, String> localeConfig = new HashMap<>();

    private volatile FileOutputStream lockStream;
    private volatile FileLock lock;
    private boolean cleanDataDirectoryAfterClosing;
    private boolean cleanDataDirectoryBeforeStarting;

    private ProcessBuilder.Redirect errorRedirector;
    private ProcessBuilder.Redirect outputRedirector;

	public static PostgresEmbeddedServer.Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		
		
		public PostgresEmbeddedServer build() {
			//TODO
			return null;
		}
	}

	public EmbeddedServer start() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

}

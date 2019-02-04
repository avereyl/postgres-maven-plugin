package fr.avereyl.tools.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.slf4j.Logger;

/**
 * Read standard output of process and write lines to given {@link Logger} as INFO;
 * depends on {@link ProcessBuilder#redirectErrorStream(boolean)} being set to {@code true} (since only stdout is
 * read).
 *
 * <p>
 * The use of the input stream is threadsafe since it's used only in a single thread&mdash;the one launched by this
 * code.
 */
public class ProcessOutputLogger implements Runnable {
	
    private final Logger logger;
    private final Process process;
    private final BufferedReader reader;

    private ProcessOutputLogger(final Logger logger, final Process process) {
        this.logger = logger;
        this.process = process;
        reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    }

    @Override
    public void run() {
        try {
            while (process.isAlive()) {
                try {
                	Optional.ofNullable(reader.readLine()).ifPresent(logger::info);
                } catch (final IOException e) {
                    logger.error("while reading output", e);
                    return;
                }
            }
        } finally {
            try {
                reader.close();
            } catch (final IOException e) {
                logger.error("caught i/o exception closing reader", e);
            }
        }
    }

    public static void buildAndStart(final Logger logger, final Process process) {
        final Thread t = new Thread(new ProcessOutputLogger(logger, process));
        t.setName("output redirector for " + process);
        t.start();
    }

}

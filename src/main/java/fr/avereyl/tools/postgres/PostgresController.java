package fr.avereyl.tools.postgres;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

import fr.avereyl.tools.utils.ProcessOutputLogger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresController {

	private ProcessBuilder.Redirect errRedirector = ProcessBuilder.Redirect.PIPE;
	private ProcessBuilder.Redirect outRedirector = ProcessBuilder.Redirect.PIPE;

	/**
	 * @see https://www.postgresql.org/docs/current/app-pg-ctl.html
	 * 
	 * @param postgresDirectory
	 * @param dataDirectory
	 * @param action
	 * @param options
	 *            List of options to associate with the command (eg. -m fast -t 5
	 *            -w)
	 */
	public Process pgCtl(File postgresDirectory, File dataDirectory, String action, List<String> options) {
		String optionsString = options.stream().collect(Collectors.joining(" "));
		return system(pgBin("pg_ctl", postgresDirectory.getPath()), action, "-D", dataDirectory.getPath(),
				optionsString);
	}

	/**
	 * @see https://www.postgresql.org/docs/current/app-initdb.html
	 * 
	 * @param postgresDirectory
	 * @param dataDirectory
	 * @param options
	 *            List of options to associate with the command (eg. -A trust -U
	 *            postgres)
	 */
	public Process initDb(File postgresDirectory, File dataDirectory, List<String> options) {
		String optionsString = options.stream().collect(Collectors.joining(" "));
		return system(pgBin("initdb", postgresDirectory.getPath()), optionsString, "-D", dataDirectory.getPath());
	}

	/**
	 * Return the path of the given binary name for the given POSTGRES directory.
	 * 
	 * @param postgresDirectory
	 *            POSTGRES directory
	 * @param binaryName
	 *            Name of the binary
	 * @return Path of the binary as a String
	 */
	public String pgBin(String postgresDirectory, String binaryName) {
		final String extension = SystemUtils.IS_OS_WINDOWS ? ".exe" : "";
		return new File(postgresDirectory, "bin/" + binaryName + extension).getPath();
	}

	/**
	 * Run the given system command with its parameters. Redirecting process error
	 * stream and output stream to the logger of this class using a
	 * {@link ProcessOutputLogger}.
	 * 
	 * @param command
	 *            The command to run with its parameters.
	 */
	private Process system(String... command) {
		try {
			final ProcessBuilder builder = new ProcessBuilder(command);
			builder.redirectErrorStream(true);
			builder.redirectError(errRedirector);
			builder.redirectOutput(outRedirector);
			final Process process = builder.start();

			if (outRedirector.type() == ProcessBuilder.Redirect.Type.PIPE) {
				ProcessOutputLogger.buildAndStart(log, process);
			}
			if (0 != process.waitFor()) {
				throw new IllegalStateException(String.format("Process %s failed%n%s", Arrays.asList(command),
						IOUtils.toString(process.getErrorStream(), Charset.defaultCharset())));
			}
			return process;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

}

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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

import fr.avereyl.tools.utils.StreamGobbler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresController {

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
	public Process pgCtl(final File postgresDirectory, final File dataDirectory, final String action,
			final List<String> options) {
		final String optionsString = options.stream().collect(Collectors.joining(" "));
		return this.system(this.pgBin(postgresDirectory.getPath(), "pg_ctl"), action, "-D", dataDirectory.getPath(),
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
	public Process initDb(final File postgresDirectory, final File dataDirectory, final List<String> options) {
		final String optionsString = options.stream().collect(Collectors.joining(" "));
		return this.system(this.pgBin(postgresDirectory.getPath(), "initdb"), optionsString, "-D",
				dataDirectory.getPath());
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
	public String pgBin(final String postgresDirectory, final String binaryName) {
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
	private Process system(final String... command) {
		log.info("Executing > {}", Arrays.asList(command).stream().collect(Collectors.joining(" ")));
		try {
			final ProcessBuilder builder = new ProcessBuilder(command);
			final Process process = builder.start();
			// handling process input stream with logger info
			StreamGobbler.buildAndStart(process.getInputStream(), log::info);
			// handling process error stream with logger error
			StreamGobbler.buildAndStart(process.getErrorStream(), log::error);
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

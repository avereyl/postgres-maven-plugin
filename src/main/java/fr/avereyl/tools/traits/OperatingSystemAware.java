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
package fr.avereyl.tools.traits;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;

import fr.avereyl.tools.utils.StreamGobbler;

public interface OperatingSystemAware {

	/**
	 * Get current operating system string. The string is used to get the
	 * appropriate POSTGRES binary name.
	 *
	 * @return Current operating system string.
	 */
	default String getOS() {
		if (SystemUtils.IS_OS_WINDOWS) {
			return "Windows";
		}
		if (SystemUtils.IS_OS_MAC_OSX) {
			return "Darwin";
		}
		if (SystemUtils.IS_OS_LINUX) {
			return "Linux";
		}
		throw new UnsupportedOperationException("Unknown OS " + SystemUtils.OS_NAME);
	}

	default int detectPort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	/**
	 * Get the machine architecture string. The string is used to get the
	 * appropriate POSTGRES binary name.
	 *
	 * @return Current machine architecture string.
	 */

	default String getArchitecture() {
		return "amd64".equals(SystemUtils.OS_ARCH) ? "x86_64" : SystemUtils.OS_ARCH;
	}

	/**
	 *
	 * @param localeConfiguration
	 * @return
	 */
	default List<String> createLocaleOptions(final Map<String, String> localeConfiguration) {
		final List<String> localeOptions = new ArrayList<>();
		for (final Entry<String, String> config : localeConfiguration.entrySet()) {
			if (SystemUtils.IS_OS_WINDOWS) {
				localeOptions.add(String.format("--%s=%s", config.getKey(), config.getValue()));
			} else {
				localeOptions.add("--" + config.getKey());
				localeOptions.add(config.getValue());
			}
		}
		return localeOptions;
	}

	/**
	 *
	 * @param port
	 * @param postgresConfiguration
	 * @return
	 */
	default List<String> createInitOptions(final Integer port, final Map<String, String> postgresConfiguration) {
		final List<String> initOptions = new ArrayList<>();
		initOptions.addAll(Arrays.asList("-p", Integer.toString(port), "-F"));

		for (final Entry<String, String> config : postgresConfiguration.entrySet()) {
			initOptions.add("-c");
			initOptions.add(config.getKey() + "=" + config.getValue());
		}
		return initOptions;
	}

	default String getBinaryPath(final String directory, final String binaryName) {
		final String extension = SystemUtils.IS_OS_WINDOWS ? ".exe" : "";
		return new File(directory, "bin/" + binaryName + extension).getPath();
	}

	/**
	 * Run the given system command with its parameters. Redirecting process error
	 * stream and output stream to the logger of this class using a
	 * {@link ProcessOutputLogger}.
	 *
	 * @param command
	 *            The command to run with its parameters.
	 */
	default Process system(final Logger log, final String... command) {
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

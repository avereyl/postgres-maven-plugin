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

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.tukaani.xz.XZInputStream;

import fr.avereyl.tools.traits.FileSystemAware;
import fr.avereyl.tools.traits.OperatingSystemAware;
import lombok.extern.slf4j.Slf4j;

/**
 * Class responsible for extracting (if needed) POSGRES binaries.
 *
 * @author guillaume
 *
 */
@Slf4j
public class PostgresBinaryPreparer implements OperatingSystemAware, FileSystemAware {

	private static final String LOCK_FILE_NAME = "epg-lock";
	private static final Lock BINARIES_PREPARATION_LOCK = new ReentrantLock();
	private static final Map<PostgresBinaryResolver, File> PREPARED_BINARIES = new HashMap<>();

	public File prepare(final PostgresBinaryResolver binaryResolver, final Optional<File> overriddenWorkingDirectory) {
		BINARIES_PREPARATION_LOCK.lock();
		try {
			// binaries already prepared -> return POSTGRES folder
			if (PREPARED_BINARIES.containsKey(binaryResolver)) {
				return PREPARED_BINARIES.get(binaryResolver);
			}
			// preparing binaries...
			final String system = this.getOS();
			final String machineHardware = this.getArchitecture();
			log.info("Detected a {} {} system.", system, machineHardware);
			// getting binary stream for the detected system (through the resolver)
			final InputStream postgresBinaryStream;
			try {
				postgresBinaryStream = binaryResolver.getPostgresBinary(system, machineHardware);
			} catch (final IOException e) {
				throw new ExceptionInInitializerError(e);
			}
			if (postgresBinaryStream == null) {
				throw new IllegalStateException("No POSTGRES binary found for " + system + " / " + machineHardware);
			}

			File postgresDirectory;
			// copy found binary into the working directory
			try (DigestInputStream postgresArchiveData = new DigestInputStream(postgresBinaryStream,
					MessageDigest.getInstance("MD5"));
					ByteArrayOutputStream binaryCopyOutputStream = new ByteArrayOutputStream()) {
				IOUtils.copy(postgresArchiveData, binaryCopyOutputStream);

				final String postgresDigest = Hex.encodeHexString(postgresArchiveData.getMessageDigest().digest());
				final File workingDirectory = overriddenWorkingDirectory.isPresent() ? overriddenWorkingDirectory.get()
						: this.getWorkingDirectory();
				postgresDirectory = new File(workingDirectory, String.format("PG-%s", postgresDigest));
				// creating the directory where to extract POSTGRES binaries (nothing done if
				// directory already exists)
				this.mkdirs(postgresDirectory);
				final File unpackLockFile = new File(postgresDirectory, LOCK_FILE_NAME);
				final File postgresDirectoryExists = new File(postgresDirectory, ".exists");
				// only in case no .exists file present in the directory
				if (!postgresDirectoryExists.exists()) {
					this.unpack(binaryCopyOutputStream, postgresDirectory, unpackLockFile, postgresDirectoryExists);
				}
			} catch (final IOException | NoSuchAlgorithmException e) {
				throw new ExceptionInInitializerError(e);
			} catch (final InterruptedException ie) {
				Thread.currentThread().interrupt();
				throw new ExceptionInInitializerError(ie);
			}

			PREPARED_BINARIES.put(binaryResolver, postgresDirectory);
			log.info("Postgres binaries at {}", postgresDirectory);
			return postgresDirectory;

		} finally {
			BINARIES_PREPARATION_LOCK.unlock();
		}
	}

	private void unpack(final ByteArrayOutputStream binaryCopyOutputStream, final File postgresDirectory,
			final File unpackLockFile, final File postgresDirectoryExists) throws InterruptedException, IOException {
		try (FileOutputStream lockStream = new FileOutputStream(unpackLockFile);
				FileLock unpackLock = lockStream.getChannel().tryLock()) {
			if (unpackLock != null) {
				// unpacking lock acquired
				try {
					if (postgresDirectoryExists.exists()) {
						throw new IllegalStateException(
								"unpack lock acquired but .exists file is present " + postgresDirectoryExists);
					}
					log.info("Extracting POSTGRES...");
					try (ByteArrayInputStream bais = new ByteArrayInputStream(binaryCopyOutputStream.toByteArray())) {
						this.extractTxz(bais, postgresDirectory.getPath());
					}
					if (!postgresDirectoryExists.createNewFile()) {
						throw new IllegalStateException("couldn't make .exists file " + postgresDirectoryExists);
					}
				} catch (final Exception e) {
					log.error("while unpacking Postgres", e);
				}
			} else {
				// the other guy is unpacking for us.
				int maxAttempts = 60;
				while (!postgresDirectoryExists.exists() && --maxAttempts > 0) {
					Thread.sleep(1000L);
				}
				if (!postgresDirectoryExists.exists()) {
					throw new IllegalStateException(
							"Waited 60 seconds for postgres to be unpacked but it never finished!");
				}
			}
		} finally {
			if (unpackLockFile.exists() && !unpackLockFile.delete()) {
				log.error("could not remove lock file {}", unpackLockFile.getAbsolutePath());
			}
		}
	}

	/**
	 * Unpack archive compressed by tar with xz compression. By default system tar
	 * is used (faster). If not found, then the java implementation takes place.
	 *
	 * @param stream
	 *            A stream with the postgres binaries.
	 * @param targetDir
	 *            The directory to extract the content to.
	 */
	private void extractTxz(final InputStream stream, final String targetDir) throws IOException {
		try (XZInputStream xzIn = new XZInputStream(stream);
				TarArchiveInputStream tarIn = new TarArchiveInputStream(xzIn)) {
			final Phaser phaser = new Phaser(1);
			TarArchiveEntry entry;

			while ((entry = tarIn.getNextTarEntry()) != null) {
				final String individualFile = entry.getName();
				final File fsObject = new File(targetDir + "/" + individualFile);

				if (entry.isSymbolicLink() || entry.isLink()) {
					final Path target = FileSystems.getDefault().getPath(entry.getLinkName());
					Files.createSymbolicLink(fsObject.toPath(), target);
				} else if (entry.isFile()) {
					final byte[] content = new byte[(int) entry.getSize()];
					final int read = tarIn.read(content, 0, content.length);
					if (read == -1) {
						throw new IllegalStateException("could not read " + individualFile);
					}
					this.mkdirs(fsObject.getParentFile());

					final AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(fsObject.toPath(),
							CREATE_NEW, WRITE);
					final ByteBuffer buffer = ByteBuffer.wrap(content);

					phaser.register();

					fileChannel.write(buffer, 0, fileChannel, new CompletionHandler<Integer, Channel>() {
						@Override
						public void completed(final Integer written, final Channel channel) {
							this.closeChannel(channel);
						}

						@Override
						public void failed(final Throwable error, final Channel channel) {
							log.error("Could not write file {}", fsObject.getAbsolutePath(), error);
							this.closeChannel(channel);
						}

						private void closeChannel(final Channel channel) {
							try {
								channel.close();
							} catch (final IOException e) {
								log.error("Unexpected error while closing the channel", e);
							} finally {
								phaser.arriveAndDeregister();
							}
						}
					});

				} else if (entry.isDirectory()) {
					this.mkdirs(fsObject);
				} else {
					throw new UnsupportedOperationException(
							String.format("Unsupported entry found: %s", individualFile));
				}

				if (individualFile.startsWith("bin/") || individualFile.startsWith("./bin/")) {
					final boolean success = fsObject.setExecutable(true);
					if (!success) {
						log.warn("Failed to set {} executable !", individualFile);
					}
				}
			}

			phaser.arriveAndAwaitAdvance();
		}
	}

}

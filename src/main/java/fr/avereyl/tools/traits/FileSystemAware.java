package fr.avereyl.tools.traits;

import java.io.File;

public interface FileSystemAware {

	/**
	 * Get the working directory from system property pmp.working-dir (if defined).
	 * Fallbacks to <java.io.tmpdir>/embedded-pg
	 * 
	 * @return The working directory
	 */
	default File getWorkingDirectory() {
		final File tempWorkingDirectory = new File(System.getProperty("java.io.tmpdir"), "embedded-pg");
		return new File(System.getProperty("pmp.working-dir", tempWorkingDirectory.getPath()));
	}

	/**
	 * Create the given directory. (Do nothing if the directory already exists)
	 * @param directory to be created.
	 */
	default void mkdirs(File directory) {
		if (!directory.mkdirs() && !(directory.isDirectory() && directory.exists())) {
			throw new IllegalStateException("Could not create " + directory);
		}
	}

}

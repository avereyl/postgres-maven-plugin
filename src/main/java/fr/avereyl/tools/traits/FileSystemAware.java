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
	 * 
	 * @param directory
	 *            to be created.
	 */
	default void mkdirs(File directory) {
		if (!directory.mkdirs() && !(directory.isDirectory() && directory.exists())) {
			throw new IllegalStateException("Could not create " + directory);
		}
	}

}

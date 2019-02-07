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
package fr.avereyl.tools.mojo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import fr.avereyl.tools.postgres.PostgresEmbeddedServer;

/**
 *
 * @author guillaume
 *
 */
@Mojo(name = "start", defaultPhase = LifecyclePhase.INITIALIZE, requiresProject = false)
public class StartPostgresMojo extends AbstractPostgresMojo {

	/*
	 * (non-Javadoc)
	 *
	 * @see fr.avereyl.tools.AbstractPostgresMojo#doExecute()
	 */
	@Override
	protected void doExecute() throws MojoExecutionException, MojoFailureException {
		try {
			this.server = PostgresEmbeddedServer.builder()
					// build server according mojo parameters
					.port(this.port).pgdir(this.pgdir).datadir(this.datadir)
					// .config("-U", "postgres")
					.build();
			final Map<String, String> connectionConfig = new HashMap<>();
			// connectionConfig.put("-U", "postgres");

			// TODO add connection config from mojo parameter
			this.server.start(connectionConfig);
		} catch (final IOException e) {
			this.getLog().error(e.getMessage());
			throw new MojoExecutionException("Unable to start the server.", e);
		}
	}

}

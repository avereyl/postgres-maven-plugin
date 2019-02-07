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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;

import fr.avereyl.tools.EmbeddedServer;

/**
 * @author guillaume
 *
 */
public abstract class AbstractPostgresMojo extends AbstractMojo {

	/**
	 * The port to start POSTGRES on.
	 */
	@Parameter(property = "postgres.port")
	public int port = 0;

	/**
	 * The schema to use for the database.
	 */
	@Parameter(property = "postgres.schema", defaultValue = "public")
	public String schema;

	/**
	 * The name to use for the database.
	 */
	@Parameter(property = "postgres.name", defaultValue = "postgresdb")
	public String name;

	/**
	 * The directory where storing postgres engine.
	 */
	@Parameter(property = "postgres.pgdir", defaultValue = "target/postgresdb")
	public String pgdir;
	/**
	 * The directory where storing data.
	 */
	@Parameter(property = "postgres.datadir", defaultValue = "target/postgresdb/data")
	public String datadir;

	/**
	 * The username to use when authenticating.
	 */
	@Parameter(property = "postgres.username", defaultValue = "postgres")
	public String username;

	/**
	 * The password to use when authenticating.
	 */
	@Parameter(property = "postgres.password", defaultValue = "postgres")
	public String password;

	/**
	 * The absolute class name of the driver.
	 */
	@Parameter(property = "postgres.driver", defaultValue = "org.postgresql.Driver")
	public String driver;

	/**
	 * The URL to use when connecting.
	 */
	@Parameter(property = "postgres.connectionURL")
	public String connectionURL;

	/**
	 * The POSTGRES validation query.
	 */
	@Parameter(property = "postgres.validationQuery", defaultValue = "SELECT 1")
	public String validationQuery;

	/**
	 * Whether to bypass running POSTGRES.
	 */
	@Parameter(property = "postgres.skip")
	public boolean skip;

	/**
	 * Delegates the mojo execution to {@link #doExecute()} after initializing the
	 * {@link Server} for localhost
	 * 
	 * @throws MojoExecutionException
	 * @throws MojoFailureException
	 */
	@Override
	public void execute() throws MojoExecutionException, MojoFailureException {
		if (this.skip) {
			this.getLog().info("Skipping POSTGRES execution.");
			return;
		}
		this.setup();
		this.doExecute();
	}

	/**
	 * Shared {@link EmbeddedServer} instance for all mojos.
	 */
	protected EmbeddedServer server;

	protected void setup() throws MojoExecutionException {
		// TODO Auto-generated method stub

	}

	protected abstract void doExecute() throws MojoExecutionException, MojoFailureException;

}

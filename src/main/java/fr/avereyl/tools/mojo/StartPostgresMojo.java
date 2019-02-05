/**
 * 
 */
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
			server = PostgresEmbeddedServer.builder()
					// build server according mojo parameters
					.port(this.port)
					.pgdir(this.pgdir)
					.datadir(this.datadir)
					//.config("key", "value")
					.build();
			Map<String, String> connectionConfig = new HashMap<>();
			
			//TODO add connection config from mojo parameter
			server.start(connectionConfig);
		} catch (IOException e) {
			getLog().error(e.getMessage());
			throw new MojoExecutionException("Unable to start the server.", e);
		}
	}

}

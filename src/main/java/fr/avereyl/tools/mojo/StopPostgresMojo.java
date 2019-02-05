/**
 * 
 */
package fr.avereyl.tools.mojo;

import java.io.IOException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * 
 * @author guillaume
 *
 */
@Mojo(name = "stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST, requiresProject = false)
public class StopPostgresMojo extends AbstractPostgresMojo {

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.avereyl.tools.AbstractPostgresMojo#doExecute()
	 */
	@Override
	protected void doExecute() throws MojoExecutionException, MojoFailureException {
		try {
			server.close();
		} catch (IOException e) {
			getLog().error(e.getMessage());
			throw new MojoExecutionException("Unable to stop the server.", e);
		}
	}

}

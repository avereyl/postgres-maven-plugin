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
		// TODO fail if already running...
		try {
			
			server.start();
		} catch (IOException e) {
			getLog().error("");
			throw new MojoExecutionException("", e);
		}
	}

}

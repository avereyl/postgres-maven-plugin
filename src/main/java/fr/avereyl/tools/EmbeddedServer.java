/**
 * 
 */
package fr.avereyl.tools;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * @author guillaume
 *
 */
public interface EmbeddedServer extends Closeable {

	EmbeddedServer start(Map<String, String> connectionConfig) throws IOException;

	boolean isCleaningDataDirectoryBeforeStartRequired();
	boolean isCleaningDataDirectoryAfterStopRequired();
}

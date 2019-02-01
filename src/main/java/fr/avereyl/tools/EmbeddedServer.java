/**
 * 
 */
package fr.avereyl.tools;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * @author guillaume
 *
 */
public interface EmbeddedServer extends Closeable {

	EmbeddedServer start() throws IOException;

	
	default int detectPort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}
}

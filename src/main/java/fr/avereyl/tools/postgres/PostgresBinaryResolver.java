/**
 * 
 */
package fr.avereyl.tools.postgres;

import java.io.IOException;
import java.io.InputStream;

/**
 * A strategy for resolving PostgreSQL binaries.
 * 
 * @author guillaume
 *
 */
public interface PostgresBinaryResolver {

	/**
     * Returns an input stream with the postgres binary for the given
     * system and hardware architecture.
     * @param system a system identification (Darwin, Linux...)
     * @param machineHardware a machine hardware architecture (x86_64...)
     * @return the binary
     */
    InputStream getPostgresBinary(String system, String machineHardware) throws IOException;

}

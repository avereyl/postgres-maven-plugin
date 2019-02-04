package fr.avereyl.tools.postgres;

import static java.lang.String.format;

import java.io.InputStream;

/**
 * Resolves pre-bundled binaries from within the JAR file.
 */
public class BundledPostgresBinaryResolver implements PostgresBinaryResolver {

	@Override
    public InputStream getPostgresBinary(String system, String machineHardware) {
        return PostgresEmbeddedServer.class.getResourceAsStream(format("/postgresql-%s-%s.txz", system, machineHardware));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}

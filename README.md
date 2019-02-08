# postgres-maven-plugin
Start and Stop embedded Postgresql DB directly from Maven

```xml
<plugin>
	<groupId>fr.avereyl.tools</groupId>
	<artifactId>postgres-maven-plugin</artifactId>
	<version>0.0.1-SNAPSHOT</version>
	<!-- call start and stop -->
	<executions>
		<execution>
			<id>start-postgres</id>
			<phase>initialize</phase>
			<goals>
				<goal>start</goal>
			</goals>
		</execution>
		<execution>
			<id>stop-postgres</id>
			<phase>post-integration-test</phase>
			<goals>
				<goal>stop</goal>
			</goals>
		</execution>
	</executions>
</plugin>
```
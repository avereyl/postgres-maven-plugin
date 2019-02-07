/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.avereyl.tools.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class StreamGobbler extends Thread {

	private InputStream inputStream;
	private Consumer<String> lineConsumer;

	private StreamGobbler(final InputStream inputStream, final Consumer<String> lineConsumer) {
		this.inputStream = inputStream;
		this.lineConsumer = lineConsumer;
	}

	@Override
	public void run() {
		new BufferedReader(new InputStreamReader(this.inputStream)).lines().forEach(this.lineConsumer);
	}

	public static StreamGobbler buildAndStart(final InputStream inputStream, final Consumer<String> lineConsumer) {
		final StreamGobbler streamGobbler = new StreamGobbler(inputStream, lineConsumer);
		streamGobbler.start();
		return streamGobbler;
	}

}

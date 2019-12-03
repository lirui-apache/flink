/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive;

import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;

import com.klarna.hiverunner.HiveServerContext;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEHISTORYFILELOC;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALSCRATCHDIR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCHDIR;

/**
 * Sub-class of FlinkEmbeddedHiveRunner. Relies on the super class to setup the hive shell,
 * and launches a standalone HMS instead of using the embedded one.
 */
public class FlinkStandaloneHiveRunner extends FlinkEmbeddedHiveRunner {
	private static final Logger LOGGER = LoggerFactory.getLogger(FlinkStandaloneHiveRunner.class);
	private static final Duration HMS_START_TIMEOUT = Duration.ofSeconds(90);
	private Future<Void> hmsWatcher;
	private int hmsPort;

	public FlinkStandaloneHiveRunner(Class<?> clazz) throws InitializationError {
		super(clazz);
	}

	@Override
	protected List<TestRule> classRules() {
		try {
			hmsPort = HiveTestUtils.getFreePort();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		List<TestRule> rules = super.classRules();

		HiveServerContext context = null;
		for (TestRule rule : rules) {
			if (rule instanceof HiveShellResource) {
				context = ((HiveShellResource) rule).context;
				break;
			}
		}
		Preconditions.checkNotNull(context, "Cannot retrieve HiveServerContext from parent's test rules");
		ExternalResource hms = new HMSResource(context);
		// insert hms after hive shell
		for (int i = 0; i < rules.size(); i++) {
			if (rules.get(i) instanceof HiveShellResource) {
				rules.add(i + 1, hms);
				break;
			}
		}
		return rules;
	}

	class HMSResource extends ExternalResource {

		private final HiveServerContext context;

		HMSResource(HiveServerContext context) {
			this.context = context;
		}

		@Override
		protected void before() throws Throwable {
			long start = System.currentTimeMillis();
			// change metastore uri to our standalone HMS
			context.getHiveConf().setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + hmsPort);
			hmsWatcher = startHMS(context, hmsPort);
			System.out.println("HMS took: " + Duration.ofMillis(System.currentTimeMillis() - start).getSeconds());
		}

		@Override
		protected void after() {
			if (hmsWatcher != null) {
				hmsWatcher.cancel(true);
			}
		}
	}

	/**
	 * Launches HMS process and returns a Future representing that process.
	 */
	private static Future<Void> startHMS(HiveServerContext context, int port) throws Exception {
		context.init();
		HiveConf outsideConf = context.getHiveConf();
		List<String> args = new ArrayList<>();
		String javaHome = System.getProperty("java.home");
		args.add(Joiner.on(File.separator).join(javaHome, "bin", "java"));
		// set classpath
		args.add("-cp");
		args.add(System.getProperty("java.class.path"));

		// set sys properties
		args.add(hiveCmdLineConfig(METASTOREWAREHOUSE.varname, outsideConf.getVar(METASTOREWAREHOUSE)));
		args.add(hiveCmdLineConfig(SCRATCHDIR.varname, outsideConf.getVar(SCRATCHDIR)));
		args.add(hiveCmdLineConfig(LOCALSCRATCHDIR.varname, outsideConf.getVar(LOCALSCRATCHDIR)));
		args.add(hiveCmdLineConfig(HIVEHISTORYFILELOC.varname, outsideConf.getVar(HIVEHISTORYFILELOC)));
		// The following config is removed in Hive 3.1.0.
		args.add(hiveCmdLineConfig("hive.warehouse.subdir.inherit.perms",
				String.valueOf(outsideConf.getBoolean("hive.warehouse.subdir.inherit.perms", true))));
		args.add(hiveCmdLineConfig("hadoop.tmp.dir", outsideConf.get("hadoop.tmp.dir")));
		args.add(hiveCmdLineConfig("test.log.dir", outsideConf.get("test.log.dir")));
		args.add(hiveCmdLineConfig(METASTORECONNECTURLKEY.varname, outsideConf.getVar(METASTORECONNECTURLKEY)));
		// config derby.log file
		File derbyLog = File.createTempFile("derby", ".log");
		derbyLog.deleteOnExit();
		args.add(hiveCmdLineConfig("derby.stream.error.file", derbyLog.getAbsolutePath()));

		args.add(HiveMetaStore.class.getCanonicalName());
		args.add("-p");
		args.add(String.valueOf(port));

		ProcessBuilder builder = new ProcessBuilder(args);
		Process process = builder.start();
		Thread inLogger = new Thread(new LogRedirect(process.getInputStream(), LOGGER));
		Thread errLogger = new Thread(new LogRedirect(process.getErrorStream(), LOGGER));
		inLogger.setDaemon(true);
		inLogger.setName("HMS-IN-Logger");
		errLogger.setDaemon(true);
		errLogger.setName("HMS-ERR-Logger");
		inLogger.start();
		errLogger.start();

		FutureTask<Void> res = new FutureTask<>(() -> {
			try {
				int r = process.waitFor();
				inLogger.join();
				errLogger.join();
				if (r != 0) {
					throw new RuntimeException("HMS process exited with " + r);
				}
			} catch (InterruptedException e) {
				LOGGER.info("Shutting down HMS");
			} finally {
				if (process.isAlive()) {
					// give it a chance to terminate gracefully
					process.destroy();
					try {
						process.waitFor(5, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						LOGGER.info("Interrupted waiting for HMS to shut down, killing it forcibly");
					}
					process.destroyForcibly();
				}
			}
		}, null);
		Thread thread = new Thread(res);
		thread.setName("HMS-Watcher");
		// we need the watcher thread to kill HMS, don't make it daemon
		thread.setDaemon(false);
		thread.start();
		waitForHMSStart(port);
		return res;
	}

	private static void waitForHMSStart(int port) throws Exception {
		final long deadline = System.currentTimeMillis() + HMS_START_TIMEOUT.toMillis();
		while (System.currentTimeMillis() < deadline) {
			try (SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", port))) {
				LOGGER.info("HMS started at port {}", port);
				return;
			} catch (ConnectException e) {
				LOGGER.info("Waiting for HMS to start...");
				Thread.sleep(1000);
			}
		}
		throw new java.util.concurrent.TimeoutException("Timeout waiting for HMS to start");
	}

	private static String hiveCmdLineConfig(String name, String val) {
		return String.format("-D%s=%s", name, val);
	}

	private static class LogRedirect implements Runnable {
		private final InputStream inputStream;
		private final Logger logger;

		LogRedirect(InputStream inputStream, Logger logger) {
			this.inputStream = inputStream;
			this.logger = logger;
		}

		@Override
		public void run() {
			try {
				new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(logger::info);
			} catch (Exception e) {
				logger.error(Throwables.getStackTraceAsString(e));
			}
		}
	}
}

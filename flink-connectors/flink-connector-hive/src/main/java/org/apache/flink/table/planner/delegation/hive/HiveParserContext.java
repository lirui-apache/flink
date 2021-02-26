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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserExplainConfiguration;
import org.apache.flink.table.planner.delegation.hive.parse.HiveParserExplainConfiguration.AnalyzeState;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.Context.
 */
public class HiveParserContext {

	public static final PathFilter HIDDEN_FILES_PATH_FILTER = p -> {
		String name = p.getName();
		return !name.startsWith("_") && !name.startsWith(".");
	};

	private boolean isHDFSCleanup;
	private Path resFile;
	private Path resDir;
	private FileSystem resFs;
	private static final Logger LOG = LoggerFactory.getLogger("hive.ql.HiveParserContext");
	private Path[] resDirPaths;
	private int resDirFilesNum;
	boolean initialized;
	String originalTracker = null;
	private final Map<String, ContentSummary> pathToCS = new ConcurrentHashMap<>();

	// scratch path to use for all non-local (ie. hdfs) file system tmp folders
	private final Path nonLocalScratchPath;

	// scratch directory to use for local file system tmp folders
	private final String localScratchDir;

	// the permission to scratch directory (local and hdfs)
	private final String scratchDirPermission;

	// Keeps track of scratch directories created for different scheme/authority
	private final Map<String, Path> fsScratchDirs = new HashMap<String, Path>();

	private final Configuration conf;
	protected int pathid = 10000;
	protected HiveParserExplainConfiguration explainConfig = null;
	protected String cboInfo;
	protected boolean cboSucceeded;
	protected String cmd = "";
	// number of previous attempts
	protected int tryCount = 0;
	private TokenRewriteStream tokenRewriteStream;
	// Holds the qualified name to tokenRewriteStream for the views
	// referenced by the query. This is used to rewrite the view AST
	// with column masking and row filtering policies.
	private final Map<String, TokenRewriteStream> viewsTokenRewriteStreams;

	private final String executionId;

	// List of Locks for this query
	protected List<HiveLock> hiveLocks;

	// Transaction manager for this query
	protected HiveTxnManager hiveTxnManager;

	private boolean needLockMgr;

	private AtomicInteger sequencer = new AtomicInteger();

	private final Map<String, Table> cteTables = new HashMap<>();

	// Keep track of the mapping from load table desc to the output and the lock
	private final Map<LoadTableDesc, WriteEntity> loadTableOutputMap =
			new HashMap<>();
	private final Map<WriteEntity, List<HiveLockObj>> outputLockObjects =
			new HashMap<>();

	private final String stagingDir;

	private boolean skipTableMasking;

	// Identify whether the query involves an UPDATE, DELETE or MERGE
	private boolean isUpdateDeleteMerge;

	private Map<Integer, DestClausePrefix> insertBranchToNamePrefix = new HashMap<>();
	private Operation operation = Operation.OTHER;

	public void setOperation(Operation operation) {
		this.operation = operation;
	}

	/**
	 * These ops require special handling in various places (note that Insert into Acid table is in OTHER category).
	 */
	public enum Operation {UPDATE, DELETE, MERGE, OTHER}

	/**
	 * DestClausePrefix.
	 */
	public enum DestClausePrefix {
		INSERT("insclause-"), UPDATE("updclause-"), DELETE("delclause-");
		private final String prefix;

		DestClausePrefix(String prefix) {
			this.prefix = prefix;
		}

		public String toString() {
			return prefix;
		}
	}

	private String getMatchedText(ASTNode n) {
		return getTokenRewriteStream().toString(n.getTokenStartIndex(), n.getTokenStopIndex() + 1).trim();
	}

	/**
	 * The suffix is always relative to a given ASTNode.
	 */
	public DestClausePrefix getDestNamePrefix(ASTNode curNode) {
		assert curNode != null : "must supply curNode";
		if (curNode.getType() != HiveASTParser.TOK_INSERT_INTO) {
			//select statement
			assert curNode.getType() == HiveASTParser.TOK_DESTINATION;
			if (operation == Operation.OTHER) {
				//not an 'interesting' op
				return DestClausePrefix.INSERT;
			}
			//if it is an 'interesting' op but it's a select it must be a sub-query or a derived table
			//it doesn't require a special Acid code path - the reset of the code here is to ensure
			//the tree structure is what we expect
			boolean thisIsInASubquery = false;
			parentLoop:
			while (curNode.getParent() != null) {
				curNode = (ASTNode) curNode.getParent();
				switch (curNode.getType()) {
					case HiveASTParser.TOK_SUBQUERY_EXPR:
						//this is a real subquery (foo IN (select ...))
					case HiveASTParser.TOK_SUBQUERY:
						//this is a Derived Table Select * from (select a from ...))
						//strictly speaking SetOps should have a TOK_SUBQUERY parent so next 6 items are redundant
					case HiveASTParser.TOK_UNIONALL:
					case HiveASTParser.TOK_UNIONDISTINCT:
					case HiveASTParser.TOK_EXCEPTALL:
					case HiveASTParser.TOK_EXCEPTDISTINCT:
					case HiveASTParser.TOK_INTERSECTALL:
					case HiveASTParser.TOK_INTERSECTDISTINCT:
						thisIsInASubquery = true;
						break parentLoop;
				}
			}
			if (!thisIsInASubquery) {
				throw new IllegalStateException("Expected '" + getMatchedText(curNode) + "' to be in sub-query or set operation.");
			}
			return DestClausePrefix.INSERT;
		}
		switch (operation) {
			case OTHER:
				return DestClausePrefix.INSERT;
			case UPDATE:
				return DestClausePrefix.UPDATE;
			case DELETE:
				return DestClausePrefix.DELETE;
			case MERGE:
      /* This is the structrue expected here
        HiveASTParser.TOK_QUERY;
          HiveASTParser.TOK_FROM
          HiveASTParser.TOK_INSERT;
            HiveASTParser.TOK_INSERT_INTO;
          HiveASTParser.TOK_INSERT;
            HiveASTParser.TOK_INSERT_INTO;
          .....*/
				ASTNode insert = (ASTNode) curNode.getParent();
				assert insert != null && insert.getType() == HiveASTParser.TOK_INSERT;
				ASTNode query = (ASTNode) insert.getParent();
				assert query != null && query.getType() == HiveASTParser.TOK_QUERY;

				for (int childIdx = 1; childIdx < query.getChildCount(); childIdx++) {//1st child is TOK_FROM
					assert query.getChild(childIdx).getType() == HiveASTParser.TOK_INSERT;
					if (insert == query.getChild(childIdx)) {
						DestClausePrefix prefix = insertBranchToNamePrefix.get(childIdx);
						if (prefix == null) {
							throw new IllegalStateException("Found a node w/o branch mapping: '" +
									getMatchedText(insert) + "'");
						}
						return prefix;
					}
				}
				throw new IllegalStateException("Could not locate '" + getMatchedText(insert) + "'");
			default:
				throw new IllegalStateException("Unexpected operation: " + operation);
		}
	}

	public HiveParserContext(Configuration conf) throws IOException {
		this(conf, generateExecutionId());
	}

	/**
	 * Create a HiveParserContext with a given executionId.  ExecutionId, together with
	 * user name and conf, will determine the temporary directory locations.
	 */
	public HiveParserContext(Configuration conf, String executionId) {
		this.conf = conf;
		this.executionId = executionId;

		// local & non-local tmp location is configurable. however it is the same across
		// all external file systems
		nonLocalScratchPath = new Path(SessionState.getHDFSSessionPath(conf), executionId);
		localScratchDir = new Path(SessionState.getLocalSessionPath(conf), executionId).toUri().getPath();
		scratchDirPermission = HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION);
		stagingDir = conf.get("hive.exec.stagingdir", ".hive-staging");

		viewsTokenRewriteStreams = new HashMap<>();
	}

	// Find whether we should execute the current query due to explain.
	public boolean isExplainSkipExecution() {
		return (explainConfig != null && explainConfig.getAnalyze() != AnalyzeState.RUNNING);
	}

	public AnalyzeState getExplainAnalyze() {
		if (explainConfig != null) {
			return explainConfig.getAnalyze();
		}
		return null;
	}

	/**
	 * Gets a temporary staging directory related to a path.
	 * If a path already contains a staging directory, then returns the current directory; otherwise
	 * create the directory if needed.
	 *
	 * @param inputPath URI of the temporary directory
	 * @param mkdir     Create the directory if True.
	 * @return A temporary path.
	 */
	private Path getStagingDir(Path inputPath, boolean mkdir) {
		final URI inputPathUri = inputPath.toUri();
		final String inputPathName = inputPathUri.getPath();
		final String fileSystem = inputPathUri.getScheme() + ":" + inputPathUri.getAuthority();
		final FileSystem fs;

		try {
			fs = inputPath.getFileSystem(conf);
		} catch (IOException e) {
			throw new IllegalStateException("Error getting FileSystem for " + inputPath + ": " + e, e);
		}

		String stagingPathName;
		if (inputPathName.indexOf(stagingDir) == -1) {
			stagingPathName = new Path(inputPathName, stagingDir).toString();
		} else {
			stagingPathName = inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length());
		}

		final String key = fileSystem + "-" + stagingPathName + "-" + TaskRunner.getTaskRunnerID();

		Path dir = fsScratchDirs.get(key);
		if (dir == null) {
			// Append task specific info to stagingPathName, instead of creating a sub-directory.
			// This way we don't have to worry about deleting the stagingPathName separately at
			// end of query execution.
			dir = fs.makeQualified(new Path(stagingPathName + "_" + this.executionId + "-" + TaskRunner.getTaskRunnerID()));

			LOG.debug("Created staging dir = " + dir + " for path = " + inputPath);

			if (mkdir) {
				try {
					boolean inheritPerms = Boolean.parseBoolean(conf.get("hive.warehouse.subdir.inherit.perms", "true"));
					if (!fs.mkdirs(dir)) {
						throw new IllegalStateException("Cannot create staging directory  '" + dir.toString() + "'");
					}

					if (isHDFSCleanup) {
						fs.deleteOnExit(dir);
					}
				} catch (IOException e) {
					throw new RuntimeException("Cannot create staging directory '" + dir.toString() + "': " + e.getMessage(), e);
				}
			}

			fsScratchDirs.put(key, dir);
		}

		return dir;
	}

	// Get a tmp directory on specified URI.
	private Path getScratchDir(String scheme, String authority,
			boolean mkdir, String scratchDir) {

		String fileSystem = scheme + ":" + authority;
		Path dir = fsScratchDirs.get(fileSystem + "-" + TaskRunner.getTaskRunnerID());

		if (dir == null) {
			Path dirPath = new Path(scheme, authority,
					scratchDir + "-" + TaskRunner.getTaskRunnerID());
			if (mkdir) {
				try {
					FileSystem fs = dirPath.getFileSystem(conf);
					dirPath = new Path(fs.makeQualified(dirPath).toString());
					FsPermission fsPermission = new FsPermission(scratchDirPermission);

					if (!fs.mkdirs(dirPath, fsPermission)) {
						throw new RuntimeException("Cannot make directory: "
								+ dirPath.toString());
					}
					if (isHDFSCleanup) {
						fs.deleteOnExit(dirPath);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			dir = dirPath;
			fsScratchDirs.put(fileSystem + "-" + TaskRunner.getTaskRunnerID(), dir);

		}

		return dir;
	}

	/**
	 * Create a local scratch directory on demand and return it.
	 */
	public Path getLocalScratchDir(boolean mkdir) {
		try {
			FileSystem fs = FileSystem.getLocal(conf);
			URI uri = fs.getUri();
			return getScratchDir(uri.getScheme(), uri.getAuthority(),
					mkdir, localScratchDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a map-reduce scratch directory on demand and return it.
	 */
	public Path getMRScratchDir() {

		// if we are executing entirely on the client side - then
		// just (re)use the local scratch directory
		if (isLocalOnlyExecutionMode()) {
			return getLocalScratchDir(!isExplainSkipExecution());
		}

		try {
			Path dir = FileUtils.makeQualified(nonLocalScratchPath, conf);
			URI uri = dir.toUri();

			Path newScratchDir = getScratchDir(uri.getScheme(), uri.getAuthority(),
					!isExplainSkipExecution(), uri.getPath());
			LOG.info("New scratch dir is " + newScratchDir);
			return newScratchDir;
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Error while making MR scratch "
					+ "directory - check filesystem config (" + e.getCause() + ")", e);
		}
	}

	private Path getExternalScratchDir(URI extURI) {
		return getStagingDir(new Path(extURI.getScheme(), extURI.getAuthority(), extURI.getPath()), !isExplainSkipExecution());
	}

	/**
	 * Remove any created scratch directories.
	 */
	public void removeScratchDir() {
		for (Map.Entry<String, Path> entry : fsScratchDirs.entrySet()) {
			try {
				Path p = entry.getValue();
				FileSystem fs = p.getFileSystem(conf);
				LOG.debug("Deleting scratch dir: {}", p);
				fs.delete(p, true);
				fs.cancelDeleteOnExit(p);
			} catch (Exception e) {
				LOG.warn("Error Removing Scratch: "
						+ StringUtils.stringifyException(e));
			}
		}
		fsScratchDirs.clear();
	}

	/**
	 * Remove any created directories for CTEs.
	 */
	public void removeMaterializedCTEs() {
		// clean CTE tables
		for (Table materializedTable : cteTables.values()) {
			Path location = materializedTable.getDataLocation();
			try {
				FileSystem fs = location.getFileSystem(conf);
				boolean status = fs.delete(location, true);
				LOG.info("Removed " + location + " for materialized "
						+ materializedTable.getTableName() + ", status=" + status);
			} catch (IOException e) {
				// ignore
				LOG.warn("Error removing " + location + " for materialized " + materializedTable.getTableName() +
						": " + StringUtils.stringifyException(e));
			}
		}
		cteTables.clear();
	}

	private String nextPathId() {
		return Integer.toString(pathid++);
	}

	private static final String MR_PREFIX = "-mr-";
	private static final String EXT_PREFIX = "-ext-";
	private static final String LOCAL_PREFIX = "-local-";

	public Path getMRTmpPath(URI uri) {
		return new Path(getStagingDir(new Path(uri), !isExplainSkipExecution()), MR_PREFIX + nextPathId());
	}

	/**
	 * Get a path to store map-reduce intermediate data in.
	 *
	 * @return next available path for map-red intermediate data
	 */
	public Path getMRTmpPath() {
		return new Path(getMRScratchDir(), MR_PREFIX + nextPathId());
	}

	/**
	 * Get a tmp path on local host to store intermediate data.
	 *
	 * @return next available tmp path on local fs
	 */
	public Path getLocalTmpPath() {
		return new Path(getLocalScratchDir(true), LOCAL_PREFIX + nextPathId());
	}

	/**
	 * This is similar to getExternalTmpPath() with difference being this method returns temp path
	 * within passed in uri, whereas getExternalTmpPath() ignores passed in path and returns temp
	 * path within /tmp.
	 */
	public Path getExtTmpPathRelTo(Path path) {
		return new Path(getStagingDir(path, !isExplainSkipExecution()), EXT_PREFIX + nextPathId());
	}

	/**
	 * @return the resFile
	 */
	public Path getResFile() {
		return resFile;
	}

	/**
	 * @param resFile the resFile to set
	 */
	public void setResFile(Path resFile) {
		this.resFile = resFile;
		resDir = null;
		resDirPaths = null;
		resDirFilesNum = 0;
	}

	/**
	 * @param resDir the resDir to set
	 */
	public void setResDir(Path resDir) {
		this.resDir = resDir;
		resFile = null;

		resDirFilesNum = 0;
		resDirPaths = null;
	}

	public void clear() throws IOException {
		if (resDir != null) {
			try {
				FileSystem fs = resDir.getFileSystem(conf);
				LOG.debug("Deleting result dir: {}", resDir);
				fs.delete(resDir, true);
			} catch (IOException e) {
				LOG.info("HiveParserContext clear error: " + StringUtils.stringifyException(e));
			}
		}

		if (resFile != null) {
			try {
				FileSystem fs = resFile.getFileSystem(conf);
				LOG.debug("Deleting result file: {}", resFile);
				fs.delete(resFile, false);
			} catch (IOException e) {
				LOG.info("HiveParserContext clear error: " + StringUtils.stringifyException(e));
			}
		}
		removeMaterializedCTEs();
		removeScratchDir();
		originalTracker = null;
		setNeedLockMgr(false);
	}

	/**
	 * Set the token rewrite stream being used to parse the current top-level SQL
	 * statement. Note that this should <b>not</b> be used for other parsing
	 * activities; for example, when we encounter a reference to a view, we switch
	 * to a new stream for parsing the stored view definition from the catalog,
	 * but we don't clobber the top-level stream in the context.
	 *
	 * @param tokenRewriteStream the stream being used
	 */
	public void setTokenRewriteStream(TokenRewriteStream tokenRewriteStream) {
		assert (this.tokenRewriteStream == null || this.getExplainAnalyze() == AnalyzeState.RUNNING);
		this.tokenRewriteStream = tokenRewriteStream;
	}

	/**
	 * @return the token rewrite stream being used to parse the current top-level
	 * SQL statement, or null if it isn't available (e.g. for parser
	 * tests)
	 */
	public TokenRewriteStream getTokenRewriteStream() {
		return tokenRewriteStream;
	}

	public void addViewTokenRewriteStream(String viewFullyQualifiedName,
			TokenRewriteStream tokenRewriteStream) {
		viewsTokenRewriteStreams.put(viewFullyQualifiedName, tokenRewriteStream);
	}

	/**
	 * Generate a unique executionId.  An executionId, together with user name and
	 * the configuration, will determine the temporary locations of all intermediate
	 * files.
	 * In the future, users can use the executionId to resume a query.
	 */
	public static String generateExecutionId() {
		Random rand = new Random();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS");
		String executionId = "hive_" + format.format(new Date()) + "_"
				+ Math.abs(rand.nextLong());
		return executionId;
	}

	/**
	 * Does Hive wants to run tasks entirely on the local machine
	 * (where the query is being compiled)?
	 * Today this translates into running hadoop jobs locally
	 */
	public boolean isLocalOnlyExecutionMode() {
		// Always allow spark to run in a cluster mode. Without this, depending on
		// user's local hadoop settings, true may be returned, which causes plan to be
		// stored in local path.
		if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
			return false;
		}

		return ShimLoader.getHadoopShims().isLocalMode(conf);
	}

	public Configuration getConf() {
		return conf;
	}

	public void setNeedLockMgr(boolean needLockMgr) {
		this.needLockMgr = needLockMgr;
	}
}

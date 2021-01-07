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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.filesystem.FileSystemFactory;

import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/** Hive {@link FileSystemFactory}, hive need use job conf to create file system. */
public class HadoopFileSystemFactory implements FileSystemFactory {

    private static final long serialVersionUID = 1L;

    private JobConfWrapper jobConfWrapper;

    HadoopFileSystemFactory(JobConf jobConf) {
        this.jobConfWrapper = new JobConfWrapper(jobConf);
    }

    @Override
    public FileSystem create(URI uri) throws IOException {
        UserGroupInformation ugi = getUGI(jobConfWrapper.conf());
        try {
            return ugi.doAs(
                    (PrivilegedExceptionAction<HadoopFileSystem>)
                            () ->
                                    new HadoopFileSystem(
                                            new Path(uri).getFileSystem(jobConfWrapper.conf())));
        } catch (InterruptedException e) {
            throw new FlinkHiveException(e);
        }
    }

    public static UserGroupInformation getUGI(Configuration conf) {
        String userName = conf.get(HiveOptions.HIVE_USER_NAME.key());
        try {
            return StringUtils.isNullOrWhitespaceOnly(userName)
                    ? UserGroupInformation.getCurrentUser()
                    : UserGroupInformation.createProxyUser(
                            userName, UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            throw new FlinkHiveException("Failed to create UGI", e);
        }
    }
}

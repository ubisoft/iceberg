/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.io;

import java.lang.reflect.Field;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Contains ported code snippets from later Hive sources. We should get rid of this class as soon as Hive 4 is released
 * and Iceberg makes a dependency to that version.
 */
public class OrcSplit extends org.apache.hadoop.hive.ql.io.orc.OrcSplit {

  public OrcSplit(Path path, long offset, long length, long fileLen, Path rootDir) {
    try {
      Field fielFs = this.getClass().getSuperclass().getSuperclass().getDeclaredField("fs");
      fielFs.setAccessible(true);
      fielFs.set(this, new FileSplit(path, offset, length, null));
      Field fieldRootDir = this.getClass().getSuperclass().getDeclaredField("rootDir");
      fieldRootDir.setAccessible(true);
      fieldRootDir.set(this, rootDir);
      Field fieldProjColsUncompressedSize = this.getClass().getSuperclass()
              .getDeclaredField("projColsUncompressedSize");
      fieldProjColsUncompressedSize.setAccessible(true);
      fieldProjColsUncompressedSize.setLong(this, length);
      Field fieldFileLen = this.getClass().getSuperclass().getDeclaredField("fileLen");
      fieldFileLen.setAccessible(true);
      fieldFileLen.setLong(this, fileLen <= 0 ? Long.MAX_VALUE : fileLen);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}

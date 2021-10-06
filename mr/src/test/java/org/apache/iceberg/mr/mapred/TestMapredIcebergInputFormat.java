/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.mapred;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestIcebergInputFormats;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * MapReduce InputFormat API V1 test for Iceberg.
 *
 * This test class is used inside of TestHiveIcebergInputFormat but it needs to access getSplitsUnchecked function
 * of MapredIcebergInputformat. This forces us to locate it inside of the package org.apache.iceberg.mr.mapred
 *
 * @param <T> Java class of records constructed by Iceberg; default is {@link Record}
 */
public class TestMapredIcebergInputFormat<T> extends TestIcebergInputFormats.TestInputFormat<T> {

  private TestMapredIcebergInputFormat(List<IcebergSplit> splits, List<T> records) {
    super(splits, records);
  }

  public static <T> TestMapredIcebergInputFormat<T> create(Configuration conf) {
    JobConf job = new JobConf(conf);
    MapredIcebergInputFormat<T> inputFormat = new MapredIcebergInputFormat<>();

    try {
      org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplitsUnchecked(job);

      List<IcebergSplit> iceSplits = Lists.newArrayListWithCapacity(splits.length);
      List<T> records = new ArrayList<>();

      for (org.apache.hadoop.mapred.InputSplit split : splits) {
        iceSplits.add((IcebergSplit) split);
        org.apache.hadoop.mapred.RecordReader<Void, Container<T>>
            reader = inputFormat.getRecordReader(split, job, Reporter.NULL);
        try {
          Container<T> container = reader.createValue();

          while (reader.next(null, container)) {
            records.add(container.get());
          }
        } finally {
          reader.close();
        }
      }

      return new TestMapredIcebergInputFormat<>(iceSplits, records);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }
}

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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerWithEngine {

  private static final String[] EXECUTION_ENGINES = new String[] {"tez", "mr"};

  private static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()),
          required(4, "product_id", Types.LongType.get())
  );

  private static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d, 1L)
          .add(101L, 0L, 22.22d, 2L)
          .add(102L, 1L, 33.33d, 3L)
          .build();

  private static final Schema PRODUCT_SCHEMA = new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "price", Types.DoubleType.get())
  );

  private static final List<Record> PRODUCT_RECORDS = TestHelper.RecordsBuilder.newInstance(PRODUCT_SCHEMA)
          .add(1L, "skirt", 11.11d)
          .add(2L, "tee", 22.22d)
          .add(3L, "watch", 33.33d)
          .build();

  private static final List<Type> SUPPORTED_TYPES =
          ImmutableList.of(Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
                  Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimestampType.withZone(),
                  Types.TimestampType.withoutZone(), Types.StringType.get(), Types.BinaryType.get(),
                  Types.DecimalType.of(3, 1), Types.UUIDType.get(), Types.FixedType.ofLength(5),
                  Types.TimeType.get());

  private static final Schema COMPLEX_SCHEMA = new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "array_string", Types.ListType.ofOptional(3, Types.StringType.get()))
  );

  private static final List<Record> COMPLEX_RECORDS = TestHelper.RecordsBuilder.newInstance(COMPLEX_SCHEMA)
          .add(100L, Arrays.asList("v11", "v12"))
          .add(200L, Arrays.asList("v21"))
          .add(300L, Arrays.asList("v31", "v32"))
          .build();

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}, isVectorized={3}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();
    String javaVersion = System.getProperty("java.specification.version");

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      for (String engine : EXECUTION_ENGINES) {
        // include Tez tests only for Java 8
        if (javaVersion.equals("1.8") || "mr".equals(engine)) {
          testParams.add(new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, false});
          // test for vectorization=ON in case of ORC format and Tez engine
          if (fileFormat == FileFormat.ORC && "tez".equals(engine) && MetastoreUtil.hive3PresentOnClasspath()) {
            testParams.add(new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, true});
          }
        }
      }
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET) and execution engine (mr)
    // skip HiveCatalog tests as they are added before
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[]{FileFormat.PARQUET, "mr", testTableType, false});
      }
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public String executionEngine;

  @Parameter(2)
  public TestTables.TestTableType testTableType;

  @Parameter(3)
  public boolean isVectorized;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(200_000, TimeUnit.MILLISECONDS);

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, executionEngine);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    // Mixing mr and tez jobs within the same JVM can cause problems. Mr jobs set the ExecMapper status to done=false
    // at the beginning and to done=true at the end. However, tez jobs also rely on this value to see if they should
    // proceed, but they do not reset it to done=false at the beginning. Therefore, without calling this after each test
    // case, any tez job that follows a completed mr job will erroneously read done=true and will not proceed.
    ExecMapper.setDone(false);
  }

  @Test
  public void testVectorizationComplexDisable() throws IOException {
    shell.setHiveSessionValue("hive.vectorized.complex.types.enabled", false);
    String tableName = "complex";
    shell.executeStatement("DROP TABLE IF EXISTS default." + tableName);
    testTables.createTable(shell, tableName, COMPLEX_SCHEMA, fileFormat, COMPLEX_RECORDS);

    List<Object[]> rows = shell.executeStatement("SELECT id, st FROM default." + tableName
            + " Lateral view explode(array_string) t as st order by id,st"
    );

    Assert.assertEquals(5, rows.size());
    Assert.assertArrayEquals(new Object[] {100L, "v11"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {100L, "v12"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {200L, "v21"}, rows.get(2));
    Assert.assertArrayEquals(new Object[] {300L, "v31"}, rows.get(3));
    Assert.assertArrayEquals(new Object[] {300L, "v32"}, rows.get(4));
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DebeziumTransformer implements Transformer {

  @Override
  public Dataset apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                       TypedProperties properties) {

    Dataset<Row> insertedOrUpdatedData = rowDataset
        .select("ts_ms", "op", "after.*")
        .withColumnRenamed("ts_ms", "_ts_ms")
        .withColumnRenamed("op", "_op")
        .filter(rowDataset.col("op").notEqual("d"));

    Dataset<Row> deletedData = rowDataset
        .select("ts_ms", "op", "before.*")
        .withColumnRenamed("ts_ms", "_ts_ms")
        .withColumnRenamed("op", "_op")
        .filter(rowDataset.col("op").equalTo("d"));

    final Dataset<Row> transformedData = insertedOrUpdatedData.union(deletedData);

    return transformedData;
  }
}
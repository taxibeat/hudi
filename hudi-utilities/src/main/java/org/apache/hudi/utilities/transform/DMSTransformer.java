package org.apache.hudi.utilities.transform;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class DMSTransformer implements Transformer {

    private static final String OP_FIELD = "Op";
    protected static final Logger LOG = LogManager.getLogger(DMSTransformer.class);

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
        Dataset<Row> augmentedWithOp;
        if (java.util.Arrays.asList(rowDataset.schema().fieldNames()).contains(OP_FIELD)) {
            augmentedWithOp = rowDataset.na().fill("I", new String[]{OP_FIELD});
        } else {
            augmentedWithOp = rowDataset.withColumn(OP_FIELD, lit("I"));
        }

        // Reorder columns
        Column[] cols = ArrayUtils.add(Arrays.stream(rowDataset.columns())
                .filter(c -> !OP_FIELD.equalsIgnoreCase(c))
                .map(functions::col)
                .toArray(Column[]::new), 0, col(OP_FIELD));

         return augmentedWithOp.select(cols);
    }
}

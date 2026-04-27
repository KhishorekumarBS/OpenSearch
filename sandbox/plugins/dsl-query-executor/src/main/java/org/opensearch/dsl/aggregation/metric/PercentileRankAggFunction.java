/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

/**
 * Custom Calcite aggregate function representing an OpenSearch percentile_ranks aggregation.
 * Carries the requested values to compute ranks for as metadata for downstream execution.
 */
public class PercentileRankAggFunction extends SqlAggFunction {

    private final double[] values;

    /**
     * Creates a percentile rank aggregate function.
     *
     * @param values the values to compute ranks for
     */
    public PercentileRankAggFunction(double[] values) {
        super(
            "PERCENTILE_RANKS",
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.NUMERIC,
            SqlFunctionCategory.NUMERIC,
            false,
            false,
            Optionality.FORBIDDEN
        );
        this.values = values.clone();
    }

    /** Returns the values to compute ranks for. */
    public double[] getValues() {
        return values.clone();
    }
}

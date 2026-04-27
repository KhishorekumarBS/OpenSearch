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
 * Custom Calcite aggregate function representing an OpenSearch percentiles aggregation.
 * Carries the requested percentile values as metadata for downstream execution.
 */
public class PercentileAggFunction extends SqlAggFunction {

    private final double[] percents;

    /**
     * Creates a percentile aggregate function.
     *
     * @param percents the percentile values to compute
     */
    public PercentileAggFunction(double[] percents) {
        super(
            "PERCENTILES",
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
        this.percents = percents.clone();
    }

    /** Returns the requested percentile values. */
    public double[] getPercents() {
        return percents.clone();
    }
}

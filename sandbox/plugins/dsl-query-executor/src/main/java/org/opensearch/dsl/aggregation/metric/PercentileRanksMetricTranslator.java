/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Translates a percentile_ranks metric aggregation to multiple Calcite {@link AggregateCall}s,
 * one per requested value.
 */
public class PercentileRanksMetricTranslator implements MetricTranslator<PercentileRanksAggregationBuilder> {

    /** Creates a percentile ranks metric translator. */
    public PercentileRanksMetricTranslator() {}

    @Override
    public Class<PercentileRanksAggregationBuilder> getAggregationType() {
        return PercentileRanksAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(PercentileRanksAggregationBuilder agg, RelDataType rowType) throws ConversionException {
        String fieldName = agg.field();
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
        }

        double[] values = agg.values();
        List<AggregateCall> calls = new ArrayList<>(values.length);
        for (double v : values) {
            calls.add(
                AggregateCall.create(
                    new PercentileRankAggFunction(new double[] { v }),
                    false,
                    false,
                    false,
                    Collections.singletonList(field.getIndex()),
                    -1,
                    RelCollations.EMPTY,
                    field.getType(),
                    agg.getName() + "_" + v
                )
            );
        }
        return calls;
    }

    @Override
    public List<String> getAggregateFieldNames(PercentileRanksAggregationBuilder agg) {
        double[] values = agg.values();
        List<String> names = new ArrayList<>(values.length);
        for (double v : values) {
            names.add(agg.getName() + "_" + v);
        }
        return names;
    }

    // TODO: implement response conversion (InternalTDigestPercentileRanks / InternalHDRPercentileRanks)
    @Override
    public InternalAggregation toInternalAggregation(String name, Map<String, Object> values) {
        throw new UnsupportedOperationException("toInternalAggregation not yet implemented for PercentileRanksMetricTranslator");
    }
}

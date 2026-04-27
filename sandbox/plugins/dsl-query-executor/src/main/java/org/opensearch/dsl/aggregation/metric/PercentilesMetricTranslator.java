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
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Translates a percentiles metric aggregation to multiple Calcite {@link AggregateCall}s,
 * one per requested percentile value.
 */
public class PercentilesMetricTranslator implements MetricTranslator<PercentilesAggregationBuilder> {

    /** Creates a percentiles metric translator. */
    public PercentilesMetricTranslator() {}

    @Override
    public Class<PercentilesAggregationBuilder> getAggregationType() {
        return PercentilesAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(PercentilesAggregationBuilder agg, RelDataType rowType) throws ConversionException {
        String fieldName = agg.field();
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
        }

        double[] percents = agg.percentiles();
        List<AggregateCall> calls = new ArrayList<>(percents.length);
        for (double p : percents) {
            calls.add(
                AggregateCall.create(
                    new PercentileAggFunction(new double[] { p }),
                    false,
                    false,
                    false,
                    Collections.singletonList(field.getIndex()),
                    -1,
                    RelCollations.EMPTY,
                    field.getType(),
                    agg.getName() + "_" + p
                )
            );
        }
        return calls;
    }

    @Override
    public List<String> getAggregateFieldNames(PercentilesAggregationBuilder agg) {
        double[] percents = agg.percentiles();
        List<String> names = new ArrayList<>(percents.length);
        for (double p : percents) {
            names.add(agg.getName() + "_" + p);
        }
        return names;
    }

    // TODO: implement response conversion (InternalTDigestPercentiles / InternalHDRPercentiles)
    @Override
    public InternalAggregation toInternalAggregation(String name, Map<String, Object> values) {
        throw new UnsupportedOperationException("toInternalAggregation not yet implemented for PercentilesMetricTranslator");
    }
}

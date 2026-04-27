/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class PercentileRanksMetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testWithValues() throws ConversionException {
        PercentileRanksMetricTranslator translator = new PercentileRanksMetricTranslator();
        PercentileRanksAggregationBuilder agg = new PercentileRanksAggregationBuilder("rank_price", new double[] { 100, 500, 1000 }).field(
            "price"
        );
        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(3, calls.size());
        for (AggregateCall call : calls) {
            assertEquals(SqlKind.OTHER_FUNCTION, call.getAggregation().getKind());
            assertEquals(1, call.getArgList().get(0).intValue()); // price is index 1
        }
        assertEquals("rank_price_100.0", calls.get(0).getName());
        assertEquals("rank_price_500.0", calls.get(1).getName());
        assertEquals("rank_price_1000.0", calls.get(2).getName());
    }

    public void testWithDoubleField() throws ConversionException {
        PercentileRanksMetricTranslator translator = new PercentileRanksMetricTranslator();
        PercentileRanksAggregationBuilder agg = new PercentileRanksAggregationBuilder("rank_rating", new double[] { 3.0, 4.5 }).field(
            "rating"
        );
        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(2, calls.size());
        assertEquals("rank_rating_3.0", calls.get(0).getName());
        assertEquals("rank_rating_4.5", calls.get(1).getName());
        assertEquals(3, calls.get(0).getArgList().get(0).intValue()); // rating is index 3
    }

    public void testThrowsForUnknownField() {
        PercentileRanksMetricTranslator translator = new PercentileRanksMetricTranslator();
        expectThrows(
            ConversionException.class,
            () -> translator.toAggregateCalls(
                new PercentileRanksAggregationBuilder("bad", new double[] { 100 }).field("nonexistent"),
                ctx.getRowType()
            )
        );
    }

    public void testAggregationType() {
        PercentileRanksMetricTranslator translator = new PercentileRanksMetricTranslator();
        assertEquals(PercentileRanksAggregationBuilder.class, translator.getAggregationType());
    }

    public void testAggregateFieldNames() {
        PercentileRanksMetricTranslator translator = new PercentileRanksMetricTranslator();
        List<String> names = translator.getAggregateFieldNames(
            new PercentileRanksAggregationBuilder("rank_price", new double[] { 100, 500 }).field("price")
        );
        assertEquals(2, names.size());
        assertEquals("rank_price_100.0", names.get(0));
        assertEquals("rank_price_500.0", names.get(1));
    }
}

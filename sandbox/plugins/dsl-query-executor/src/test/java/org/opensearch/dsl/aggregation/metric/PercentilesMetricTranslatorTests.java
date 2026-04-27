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
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class PercentilesMetricTranslatorTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testDefaultPercents() throws ConversionException {
        PercentilesMetricTranslator translator = new PercentilesMetricTranslator();
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pct_price").field("price");
        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(7, calls.size()); // default: 1, 5, 25, 50, 75, 95, 99
        for (AggregateCall call : calls) {
            assertEquals(SqlKind.OTHER_FUNCTION, call.getAggregation().getKind());
            assertEquals(1, call.getArgList().get(0).intValue()); // price is index 1
        }
        assertEquals("pct_price_1.0", calls.get(0).getName());
        assertEquals("pct_price_50.0", calls.get(3).getName());
        assertEquals("pct_price_99.0", calls.get(6).getName());
    }

    public void testCustomPercents() throws ConversionException {
        PercentilesMetricTranslator translator = new PercentilesMetricTranslator();
        PercentilesAggregationBuilder agg = new PercentilesAggregationBuilder("pct_rating").field("rating").percentiles(10, 50, 90);
        List<AggregateCall> calls = translator.toAggregateCalls(agg, ctx.getRowType());

        assertEquals(3, calls.size());
        assertEquals("pct_rating_10.0", calls.get(0).getName());
        assertEquals("pct_rating_50.0", calls.get(1).getName());
        assertEquals("pct_rating_90.0", calls.get(2).getName());
        assertEquals(3, calls.get(0).getArgList().get(0).intValue()); // rating is index 3
    }

    public void testThrowsForUnknownField() {
        PercentilesMetricTranslator translator = new PercentilesMetricTranslator();
        expectThrows(
            ConversionException.class,
            () -> translator.toAggregateCalls(new PercentilesAggregationBuilder("bad").field("nonexistent"), ctx.getRowType())
        );
    }

    public void testAggregationType() {
        PercentilesMetricTranslator translator = new PercentilesMetricTranslator();
        assertEquals(PercentilesAggregationBuilder.class, translator.getAggregationType());
    }

    public void testAggregateFieldNames() {
        PercentilesMetricTranslator translator = new PercentilesMetricTranslator();
        List<String> names = translator.getAggregateFieldNames(
            new PercentilesAggregationBuilder("pct_price").field("price").percentiles(25, 75)
        );
        assertEquals(2, names.size());
        assertEquals("pct_price_25.0", names.get(0));
        assertEquals("pct_price_75.0", names.get(1));
    }
}

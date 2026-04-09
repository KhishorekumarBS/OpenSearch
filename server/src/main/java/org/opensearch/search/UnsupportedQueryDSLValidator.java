/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Validates search requests and throws errors for unsupported DSL query types.
 *
 * @opensearch.internal
 */
public final class UnsupportedQueryDSLValidator {

    private UnsupportedQueryDSLValidator() {}

    private static final Set<String> UNSUPPORTED_QUERIES = new HashSet<>(Arrays.asList(
        // Geo queries
        "geo_distance", "geo_bounding_box", "geo_shape",
        // Span queries
        "span_term", "span_not", "span_within", "span_containing", "span_first",
        "span_near", "span_gap", "span_or", "span_multi", "span_field_masking",
        // Scoring/compound queries
        "boosting", "constant_score", "function_score", "dis_max", "script_score",
        // Join queries
        "nested", "has_child", "has_parent", "parent_id",
        // Script queries
        "script",
        // Other queries
        "terms_set", "ids", "intervals", "wrapper", "percolate",
        // Hybrid (plugin-based, included for completeness)
        "hybrid"
    ));

    private static final Set<String> UNSUPPORTED_AGGS = new HashSet<>(Arrays.asList(
        // Metric aggregations
        "median_absolute_deviation", "percentile_ranks", "matrix_stats",
        "scripted_metric", "weighted_avg", "geo_bounds", "geo_centroid", "top_hits",
        // Bucket aggregations
        "global", "missing", "sampler", "diversified_sampler",
        "significant_terms", "significant_text", "rare_terms",
        "adjacency_matrix", "geohash_grid", "geohex_grid", "geotile_grid",
        "geo_distance", "children", "parent", "ip_range",
        "nested", "reverse_nested"
    ));

    private static final Set<String> UNSUPPORTED_PIPELINE_AGGS = new HashSet<>(Arrays.asList(
        "extended_stats_bucket", "percentiles_bucket", "derivative",
        "cumulative_sum", "serial_diff", "bucket_script", "bucket_selector",
        "moving_avg", "moving_fn"
    ));

    /**
     * Validates a SearchRequest and throws IllegalArgumentException for unsupported DSL features.
     */
    public static void validate(SearchRequest searchRequest) {
        if (searchRequest.scroll() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [scroll]");
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source == null) {
            return;
        }

        validateTopLevelFeatures(source);
        validateSorts(source);

        if (source.query() != null) {
            validateQuery(source.query());
        }
        if (source.aggregations() != null) {
            validateAggregations(source.aggregations());
        }
    }

    private static void validateTopLevelFeatures(SearchSourceBuilder source) {
        if (source.postFilter() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [post_filter]");
        }
        if (source.collapse() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [collapse]");
        }
        if (source.highlighter() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [highlight]");
        }
        if (source.suggest() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [suggest]");
        }
        if (source.scriptFields() != null && !source.scriptFields().isEmpty()) {
            throw new IllegalArgumentException("Unsupported DSL feature: [script_fields]");
        }
        if (source.storedFields() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [stored_fields]");
        }
        if (source.pointInTimeBuilder() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [point_in_time]");
        }
        if (source.rescores() != null && !source.rescores().isEmpty()) {
            throw new IllegalArgumentException("Unsupported DSL feature: [rescore]");
        }
        if (source.trackScores()) {
            throw new IllegalArgumentException("Unsupported DSL feature: [scoring (track_scores)]");
        }
        if (source.minScore() != null) {
            throw new IllegalArgumentException("Unsupported DSL feature: [scoring (min_score)]");
        }
        if ((source.getDerivedFieldsObject() != null && !source.getDerivedFieldsObject().isEmpty())
            || (source.getDerivedFields() != null && !source.getDerivedFields().isEmpty())) {
            throw new IllegalArgumentException("Unsupported DSL feature: [derived_fields]");
        }
    }

    private static void validateSorts(SearchSourceBuilder source) {
        if (source.sorts() == null) {
            return;
        }
        for (SortBuilder<?> sort : source.sorts()) {
            String sortName = sort.getWriteableName();
            if ("_score".equals(sortName)) {
                throw new IllegalArgumentException("Unsupported DSL feature: [score sort]");
            }
            if ("_geo_distance".equals(sortName)) {
                throw new IllegalArgumentException("Unsupported DSL feature: [geo_distance sort]");
            }
            if ("_script".equals(sortName)) {
                throw new IllegalArgumentException("Unsupported DSL feature: [script sort]");
            }
        }
    }

    private static void validateQuery(QueryBuilder query) {
        String name = query.getName();

        if ("range".equals(name) && query instanceof RangeQueryBuilder) {
            if (((RangeQueryBuilder) query).format() != null) {
                throw new IllegalArgumentException("Unsupported DSL feature: [range query with format option]");
            }
        }

        if (UNSUPPORTED_QUERIES.contains(name)) {
            throw new IllegalArgumentException("Unsupported DSL query type: [" + name + "]");
        }

        // Recurse into bool query clauses
        if (query instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
            boolQuery.must().forEach(UnsupportedQueryDSLValidator::validateQuery);
            boolQuery.mustNot().forEach(UnsupportedQueryDSLValidator::validateQuery);
            boolQuery.should().forEach(UnsupportedQueryDSLValidator::validateQuery);
            boolQuery.filter().forEach(UnsupportedQueryDSLValidator::validateQuery);
        }
    }

    private static void validateAggregations(AggregatorFactories.Builder aggs) {
        for (AggregationBuilder agg : aggs.getAggregatorFactories()) {
            validateAggregation(agg);
        }
        for (PipelineAggregationBuilder pipelineAgg : aggs.getPipelineAggregatorFactories()) {
            String type = pipelineAgg.getType();
            if (UNSUPPORTED_PIPELINE_AGGS.contains(type)) {
                throw new IllegalArgumentException("Unsupported DSL pipeline aggregation type: [" + type + "]");
            }
        }
    }

    private static void validateAggregation(AggregationBuilder agg) {
        String type = agg.getType();
        if (UNSUPPORTED_AGGS.contains(type)) {
            throw new IllegalArgumentException("Unsupported DSL aggregation type: [" + type + "]");
        }
        // Block scripts inside value-source aggregations (e.g. terms agg with script)
        if (agg instanceof ValuesSourceAggregationBuilder) {
            if (((ValuesSourceAggregationBuilder<?>) agg).script() != null) {
                throw new IllegalArgumentException("Unsupported DSL feature: [script in " + type + " aggregation]");
            }
        }
        // Recurse into sub-aggregations
        for (AggregationBuilder subAgg : agg.getSubAggregations()) {
            validateAggregation(subAgg);
        }
        for (PipelineAggregationBuilder pipelineAgg : agg.getPipelineAggregations()) {
            String pipelineType = pipelineAgg.getType();
            if (UNSUPPORTED_PIPELINE_AGGS.contains(pipelineType)) {
                throw new IllegalArgumentException("Unsupported DSL pipeline aggregation type: [" + pipelineType + "]");
            }
        }
    }
}

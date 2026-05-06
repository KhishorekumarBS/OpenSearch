/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.analytics.EngineContext;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class TransportDslExecuteActionTests extends OpenSearchTestCase {

    public void testDoExecuteRejectsWithUnsupportedOperationException() {
        TransportDslExecuteAction action = new TransportDslExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            mock(EngineContext.class),
            (plan, ctx) -> Collections.emptyList(),
            mock(ClusterService.class),
            mock(IndexNameExpressionResolver.class),
            mock(ThreadPool.class)
        );

        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder());

        AtomicReference<Exception> failure = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse r) {
                fail("Expected failure but got response");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });

        assertNotNull(failure.get());
        assertTrue(failure.get() instanceof UnsupportedOperationException);
        assertEquals("DSL is currently not supported. Please use PPL instead.", failure.get().getMessage());
    }
}

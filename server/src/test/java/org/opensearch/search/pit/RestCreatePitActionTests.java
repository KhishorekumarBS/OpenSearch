/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestCreatePitAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests to verify behavior of create pit rest action
 */
public class RestCreatePitActionTests extends OpenSearchTestCase {
    public void testRestCreatePit() throws Exception {
        RestCreatePitAction action = new RestCreatePitAction();
        Map<String, String> params = new HashMap<>();
        params.put("keep_alive", "1m");
        params.put("allow_partial_pit_creation", "false");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), containsString("Unsupported DSL feature: [point_in_time]"));
    }

    public void testRestCreatePitDefaultPartialCreation() throws Exception {
        RestCreatePitAction action = new RestCreatePitAction();
        Map<String, String> params = new HashMap<>();
        params.put("keep_alive", "1m");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
            .withMethod(RestRequest.Method.POST)
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), containsString("Unsupported DSL feature: [point_in_time]"));
    }
}

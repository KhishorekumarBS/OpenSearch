/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.scroll;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestSearchScrollAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.containsString;

public class RestSearchScrollActionTests extends OpenSearchTestCase {

    public void testParseSearchScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestSearchScrollAction action = new RestSearchScrollAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            MediaTypeRegistry.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), containsString("Unsupported DSL feature: [scroll]"));
    }

    public void testBodyParamsOverrideQueryStringParams() throws Exception {
        RestSearchScrollAction action = new RestSearchScrollAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{\"scroll_id\":\"BODY\", \"scroll\":\"1m\"}"),
            MediaTypeRegistry.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), containsString("Unsupported DSL feature: [scroll]"));
    }
}

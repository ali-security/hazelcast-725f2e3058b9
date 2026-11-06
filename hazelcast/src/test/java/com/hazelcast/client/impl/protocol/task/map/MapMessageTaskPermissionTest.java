/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.protocol.codec.MapFetchKeysCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

import java.security.Permission;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the {@link com.hazelcast.core.IMap} iteration, query cache and partition lost listener
 * client message tasks require a {@link MapPermission}, so an authenticated client without the proper
 * map permission can neither read the map entries nor subscribe to or destroy its query caches.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapMessageTaskPermissionTest {

    private static final String MAP_NAME = "myMap";
    private static final String QUERY_CACHE_NAME = "myQueryCache";
    private static final String[] MAP_ACTIONS = {
            ActionConstants.ACTION_CREATE,
            ActionConstants.ACTION_DESTROY,
            ActionConstants.ACTION_PUT,
            ActionConstants.ACTION_REMOVE,
            ActionConstants.ACTION_READ,
            ActionConstants.ACTION_LISTEN,
            ActionConstants.ACTION_LOCK,
            ActionConstants.ACTION_INDEX,
            ActionConstants.ACTION_INTERCEPT,
    };

    private Node node;
    private Connection connection;

    @Before
    public void setup() {
        ClientEngine clientEngine = mock(ClientEngine.class);
        when(clientEngine.getEndpointManager()).thenReturn(mock(ClientEndpointManager.class));

        node = mock(Node.class);
        Whitebox.setInternalState(node, "nodeEngine", mock(NodeEngineImpl.class));
        Whitebox.setInternalState(node, "clientEngine", clientEngine);

        connection = mock(Connection.class);
    }

    @Test
    public void fetchEntries_requiresReadPermission() {
        MapFetchEntriesCodec.RequestParameters parameters = new MapFetchEntriesCodec.RequestParameters();
        parameters.name = MAP_NAME;
        MapFetchEntriesMessageTask task = new MapFetchEntriesMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_READ, withParameters(task, parameters));
    }

    @Test
    public void fetchKeys_requiresReadPermission() {
        MapFetchKeysCodec.RequestParameters parameters = new MapFetchKeysCodec.RequestParameters();
        parameters.name = MAP_NAME;
        MapFetchKeysMessageTask task = new MapFetchKeysMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_READ, withParameters(task, parameters));
    }

    @Test
    public void publisherCreate_requiresListenPermission() {
        ContinuousQueryPublisherCreateCodec.RequestParameters parameters
                = new ContinuousQueryPublisherCreateCodec.RequestParameters();
        parameters.mapName = MAP_NAME;
        parameters.cacheName = QUERY_CACHE_NAME;
        MapPublisherCreateMessageTask task = new MapPublisherCreateMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void publisherCreateWithValue_requiresListenPermission() {
        ContinuousQueryPublisherCreateWithValueCodec.RequestParameters parameters
                = new ContinuousQueryPublisherCreateWithValueCodec.RequestParameters();
        parameters.mapName = MAP_NAME;
        parameters.cacheName = QUERY_CACHE_NAME;
        MapPublisherCreateWithValueMessageTask task = new MapPublisherCreateWithValueMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void setReadCursor_requiresReadPermission() {
        ContinuousQuerySetReadCursorCodec.RequestParameters parameters
                = new ContinuousQuerySetReadCursorCodec.RequestParameters();
        parameters.mapName = MAP_NAME;
        parameters.cacheName = QUERY_CACHE_NAME;
        MapSetReadCursorMessageTask task = new MapSetReadCursorMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_READ, withParameters(task, parameters));
    }

    @Test
    public void destroyQueryCache_requiresDestroyPermission() {
        ContinuousQueryDestroyCacheCodec.RequestParameters parameters = new ContinuousQueryDestroyCacheCodec.RequestParameters();
        parameters.mapName = MAP_NAME;
        parameters.cacheName = QUERY_CACHE_NAME;
        MapDestroyCacheMessageTask task = new MapDestroyCacheMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_DESTROY, withParameters(task, parameters));
    }

    @Test
    public void removePartitionLostListener_requiresListenPermission() {
        MapRemovePartitionLostListenerCodec.RequestParameters parameters
                = new MapRemovePartitionLostListenerCodec.RequestParameters();
        parameters.name = MAP_NAME;
        MapRemovePartitionLostListenerMessageTask task = new MapRemovePartitionLostListenerMessageTask(null, node, connection);

        assertMapPermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    private static <T extends AbstractMessageTask> T withParameters(T task, Object parameters) {
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private static void assertMapPermissionRequired(String action, AbstractMessageTask task) {
        Permission required = task.getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new MapPermission(MAP_NAME, action), required);

        // only the expected (or all) action on the right map grants access
        assertTrue(new MapPermission(MAP_NAME, action).implies(required));
        assertTrue(new MapPermission(MAP_NAME, ActionConstants.ACTION_ALL).implies(required));
        for (String otherAction : MAP_ACTIONS) {
            if (!otherAction.equals(action)) {
                assertFalse(otherAction + " must not grant " + action,
                        new MapPermission(MAP_NAME, otherAction).implies(required));
            }
        }
        assertFalse(new MapPermission("anotherMap", ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new MapPermission(QUERY_CACHE_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new CachePermission(MAP_NAME, ActionConstants.ACTION_ALL).implies(required));
    }
}

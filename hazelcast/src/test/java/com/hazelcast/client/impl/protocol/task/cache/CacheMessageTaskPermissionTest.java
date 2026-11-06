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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheCreateConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheIterateCodec;
import com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheManagementConfigCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.ConfigPermission;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the {@link javax.cache.Cache} related client message tasks require
 * a {@link CachePermission}, so an authenticated client without the proper cache
 * permission can neither read, listen to, create nor destroy caches.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheMessageTaskPermissionTest {

    private static final String CACHE_NAME = "/hz/myCache";
    private static final String[] CACHE_ACTIONS = {
            ActionConstants.ACTION_CREATE,
            ActionConstants.ACTION_DESTROY,
            ActionConstants.ACTION_PUT,
            ActionConstants.ACTION_REMOVE,
            ActionConstants.ACTION_READ,
            ActionConstants.ACTION_LISTEN,
    };

    private Node node;
    private Connection connection;
    private NodeEngineImpl nodeEngine;

    @Before
    public void setup() {
        nodeEngine = mock(NodeEngineImpl.class);

        ClientEndpoint endpoint = mock(ClientEndpoint.class);
        when(endpoint.getClientVersion()).thenReturn(BuildInfo.calculateVersion("3.12"));
        ClientEndpointManager endpointManager = mock(ClientEndpointManager.class);
        connection = mock(Connection.class);
        when(endpointManager.getEndpoint(connection)).thenReturn(endpoint);

        ClientEngine clientEngine = mock(ClientEngine.class);
        when(clientEngine.getEndpointManager()).thenReturn(endpointManager);

        node = mock(Node.class);
        Whitebox.setInternalState(node, "nodeEngine", nodeEngine);
        Whitebox.setInternalState(node, "clientEngine", clientEngine);
    }

    @Test
    public void addNearCacheInvalidationListener_requiresListenPermission() {
        CacheAddNearCacheInvalidationListenerCodec.RequestParameters parameters
                = new CacheAddNearCacheInvalidationListenerCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheAddNearCacheInvalidationListenerTask task = new CacheAddNearCacheInvalidationListenerTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void pre38AddInvalidationListener_requiresListenPermission() {
        CacheAddInvalidationListenerCodec.RequestParameters parameters
                = new CacheAddInvalidationListenerCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        Pre38CacheAddInvalidationListenerTask task = new Pre38CacheAddInvalidationListenerTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void addPartitionLostListener_requiresListenPermission() {
        CacheAddPartitionLostListenerCodec.RequestParameters parameters
                = new CacheAddPartitionLostListenerCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheAddPartitionLostListenerMessageTask task = new CacheAddPartitionLostListenerMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void listenerRegistration_requiresListenPermission() {
        CacheListenerRegistrationCodec.RequestParameters parameters = new CacheListenerRegistrationCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheListenerRegistrationMessageTask task = new CacheListenerRegistrationMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void removeInvalidationListener_requiresListenPermission() {
        CacheRemoveInvalidationListenerCodec.RequestParameters parameters
                = new CacheRemoveInvalidationListenerCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheRemoveInvalidationListenerMessageTask task = new CacheRemoveInvalidationListenerMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void removePartitionLostListener_requiresListenPermission() {
        CacheRemovePartitionLostListenerCodec.RequestParameters parameters
                = new CacheRemovePartitionLostListenerCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheRemovePartitionLostListenerMessageTask task
                = new CacheRemovePartitionLostListenerMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_LISTEN, withParameters(task, parameters));
    }

    @Test
    public void getConfig_requiresReadPermission() {
        CacheGetConfigCodec.RequestParameters parameters = new CacheGetConfigCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheGetConfigMessageTask task = new CacheGetConfigMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_READ, withParameters(task, parameters));
    }

    @Test
    public void iterate_requiresReadPermission() {
        CacheIterateCodec.RequestParameters parameters = new CacheIterateCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheIterateMessageTask task = new CacheIterateMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_READ, withParameters(task, parameters));
    }

    @Test
    public void loadAll_requiresReadPermission() {
        CacheLoadAllCodec.RequestParameters parameters = new CacheLoadAllCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheLoadAllMessageTask task = new CacheLoadAllMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_READ, withParameters(task, parameters));
    }

    @Test
    public void destroy_requiresDestroyPermission() {
        CacheDestroyCodec.RequestParameters parameters = new CacheDestroyCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheDestroyMessageTask task = withParameters(new CacheDestroyMessageTask(null, node, connection), parameters);

        assertEquals(CACHE_NAME, task.getDistributedObjectName());
        assertCachePermissionRequired(ActionConstants.ACTION_DESTROY, task);
    }

    @Test
    public void createConfig_requiresCreatePermissionOnTheCacheOfTheConfig() {
        CacheConfig cacheConfig = new CacheConfig("myCache");
        when(nodeEngine.toObject(any())).thenReturn(cacheConfig);
        CacheCreateConfigCodec.RequestParameters parameters = new CacheCreateConfigCodec.RequestParameters();
        CacheCreateConfigMessageTask task = new CacheCreateConfigMessageTask(null, node, connection);

        assertCachePermissionRequired(ActionConstants.ACTION_CREATE, withParameters(task, parameters));
    }

    @Test
    public void managementConfig_requiresConfigPermission() {
        CacheManagementConfigCodec.RequestParameters parameters = new CacheManagementConfigCodec.RequestParameters();
        parameters.name = CACHE_NAME;
        CacheManagementConfigMessageTask task = new CacheManagementConfigMessageTask(null, node, connection);

        Permission required = withParameters(task, parameters).getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new ConfigPermission(), required);
        assertTrue(new ConfigPermission().implies(required));
        assertFalse(new CachePermission(CACHE_NAME, ActionConstants.ACTION_ALL).implies(required));
    }

    private static <T extends AbstractMessageTask> T withParameters(T task, Object parameters) {
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private static void assertCachePermissionRequired(String action, AbstractMessageTask task) {
        Permission required = task.getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new CachePermission(CACHE_NAME, action), required);

        // only the expected (or all) action on the right cache grants access
        assertTrue(new CachePermission(CACHE_NAME, action).implies(required));
        assertTrue(new CachePermission(CACHE_NAME, ActionConstants.ACTION_ALL).implies(required));
        for (String otherAction : CACHE_ACTIONS) {
            if (!otherAction.equals(action)) {
                assertFalse(otherAction + " must not grant " + action,
                        new CachePermission(CACHE_NAME, otherAction).implies(required));
            }
        }
        assertFalse(new CachePermission("/hz/anotherCache", ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new MapPermission(CACHE_NAME, ActionConstants.ACTION_ALL).implies(required));
    }
}

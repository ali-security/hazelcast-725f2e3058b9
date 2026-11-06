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

package com.hazelcast.client.impl.protocol.task.executorservice.durable;

import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.DurableExecutorServicePermission;
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
 * Verifies that the client message tasks of the {@link com.hazelcast.durableexecutor.DurableExecutorService}
 * require a {@link DurableExecutorServicePermission}, so an authenticated client without the proper
 * durable executor permission can neither submit code to the cluster nor read, dispose results or
 * shut down the durable executor.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DurableExecutorMessageTaskPermissionTest {

    private static final String EXECUTOR_NAME = "durableExecutor";

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
    public void submitToPartition_requiresModifyPermission() {
        DurableExecutorSubmitToPartitionCodec.RequestParameters parameters
                = new DurableExecutorSubmitToPartitionCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        DurableExecutorSubmitToPartitionMessageTask task
                = new DurableExecutorSubmitToPartitionMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertModifyPermissionRequired(task.getRequiredPermission());
    }

    @Test
    public void shutdown_requiresModifyPermission() {
        DurableExecutorShutdownCodec.RequestParameters parameters = new DurableExecutorShutdownCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        DurableExecutorShutdownMessageTask task = new DurableExecutorShutdownMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertModifyPermissionRequired(task.getRequiredPermission());
    }

    @Test
    public void disposeResult_requiresModifyPermission() {
        DurableExecutorDisposeResultCodec.RequestParameters parameters
                = new DurableExecutorDisposeResultCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        DurableExecutorDisposeResultMessageTask task = new DurableExecutorDisposeResultMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertModifyPermissionRequired(task.getRequiredPermission());
    }

    @Test
    public void isShutdown_requiresReadPermission() {
        DurableExecutorIsShutdownCodec.RequestParameters parameters = new DurableExecutorIsShutdownCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        DurableExecutorIsShutdownMessageTask task = new DurableExecutorIsShutdownMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertReadPermissionRequired(task.getRequiredPermission());
    }

    @Test
    public void retrieveResult_requiresReadPermission() {
        DurableExecutorRetrieveResultCodec.RequestParameters parameters
                = new DurableExecutorRetrieveResultCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        DurableExecutorRetrieveResultMessageTask task = new DurableExecutorRetrieveResultMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertReadPermissionRequired(task.getRequiredPermission());
    }

    @Test
    public void retrieveAndDisposeResult_requiresReadAndModifyPermissions() {
        DurableExecutorRetrieveAndDisposeResultCodec.RequestParameters parameters
                = new DurableExecutorRetrieveAndDisposeResultCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        DurableExecutorRetrieveAndDisposeResultMessageTask task
                = new DurableExecutorRetrieveAndDisposeResultMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        Permission required = task.getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ,
                ActionConstants.ACTION_MODIFY), required);
        assertTrue(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ,
                ActionConstants.ACTION_MODIFY).implies(required));
        assertTrue(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_MODIFY).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_CREATE).implies(required));
    }

    private static void assertModifyPermissionRequired(Permission required) {
        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_MODIFY), required);

        // only the modify (or all) action on the right durable executor grants access
        assertTrue(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_MODIFY).implies(required));
        assertTrue(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_CREATE).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_CREATE,
                ActionConstants.ACTION_DESTROY).implies(required));
        assertFalse(new DurableExecutorServicePermission("anotherExecutor", ActionConstants.ACTION_ALL).implies(required));
    }

    private static void assertReadPermissionRequired(Permission required) {
        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ), required);

        // only the read (or all) action on the right durable executor grants access
        assertTrue(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ).implies(required));
        assertTrue(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_CREATE).implies(required));
        assertFalse(new DurableExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_MODIFY).implies(required));
        assertFalse(new DurableExecutorServicePermission("anotherExecutor", ActionConstants.ACTION_ALL).implies(required));
    }
}

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

package com.hazelcast.client.impl.protocol.task.executorservice;

import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ExecutorServicePermission;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the client message tasks of the {@link com.hazelcast.core.IExecutorService}
 * require an {@link ExecutorServicePermission}, so an authenticated client without the proper
 * executor permission can neither submit code to the cluster nor cancel or shut down executors.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExecutorServiceMessageTaskPermissionTest {

    private static final String EXECUTOR_NAME = "executor";
    private static final String TASK_UUID = "task-uuid";

    private Node node;
    private Connection connection;
    private DistributedExecutorService executorService;

    @Before
    public void setup() {
        executorService = mock(DistributedExecutorService.class);

        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        when(nodeEngine.getService(DistributedExecutorService.SERVICE_NAME)).thenReturn(executorService);

        ClientEngine clientEngine = mock(ClientEngine.class);
        when(clientEngine.getEndpointManager()).thenReturn(mock(ClientEndpointManager.class));

        node = mock(Node.class);
        Whitebox.setInternalState(node, "nodeEngine", nodeEngine);
        Whitebox.setInternalState(node, "clientEngine", clientEngine);

        connection = mock(Connection.class);
    }

    @Test
    public void submitToPartition_requiresModifyPermission() {
        ExecutorServiceSubmitToPartitionCodec.RequestParameters parameters
                = new ExecutorServiceSubmitToPartitionCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        parameters.uuid = TASK_UUID;
        ExecutorServiceSubmitToPartitionMessageTask task
                = new ExecutorServiceSubmitToPartitionMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertModifyPermissionRequired(EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void submitToAddress_requiresModifyPermission() {
        ExecutorServiceSubmitToAddressCodec.RequestParameters parameters
                = new ExecutorServiceSubmitToAddressCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        parameters.uuid = TASK_UUID;
        ExecutorServiceSubmitToAddressMessageTask task = new ExecutorServiceSubmitToAddressMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertModifyPermissionRequired(EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void cancelOnPartition_requiresModifyPermissionOnTheExecutorOfTheTask() {
        when(executorService.getName(TASK_UUID)).thenReturn(EXECUTOR_NAME);
        ExecutorServiceCancelOnPartitionMessageTask task = cancelOnPartitionTask(TASK_UUID);

        assertEquals(EXECUTOR_NAME, task.getDistributedObjectName());
        assertModifyPermissionRequired(EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void cancelOnPartition_whenTaskUnknown_requiresModifyPermissionOnEmptyExecutorName() {
        ExecutorServiceCancelOnPartitionMessageTask task = cancelOnPartitionTask("unknown-uuid");

        assertNull(task.getDistributedObjectName());
        assertModifyPermissionRequired(ExecutorServicePermission.EMPTY_EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void cancelOnAddress_requiresModifyPermissionOnTheExecutorOfTheTask() {
        when(executorService.getName(TASK_UUID)).thenReturn(EXECUTOR_NAME);
        ExecutorServiceCancelOnAddressMessageTask task = cancelOnAddressTask(TASK_UUID);

        assertEquals(EXECUTOR_NAME, task.getDistributedObjectName());
        assertModifyPermissionRequired(EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void cancelOnAddress_whenTaskUnknown_requiresModifyPermissionOnEmptyExecutorName() {
        ExecutorServiceCancelOnAddressMessageTask task = cancelOnAddressTask("unknown-uuid");

        assertNull(task.getDistributedObjectName());
        assertModifyPermissionRequired(ExecutorServicePermission.EMPTY_EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void shutdown_requiresModifyPermission() {
        ExecutorServiceShutdownCodec.RequestParameters parameters = new ExecutorServiceShutdownCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        ExecutorServiceShutdownMessageTask task = new ExecutorServiceShutdownMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        assertEquals(EXECUTOR_NAME, task.getDistributedObjectName());
        assertModifyPermissionRequired(EXECUTOR_NAME, task.getRequiredPermission());
    }

    @Test
    public void isShutdown_requiresReadPermission() {
        ExecutorServiceIsShutdownCodec.RequestParameters parameters = new ExecutorServiceIsShutdownCodec.RequestParameters();
        parameters.name = EXECUTOR_NAME;
        ExecutorServiceIsShutdownMessageTask task = new ExecutorServiceIsShutdownMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        Permission required = task.getRequiredPermission();

        assertEquals(EXECUTOR_NAME, task.getDistributedObjectName());
        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new ExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ), required);
        assertTrue(new ExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_READ).implies(required));
        assertFalse(new ExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_CREATE).implies(required));
        assertFalse(new ExecutorServicePermission(EXECUTOR_NAME, ActionConstants.ACTION_MODIFY).implies(required));
    }

    private ExecutorServiceCancelOnPartitionMessageTask cancelOnPartitionTask(String uuid) {
        ExecutorServiceCancelOnPartitionCodec.RequestParameters parameters
                = new ExecutorServiceCancelOnPartitionCodec.RequestParameters();
        parameters.uuid = uuid;
        ExecutorServiceCancelOnPartitionMessageTask task
                = new ExecutorServiceCancelOnPartitionMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private ExecutorServiceCancelOnAddressMessageTask cancelOnAddressTask(String uuid) {
        ExecutorServiceCancelOnAddressCodec.RequestParameters parameters
                = new ExecutorServiceCancelOnAddressCodec.RequestParameters();
        parameters.uuid = uuid;
        ExecutorServiceCancelOnAddressMessageTask task = new ExecutorServiceCancelOnAddressMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private static void assertModifyPermissionRequired(String executorName, Permission required) {
        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new ExecutorServicePermission(executorName, ActionConstants.ACTION_MODIFY), required);

        // only the modify (or all) action on the right executor grants access
        assertTrue(new ExecutorServicePermission(executorName, ActionConstants.ACTION_MODIFY).implies(required));
        assertTrue(new ExecutorServicePermission(executorName, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new ExecutorServicePermission(executorName, ActionConstants.ACTION_CREATE).implies(required));
        assertFalse(new ExecutorServicePermission(executorName, ActionConstants.ACTION_READ).implies(required));
        assertFalse(new ExecutorServicePermission(executorName, ActionConstants.ACTION_CREATE,
                ActionConstants.ACTION_DESTROY).implies(required));
        assertFalse(new ExecutorServicePermission("anotherExecutor", ActionConstants.ACTION_ALL).implies(required));
    }
}

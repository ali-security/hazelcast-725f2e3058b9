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

package com.hazelcast.client.impl.protocol.task.scheduledexecutor;

import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToAddressCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.security.SecureCallable;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.reflect.Whitebox;

import javax.security.auth.Subject;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies that the tasks submitted by clients to the {@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}
 * are executed with the client's {@link Subject} (wrapped into a {@link SecureCallable}) when security is enabled,
 * so that the submitted code cannot bypass the permissions of the client.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unchecked")
public class ScheduledExecutorSubmitMessageTaskSecurityTest {

    private static final String SCHEDULER_NAME = "scheduler";
    private static final String TASK_NAME = "task";

    private Node node;
    private Connection connection;
    private ClientEngine clientEngine;
    private ManagedContext managedContext;
    private Data taskData;
    private Callable<String> clientTask;

    @Before
    public void setup() {
        clientTask = new ClientTask();
        taskData = mock(Data.class);
        managedContext = mock(ManagedContext.class);

        InternalSerializationService serializationService = mock(InternalSerializationService.class);
        when(serializationService.toObject(taskData)).thenReturn(clientTask);
        when(serializationService.getManagedContext()).thenReturn(managedContext);

        clientEngine = mock(ClientEngine.class);
        when(clientEngine.getEndpointManager()).thenReturn(mock(ClientEndpointManager.class));

        node = mock(Node.class);
        when(node.getSerializationService()).thenReturn(serializationService);
        Whitebox.setInternalState(node, "nodeEngine", mock(NodeEngineImpl.class));
        Whitebox.setInternalState(node, "clientEngine", clientEngine);

        connection = mock(Connection.class);
    }

    @Test
    public void submitToPartition_whenSecurityEnabled_thenTaskIsWrappedIntoSecureCallable() {
        SecurityContext securityContext = mock(SecurityContext.class);
        SecureCallable secureCallable = mock(SecureCallable.class);
        when(securityContext.createSecureCallable((Subject) any(), any(Callable.class))).thenReturn(secureCallable);
        when(clientEngine.getSecurityContext()).thenReturn(securityContext);

        Operation operation = submitToPartitionTask().prepareOperation();

        assertTaskIsSecured(securityContext, secureCallable, operation);
    }

    @Test
    public void submitToAddress_whenSecurityEnabled_thenTaskIsWrappedIntoSecureCallable() {
        SecurityContext securityContext = mock(SecurityContext.class);
        SecureCallable secureCallable = mock(SecureCallable.class);
        when(securityContext.createSecureCallable((Subject) any(), any(Callable.class))).thenReturn(secureCallable);
        when(clientEngine.getSecurityContext()).thenReturn(securityContext);

        Operation operation = submitToAddressTask().prepareOperation();

        assertTaskIsSecured(securityContext, secureCallable, operation);
    }

    @Test
    public void submitToPartition_whenSecurityDisabled_thenTaskIsNotWrapped() {
        Operation operation = submitToPartitionTask().prepareOperation();

        assertSame(clientTask, getTaskDefinition(operation).getCommand());
        verifyZeroInteractions(managedContext);
    }

    @Test
    public void submitToAddress_whenSecurityDisabled_thenTaskIsNotWrapped() {
        Operation operation = submitToAddressTask().prepareOperation();

        assertSame(clientTask, getTaskDefinition(operation).getCommand());
        verifyZeroInteractions(managedContext);
    }

    private void assertTaskIsSecured(SecurityContext securityContext, SecureCallable secureCallable, Operation operation) {
        ArgumentCaptor<Callable> callableCaptor = ArgumentCaptor.forClass(Callable.class);
        verify(securityContext).createSecureCallable((Subject) any(), callableCaptor.capture());
        assertSame(clientTask, callableCaptor.getValue());

        verify(managedContext).initialize(secureCallable);
        assertSame(secureCallable, getTaskDefinition(operation).getCommand());
    }

    private ScheduledExecutorSubmitToPartitionMessageTask submitToPartitionTask() {
        ScheduledExecutorSubmitToPartitionCodec.RequestParameters parameters
                = new ScheduledExecutorSubmitToPartitionCodec.RequestParameters();
        parameters.schedulerName = SCHEDULER_NAME;
        parameters.type = TaskDefinition.Type.SINGLE_RUN.getId();
        parameters.taskName = TASK_NAME;
        parameters.task = taskData;
        ScheduledExecutorSubmitToPartitionMessageTask task
                = new ScheduledExecutorSubmitToPartitionMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private ScheduledExecutorSubmitToAddressMessageTask submitToAddressTask() {
        ScheduledExecutorSubmitToAddressCodec.RequestParameters parameters
                = new ScheduledExecutorSubmitToAddressCodec.RequestParameters();
        parameters.schedulerName = SCHEDULER_NAME;
        parameters.type = TaskDefinition.Type.SINGLE_RUN.getId();
        parameters.taskName = TASK_NAME;
        parameters.task = taskData;
        ScheduledExecutorSubmitToAddressMessageTask task
                = new ScheduledExecutorSubmitToAddressMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private static TaskDefinition getTaskDefinition(Operation operation) {
        return Whitebox.getInternalState(operation, "definition");
    }

    private static class ClientTask implements Callable<String> {
        @Override
        public String call() {
            return "result";
        }
    }
}

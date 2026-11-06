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

package com.hazelcast.cp.internal.datastructures;

import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.client.GetSemaphoreTypeMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.client.DestroyRaftObjectMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.security.permission.SemaphorePermission;
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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the CP subsystem client message tasks for destroying CP objects and for
 * reading the semaphore type are guarded by a permission of the targeted data structure.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPMessageTaskPermissionTest {

    private static final String OBJECT_NAME = "object";

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
    public void destroyRaftAtomicLong_requiresDestroyPermissionOnTheAtomicLong() {
        Permission required = destroyRaftObjectTask(RaftAtomicLongService.SERVICE_NAME).getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new AtomicLongPermission(OBJECT_NAME, ActionConstants.ACTION_DESTROY), required);
        assertTrue(new AtomicLongPermission(OBJECT_NAME, ActionConstants.ACTION_DESTROY).implies(required));
        assertTrue(new AtomicLongPermission(OBJECT_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new AtomicLongPermission(OBJECT_NAME, ActionConstants.ACTION_CREATE, ActionConstants.ACTION_READ,
                ActionConstants.ACTION_MODIFY).implies(required));
        assertFalse(new AtomicLongPermission("anotherObject", ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new LockPermission(OBJECT_NAME, ActionConstants.ACTION_ALL).implies(required));
    }

    @Test
    public void destroyRaftLock_requiresDestroyPermissionOnTheLock() {
        Permission required = destroyRaftObjectTask(RaftLockService.SERVICE_NAME).getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new LockPermission(OBJECT_NAME, ActionConstants.ACTION_DESTROY), required);
        assertTrue(new LockPermission(OBJECT_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new LockPermission(OBJECT_NAME, ActionConstants.ACTION_CREATE, ActionConstants.ACTION_READ,
                ActionConstants.ACTION_LOCK).implies(required));
        assertFalse(new AtomicLongPermission(OBJECT_NAME, ActionConstants.ACTION_ALL).implies(required));
    }

    @Test
    public void destroyRaftObject_ofUnknownService_isRejected() {
        DestroyRaftObjectMessageTask task = destroyRaftObjectTask("hz:raft:unknownService");
        try {
            task.getRequiredPermission();
            fail("Destroying an object of an unknown service must not pass the permission check");
        } catch (IllegalArgumentException expected) {
            // no permission can be granted for an unknown service
        }
    }

    @Test
    public void getSemaphoreType_requiresSemaphorePermission() {
        CPSemaphoreGetSemaphoreTypeCodec.RequestParameters parameters = new CPSemaphoreGetSemaphoreTypeCodec.RequestParameters();
        parameters.proxyName = OBJECT_NAME;
        GetSemaphoreTypeMessageTask task = new GetSemaphoreTypeMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);

        Permission required = task.getRequiredPermission();

        assertNotNull("The request must be guarded by a permission", required);
        assertEquals(new SemaphorePermission(OBJECT_NAME), required);
        assertTrue(new SemaphorePermission(OBJECT_NAME, ActionConstants.ACTION_READ).implies(required));
        assertTrue(new SemaphorePermission(OBJECT_NAME, ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new SemaphorePermission("anotherSemaphore", ActionConstants.ACTION_ALL).implies(required));
        assertFalse(new LockPermission(OBJECT_NAME, ActionConstants.ACTION_ALL).implies(required));
    }

    private DestroyRaftObjectMessageTask destroyRaftObjectTask(String serviceName) {
        CPGroupDestroyCPObjectCodec.RequestParameters parameters = new CPGroupDestroyCPObjectCodec.RequestParameters();
        parameters.groupId = new RaftGroupId("default", 0, 0);
        parameters.serviceName = serviceName;
        parameters.objectName = OBJECT_NAME;
        DestroyRaftObjectMessageTask task = new DestroyRaftObjectMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }
}

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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.client.impl.protocol.task.AddDistributedObjectListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddMembershipListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddPartitionListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AddPartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.client.impl.protocol.task.ClientStatisticsMessageTask;
import com.hazelcast.client.impl.protocol.task.CreateProxiesMessageTask;
import com.hazelcast.client.impl.protocol.task.GetDistributedObjectsMessageTask;
import com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask;
import com.hazelcast.client.impl.protocol.task.IsFailoverSupportedMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.NoSuchMessageTask;
import com.hazelcast.client.impl.protocol.task.PingMessageTask;
import com.hazelcast.client.impl.protocol.task.RemoveAllListenersMessageTask;
import com.hazelcast.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.RemovePartitionLostListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheAssignAndGetUuidsMessageTask;
import com.hazelcast.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask;
import com.hazelcast.client.impl.protocol.task.map.MapAddListenerMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapAssignAndGetUuidsMessageTask;
import com.hazelcast.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask;
import com.hazelcast.client.impl.protocol.task.map.MapMadePublishableMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceCancelMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForCustomMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForListMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForMapMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForMultiMapMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceForSetMessageTask;
import com.hazelcast.client.impl.protocol.task.mapreduce.MapReduceJobProcessInformationMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.client.CreateRaftGroupMessageTask;
import com.hazelcast.cp.internal.session.client.CloseSessionMessageTask;
import com.hazelcast.cp.internal.session.client.CreateSessionMessageTask;
import com.hazelcast.cp.internal.session.client.GenerateThreadIdMessageTask;
import com.hazelcast.cp.internal.session.client.HeartbeatSessionMessageTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import javassist.ClassPool;
import javassist.LoaderClassPath;
import javassist.NotFoundException;
import javassist.bytecode.BadBytecode;
import javassist.bytecode.ClassFile;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.CodeIterator;
import javassist.bytecode.MethodInfo;
import javassist.bytecode.Mnemonic;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies the {@code getRequiredPermission()} method doesn't simply return null in client {@link MessageTask} instances.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MessageTaskSecurityTest {

    private static final String[] RETURN_NULL_OPS = {"aconst_null", "areturn"};
    private static final Map<String, String> SKIP_CLASS_MAP = new ConcurrentHashMap<String, String>();
    private static final ClassPool CLASS_POOL = new ClassPool(true);

    static {
        CLASS_POOL.appendClassPath(new LoaderClassPath(MessageTaskSecurityTest.class.getClassLoader()));

        skip(AddDistributedObjectListenerMessageTask.class, "Adds distributed object listener by user's request");
        skip(AddMembershipListenerMessageTask.class, "Adds listener for listening to member list changes");
        skip(AddPartitionListenerMessageTask.class, "Adds listener for listening to partition table changes");
        skip(AddPartitionLostListenerMessageTask.class, "Adds an internal listener");
        skip(CacheAssignAndGetUuidsMessageTask.class, "Internal task used by RepairingTask");
        skip(CacheFetchNearCacheInvalidationMetadataTask.class, "Internal task used by RepairingTask");
        skip(ClientStatisticsMessageTask.class, "Client statistics collection task");
        skip(GetDistributedObjectsMessageTask.class, "Gets proxies");
        skip(GetPartitionsMessageTask.class, "Gets the partition table - no cluster action performed");
        skip(IsFailoverSupportedMessageTask.class, "Checks the cluster failover support - no cluster action performed");
        skip(MapAddListenerMessageTask.class, "Permissions checked by subsequent MapPublisherCreate* tasks");
        skip(MapAssignAndGetUuidsMessageTask.class, "Internal task used by RepairingTask");
        skip(MapFetchNearCacheInvalidationMetadataTask.class, "Internal task used by RepairingTask");
        skip(MapMadePublishableMessageTask.class, "Internal task used by RepairingTask");
        skip(NoSuchMessageTask.class, "Fallback MessageTask type - no cluster action performed");
        skip(PingMessageTask.class, "Heart beat type - no cluster action performed");
        skip(RemoveAllListenersMessageTask.class, "Removes the listeners registered by the calling client itself");
        skip(RemoveDistributedObjectListenerMessageTask.class, "Removes distributed object listener by user's request");
        skip(RemovePartitionLostListenerMessageTask.class, "Adds an internal listener");
        skip(CreateRaftGroupMessageTask.class, "Initial message while creating a Client proxy for any CP object");
        skip(CreateSessionMessageTask.class, "CP session management - no data structure access");
        skip(CloseSessionMessageTask.class, "CP session management - no data structure access");
        skip(HeartbeatSessionMessageTask.class, "CP session management - no data structure access");
        skip(GenerateThreadIdMessageTask.class, "CP session management - no data structure access");
        skip(MapReduceForMapMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(MapReduceForMultiMapMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(MapReduceForListMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(MapReduceForSetMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(MapReduceForCustomMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(MapReduceCancelMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(MapReduceJobProcessInformationMessageTask.class, "Legacy MapReduce framework has no client permission model");
        skip(AuthenticationMessageTask.class, "Authentication message processing");
        skip(AuthenticationCustomCredentialsMessageTask.class, "Authentication message processing");
        skip(CreateProxiesMessageTask.class, "Permissions handled in beforeProcess() method");
    }

    @Test
    public void testGetRequiredPermissions() throws Exception {
        Set<Class<? extends AbstractMessageTask>> subTypes = REFLECTIONS.getSubTypesOf(AbstractMessageTask.class);
        assertFalse("No message task found", subTypes.isEmpty());
        List<String> classesReturningNull = new ArrayList<String>();
        for (Class clazz : subTypes) {
            if (!Modifier.isAbstract(clazz.getModifiers()) && !isGetRequiredPermissionAllowedToReturnNull(clazz.getName())
                    && doesGetRequiredPermissionSimpleReturnNull(clazz.getName())) {
                classesReturningNull.add(clazz.getName());
            }
        }
        assertTrue("These message tasks return null in getRequiredPermission(): " + classesReturningNull,
                classesReturningNull.isEmpty());
    }

    @Test
    public void testCreateProxiesOverridesBeforeProcess() throws Exception {
        assertNotNull(CreateProxiesMessageTask.class.getDeclaredMethod("beforeProcess"));
    }

    private static boolean isGetRequiredPermissionAllowedToReturnNull(String clsname) {
        return SKIP_CLASS_MAP.containsKey(clsname);
    }

    private boolean doesGetRequiredPermissionSimpleReturnNull(String clsname) throws NotFoundException, Exception, BadBytecode {
        if (clsname == null) {
            fail("Class with getRequiredPermission() method implementation not found");
        }
        ClassFile cf = CLASS_POOL.get(clsname).getClassFile();
        MethodInfo minfo = cf.getMethod("getRequiredPermission");
        if (minfo == null) {
            return doesGetRequiredPermissionSimpleReturnNull(cf.getSuperclass());
        }
        CodeAttribute ca = minfo.getCodeAttribute();
        CodeIterator ci = ca.iterator();
        String[] ops = new String[RETURN_NULL_OPS.length];
        int i = 0;
        while (ci.hasNext()) {
            i++;
            if (i > RETURN_NULL_OPS.length) {
                return false;
            }
            int index = ci.next();
            int op = ci.byteAt(index);
            ops[i - 1] = Mnemonic.OPCODE[op];
        }
        return Arrays.equals(RETURN_NULL_OPS, ops);
    }

    private static void skip(Class<?> classToSkip, String reason) {
        assertTrue(reason != null && !reason.isEmpty());
        SKIP_CLASS_MAP.put(classToSkip.getName(), reason);
    }
}

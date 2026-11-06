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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxiesCodec;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyInfo;
import com.hazelcast.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import javax.security.auth.Subject;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link CreateProxiesMessageTask} only creates the proxies the client
 * is allowed to create, so an authenticated client cannot create arbitrary distributed
 * objects on the cluster members without the {@code create} permission.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CreateProxiesMessageTaskSecurityTest {

    private static final String ALLOWED_MAP = "allowedMap";
    private static final String SECRET_MAP = "secretMap";
    private static final String EXISTING_MAP = "existingMap";
    private static final String SECRET_QUEUE = "secretQueue";
    private static final String UNKNOWN_SERVICE_OBJECT = "unknownServiceObject";
    private static final String UNKNOWN_SERVICE = "hz:impl:unknownService";

    private final List<Permission> grantedPermissions = new ArrayList<Permission>();
    private final Subject subject = new Subject();

    private Node node;
    private Connection connection;
    private ClientEngine clientEngine;
    private SecurityContext securityContext;
    private ProxyService proxyService;
    private InternalOperationService operationService;
    private Address memberAddress;

    @Before
    public void setup() throws Exception {
        grantedPermissions.add(new MapPermission(ALLOWED_MAP, ActionConstants.ACTION_CREATE));
        // the client may read the secret structures but it must not create them
        grantedPermissions.add(new MapPermission(SECRET_MAP, ActionConstants.ACTION_READ));
        grantedPermissions.add(new QueuePermission(SECRET_QUEUE, ActionConstants.ACTION_READ));

        securityContext = mock(SecurityContext.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                Permission permission = invocation.getArgument(1);
                for (Permission granted : grantedPermissions) {
                    if (granted.implies(permission)) {
                        return null;
                    }
                }
                throw new AccessControlException("Permission " + permission + " denied!", permission);
            }
        }).when(securityContext).checkPermission(any(Subject.class), any(Permission.class));

        proxyService = mock(ProxyService.class);
        when(proxyService.getDistributedObjectNames(MapService.SERVICE_NAME))
                .thenReturn(Collections.singleton(EXISTING_MAP));

        ClientEndpoint endpoint = mock(ClientEndpoint.class);
        when(endpoint.getSubject()).thenReturn(subject);
        ClientEndpointManager endpointManager = mock(ClientEndpointManager.class);
        connection = mock(Connection.class);
        when(endpointManager.getEndpoint(connection)).thenReturn(endpoint);

        clientEngine = mock(ClientEngine.class);
        when(clientEngine.getEndpointManager()).thenReturn(endpointManager);
        when(clientEngine.getProxyService()).thenReturn(proxyService);
        when(clientEngine.getSecurityContext()).thenReturn(securityContext);

        memberAddress = new Address("127.0.0.1", 5701);
        Member member = mock(Member.class);
        when(member.getAddress()).thenReturn(memberAddress);
        Set<Member> members = new HashSet<Member>();
        members.add(member);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getMembers()).thenReturn(members);

        InvocationBuilder invocationBuilder = mock(InvocationBuilder.class);
        when(invocationBuilder.setResultDeserialized(anyBoolean())).thenReturn(invocationBuilder);
        when(invocationBuilder.setExecutionCallback(any(ExecutionCallback.class))).thenReturn(invocationBuilder);
        operationService = mock(InternalOperationService.class);
        when(operationService.createInvocationBuilder(nullable(String.class), any(Operation.class), any(Address.class)))
                .thenReturn(invocationBuilder);

        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(nodeEngine.getOperationService()).thenReturn(operationService);

        node = mock(Node.class);
        when(node.getLogger(any(Class.class))).thenReturn(mock(ILogger.class));
        Whitebox.setInternalState(node, "nodeEngine", nodeEngine);
        Whitebox.setInternalState(node, "clientEngine", clientEngine);
    }

    @Test
    public void beforeProcess_createsOnlyProxiesTheClientIsAllowedToCreate() {
        CreateProxiesMessageTask task = createProxiesTask();

        task.beforeProcess();

        assertEquals(Collections.singletonList(ALLOWED_MAP), proxyNames((PostJoinProxyOperation) task.get()));
    }

    @Test
    public void beforeProcess_doesNotCheckCreatePermissionOfAlreadyExistingProxies() {
        CreateProxiesMessageTask task = createProxiesTask();

        task.beforeProcess();

        verify(securityContext, never()).checkPermission(subject,
                new MapPermission(EXISTING_MAP, ActionConstants.ACTION_CREATE));
        verify(securityContext).checkPermission(subject, new MapPermission(SECRET_MAP, ActionConstants.ACTION_CREATE));
        verify(securityContext).checkPermission(subject, new QueuePermission(SECRET_QUEUE, ActionConstants.ACTION_CREATE));
    }

    @Test
    public void beforeProcess_withoutSecurity_createsAllRequestedProxies() {
        when(clientEngine.getSecurityContext()).thenReturn(null);
        CreateProxiesMessageTask task = createProxiesTask();

        task.beforeProcess();

        assertEquals(new HashSet<String>(Arrays.asList(ALLOWED_MAP, SECRET_MAP, EXISTING_MAP, SECRET_QUEUE,
                UNKNOWN_SERVICE_OBJECT)), new HashSet<String>(proxyNames((PostJoinProxyOperation) task.get())));
    }

    @Test
    public void processMessage_sendsOnlyPermittedProxiesToTheMembers() throws Throwable {
        CreateProxiesMessageTask task = createProxiesTask();

        task.processMessage();

        ArgumentCaptor<Operation> captor = ArgumentCaptor.forClass(Operation.class);
        verify(operationService).createInvocationBuilder(nullable(String.class), captor.capture(), eq(memberAddress));
        Operation sent = captor.getValue();
        assertTrue(sent instanceof PostJoinProxyOperation);
        assertEquals(Collections.singletonList(ALLOWED_MAP), proxyNames((PostJoinProxyOperation) sent));
    }

    private CreateProxiesMessageTask createProxiesTask() {
        List<Map.Entry<String, String>> proxies = new ArrayList<Map.Entry<String, String>>();
        proxies.add(proxy(ALLOWED_MAP, MapService.SERVICE_NAME));
        proxies.add(proxy(SECRET_MAP, MapService.SERVICE_NAME));
        proxies.add(proxy(EXISTING_MAP, MapService.SERVICE_NAME));
        proxies.add(proxy(SECRET_QUEUE, QueueService.SERVICE_NAME));
        proxies.add(proxy(UNKNOWN_SERVICE_OBJECT, UNKNOWN_SERVICE));
        ClientCreateProxiesCodec.RequestParameters parameters = new ClientCreateProxiesCodec.RequestParameters();
        parameters.proxies = proxies;

        CreateProxiesMessageTask task = new CreateProxiesMessageTask(null, node, connection);
        Whitebox.setInternalState(task, "parameters", parameters);
        return task;
    }

    private static Map.Entry<String, String> proxy(String objectName, String serviceName) {
        return new AbstractMap.SimpleEntry<String, String>(objectName, serviceName);
    }

    private static List<String> proxyNames(PostJoinProxyOperation operation) {
        Collection<ProxyInfo> proxyInfos = Whitebox.getInternalState(operation, "proxies");
        List<String> names = new ArrayList<String>();
        for (ProxyInfo proxyInfo : proxyInfos) {
            names.add(proxyInfo.getObjectName());
        }
        return names;
    }
}

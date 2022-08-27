/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.seata.core.rpc.RpcChannelPoolKey;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static io.seata.common.DefaultValues.DEFAULT_TX_GROUP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Netty client channel manager test.
 *
 * @author zhaojun
 */
@ExtendWith(MockitoExtension.class)
class NettyClientChannelManagerTest {
    
    private NettyClientChannelManager channelManager;
    
    @Mock
    private NettyPoolableFactory poolableFactory;
    
    @Mock
    private Function<String, RpcChannelPoolKey> poolKeyFunction;
    
    private NettyClientConfig nettyClientConfig = new NettyClientConfig();
    
    @Mock
    private RpcChannelPoolKey rpcChannelPoolKey;
    
    @Mock
    private Channel channel;
    
    @Mock
    private Channel newChannel;
    
    @Mock
    private GenericKeyedObjectPool keyedObjectPool;
    
    @BeforeEach
    void setUp() {
        channelManager = new NettyClientChannelManager(poolableFactory, poolKeyFunction, nettyClientConfig);
    }
    
    @AfterEach
    void tearDown() {
    }
    
    @Test
    void assertAcquireChannelFromPool() {
        setupPoolFactory(rpcChannelPoolKey, channel);
        Channel actual = channelManager.acquireChannel("localhost");
        verify(poolableFactory).makeObject(rpcChannelPoolKey);
        Assertions.assertEquals(actual, channel);
    }
    
    private void setupPoolFactory(final RpcChannelPoolKey rpcChannelPoolKey, final Channel channel) {
        when(poolKeyFunction.apply(anyString())).thenReturn(rpcChannelPoolKey);
        when(poolableFactory.makeObject(rpcChannelPoolKey)).thenReturn(channel);
        when(poolableFactory.validateObject(rpcChannelPoolKey, channel)).thenReturn(true);
    }
    
    @Test
    void assertAcquireChannelFromCache() {
        channelManager.getChannels().putIfAbsent("localhost", channel);
        when(channel.isActive()).thenReturn(true);
        Channel actual = channelManager.acquireChannel("localhost");
        verify(poolableFactory, times(0)).makeObject(rpcChannelPoolKey);
        Assertions.assertEquals(actual, channel);
    }
    
    @Test
    void assertAcquireChannelFromPoolContainsInactiveCache() {
        channelManager.getChannels().putIfAbsent("localhost", channel);
        when(channel.isActive()).thenReturn(false);
        setupPoolFactory(rpcChannelPoolKey, newChannel);
        Channel actual = channelManager.acquireChannel("localhost");
        verify(poolableFactory).makeObject(rpcChannelPoolKey);
        Assertions.assertEquals(actual, newChannel);
    }
    
    @Test
    void assertReconnect() {
        channelManager.getChannels().putIfAbsent("127.0.0.1:8091", channel);
        channelManager.reconnect(DEFAULT_TX_GROUP);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    void assertReleaseChannelWhichCacheIsEmpty() throws Exception {
        setNettyClientKeyPool();
        setUpReleaseChannel();
        channelManager.releaseChannel(channel, "127.0.0.1:8091");
        verify(keyedObjectPool).returnObject(rpcChannelPoolKey, channel);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    void assertReleaseCachedChannel() throws Exception {
        setNettyClientKeyPool();
        setUpReleaseChannel();
        channelManager.getChannels().putIfAbsent("127.0.0.1:8091", channel);
        channelManager.releaseChannel(channel, "127.0.0.1:8091");
        assertTrue(channelManager.getChannels().isEmpty());
        verify(keyedObjectPool).returnObject(rpcChannelPoolKey, channel);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    void assertReleaseChannelNotEqualToCache() throws Exception {
        setNettyClientKeyPool();
        setUpReleaseChannel();
        channelManager.getChannels().putIfAbsent("127.0.0.1:8091", newChannel);
        channelManager.releaseChannel(channel, "127.0.0.1:8091");
        assertEquals(1, channelManager.getChannels().size());
        verify(keyedObjectPool).returnObject(rpcChannelPoolKey, channel);
    }
    
    @SuppressWarnings("unchecked")
    private void setUpReleaseChannel() {
        ConcurrentMap<String, Object> channelLocks =
            (ConcurrentMap<String, Object>) getFieldValue("channelLocks", channelManager);
        channelLocks.putIfAbsent("127.0.0.1:8091", new Object());
        ConcurrentMap<String, RpcChannelPoolKey> poolKeyMap =
            (ConcurrentMap<String, RpcChannelPoolKey>) getFieldValue("poolKeyMap", channelManager);
        poolKeyMap.putIfAbsent("127.0.0.1:8091", rpcChannelPoolKey);
    }
    
    private Object getFieldValue(final String fieldName, final Object targetObject) {
        try {
            Field field = targetObject.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(targetObject);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void setNettyClientKeyPool() {
        try {
            Field field = channelManager.getClass().getDeclaredField("nettyClientKeyPool");
            field.setAccessible(true);
            field.set(channelManager, keyedObjectPool);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
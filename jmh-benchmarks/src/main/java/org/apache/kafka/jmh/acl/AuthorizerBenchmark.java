/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.acl;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AuthorizerBenchmark {

    @Param({"10000", "50000", "200000"})
    private int resourceCount;
    //no. of. rules per resource
    @Param({"10", "50"})
    private int aclCount;

    @Param({"0", "20", "50", "90", "99", "99.9", "99.99", "100"})
    private double denyPercentage;

    private final int hostPreCount = 10;
    private final String resourceNamePrefix = "foo-bar35_resource-";
    private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private final String authorizeByResourceTypeHostName = "127.0.0.2";
    private StandardAuthorizer authorizer;
    private List<Action> actions = new ArrayList<>();
    private RequestContext authorizeContext;
    private RequestContext authorizeByResourceTypeContext;
    private AclBindingFilter filter;
    private AclOperation op;
    private ResourceType resourceType;

    Random rand = new Random(System.currentTimeMillis());
    double eps = 1e-9;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        authorizer = new StandardAuthorizer();
        filter = AclBindingFilter.ANY;
        op = AclOperation.READ;
        resourceType = ResourceType.TOPIC;
        prepareAclCache();
        // By adding `-95` to the resource name prefix, we cause the `TreeMap.from/to` call to return
        // most map entries. In such cases, we rely on the filtering based on `String.startsWith`
        // to return the matching ACLs. Using a more efficient data structure (e.g. a prefix
        // tree) should improve performance significantly.
        actions = List.of(new Action(AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix + 95, PatternType.LITERAL),
            1, true, true));
        authorizeContext = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
            "someclient", 1), "1", InetAddress.getByName("127.0.0.1"), principal,
            ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        authorizeByResourceTypeContext = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
            "someclient", 1), "1", InetAddress.getByName(authorizeByResourceTypeHostName), principal,
            ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
    }

    private void prepareAclCache() {
        Map<ResourcePattern, Set<AccessControlEntry>> aclEntries = new HashMap<>();
        for (int resourceId = 0; resourceId < resourceCount; resourceId++) {
            ResourcePattern resource = new ResourcePattern(
                (resourceId % 10 == 0) ? ResourceType.GROUP : ResourceType.TOPIC,
                resourceNamePrefix + resourceId,
                (resourceId % 5 == 0) ? PatternType.PREFIXED : PatternType.LITERAL);

            Set<AccessControlEntry> entries = aclEntries.computeIfAbsent(resource, k -> new HashSet<>());

            for (int aclId = 0; aclId < aclCount; aclId++) {
                // The principal in the request context we are using
                // is principal.toString without any suffix
                String principalName = principal.toString() + (aclId == 0 ? "" : aclId);
                AccessControlEntry allowAce = new AccessControlEntry(
                    principalName, "*", AclOperation.READ, AclPermissionType.ALLOW);

                entries.add(new AccessControlEntry(allowAce.principal(), allowAce.host(), allowAce.operation(), allowAce.permissionType()));

                if (shouldDeny()) {
                    entries.add(new AccessControlEntry(principalName, "*", AclOperation.READ, AclPermissionType.DENY));
                }
            }
        }

        ResourcePattern resourcePrefix = new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix,
            PatternType.PREFIXED);
        Set<AccessControlEntry> entriesPrefix = aclEntries.computeIfAbsent(resourcePrefix, k -> new HashSet<>());
        for (int hostId = 0; hostId < hostPreCount; hostId++) {
            AccessControlEntry allowAce = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                AclOperation.READ, AclPermissionType.ALLOW);
            entriesPrefix.add(new AccessControlEntry(allowAce.principal(), allowAce.host(), allowAce.operation(), allowAce.permissionType()));

            if (shouldDeny()) {
                entriesPrefix.add(new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                        AclOperation.READ, AclPermissionType.DENY));
            }
        }

        ResourcePattern resourceWildcard = new ResourcePattern(ResourceType.TOPIC, ResourcePattern.WILDCARD_RESOURCE,
            PatternType.LITERAL);
        Set<AccessControlEntry> entriesWildcard = aclEntries.computeIfAbsent(resourceWildcard, k -> new HashSet<>());
        // get dynamic entries number for wildcard acl
        for (int hostId = 0; hostId < resourceCount / 10; hostId++) {
            String hostName = "127.0.0" + hostId;
            // AuthorizeByResourceType is optimizing the wildcard deny case.
            // If we didn't skip the host, we would end up having a biased short runtime.
            if (hostName.equals(authorizeByResourceTypeHostName)) {
                continue;
            }

            AccessControlEntry allowAce = new AccessControlEntry(principal.toString(), hostName,
                AclOperation.READ, AclPermissionType.ALLOW);
            entriesWildcard.add(new AccessControlEntry(allowAce.principal(), allowAce.host(), allowAce.operation(), allowAce.permissionType()));
            if (shouldDeny()) {
                entriesWildcard.add(new AccessControlEntry(principal.toString(), hostName,
                        AclOperation.READ, AclPermissionType.DENY));
            }
        }

        setupAcls(aclEntries);
    }

    private void setupAcls(Map<ResourcePattern, Set<AccessControlEntry>> aclEntries) {
        for (Map.Entry<ResourcePattern, Set<AccessControlEntry>> entryMap : aclEntries.entrySet()) {
            ResourcePattern resourcePattern = entryMap.getKey();

            for (AccessControlEntry accessControlEntry : entryMap.getValue()) {
                StandardAcl standardAcl = StandardAcl.fromAclBinding(new AclBinding(resourcePattern, accessControlEntry));
                authorizer.addAcl(Uuid.randomUuid(), standardAcl);
            }
            authorizer.completeInitialLoad();

        }
    }

    private Boolean shouldDeny() {
        return rand.nextDouble() * 100.0 - eps < denyPercentage;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        authorizer.close();
    }

    @Benchmark
    public Iterable<AclBinding> testAclsIterator() {
        return authorizer.acls(filter);
    }

    @Benchmark
    public List<AuthorizationResult> testAuthorizer() {
        return authorizer.authorize(authorizeContext, actions);
    }

    @Benchmark
    public AuthorizationResult testAuthorizeByResourceType() {
        return authorizer.authorizeByResourceType(authorizeByResourceTypeContext, op, resourceType);
    }
}

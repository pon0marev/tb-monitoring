/**
 * Copyright © 2016-2026 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.monitoring.service;

import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.thingsboard.monitoring.client.TbClient;
import org.thingsboard.monitoring.client.WsClient;
import org.thingsboard.monitoring.client.WsClientFactory;
import org.thingsboard.monitoring.config.MonitoringConfig;
import org.thingsboard.monitoring.config.MonitoringTarget;
import org.thingsboard.monitoring.data.Latencies;
import org.thingsboard.monitoring.data.MonitoredServiceKey;
import org.thingsboard.monitoring.data.ServiceFailureException;
import org.thingsboard.monitoring.data.notification.ShortNameProvider;
import org.thingsboard.monitoring.metrics.ProbeMetricsRecorder;
import org.thingsboard.monitoring.util.TbStopWatch;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.query.EntityData;
import org.thingsboard.server.common.data.query.EntityDataPageLink;
import org.thingsboard.server.common.data.query.EntityDataQuery;
import org.thingsboard.server.common.data.query.EntityDataSortOrder;
import org.thingsboard.server.common.data.query.EntityKey;
import org.thingsboard.server.common.data.query.EntityKeyType;
import org.thingsboard.server.common.data.query.EntityListFilter;
import org.thingsboard.server.common.data.query.TsValue;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.thingsboard.monitoring.service.BaseHealthChecker.TEST_CF_TELEMETRY_KEY;
import static org.thingsboard.monitoring.service.BaseHealthChecker.TEST_TELEMETRY_KEY;

@Slf4j
public abstract class BaseMonitoringService<C extends MonitoringConfig<T>, T extends MonitoringTarget> {

    @Autowired(required = false)
    private List<C> configs;
    private final List<BaseHealthChecker<C, T>> healthCheckers = new LinkedList<>();
    private final List<UUID> devices = new LinkedList<>();

    @Autowired
    private TbClient tbClient;
    @Autowired
    private WsClientFactory wsClientFactory;
    @Autowired
    private TbStopWatch stopWatch;
    @Autowired
    private MonitoringReporter reporter;
    @Autowired
    private ProbeMetricsRecorder probeMetricsRecorder;
    @Autowired
    protected ApplicationContext applicationContext;
    @Autowired
    private ScheduledExecutorService scheduler;

    @Value("${monitoring.edqs.enabled:false}")
    private boolean checkEdqs;
    @Value("${monitoring.calculated_fields.enabled:true}")
    protected boolean checkCalculatedFields;
    @Value("${monitoring.monitoring_rate_ms}")
    private int monitoringRateMs;

    public void init() {
        if (configs == null || configs.isEmpty()) {
            return;
        }

        configs.forEach(config -> {
            config.getTargets().forEach(target -> {
                BaseHealthChecker<C, T> healthChecker = initHealthChecker(target, config);
                healthCheckers.add(healthChecker);

                if (target.isCheckDomainIps()) {
                    getAssociatedUrls(target.getBaseUrl()).forEach(url -> {
                        healthChecker.getAssociates().put(url, initHealthChecker(createTarget(url), config));
                    });
                }
            });
        });
    }

    private BaseHealthChecker<C, T> initHealthChecker(T target, C config) {
        BaseHealthChecker<C, T> healthChecker = (BaseHealthChecker<C, T>) createHealthChecker(config, target);
        log.info("Initializing {} for {}", healthChecker.getClass().getSimpleName(), target.getBaseUrl());
        healthChecker.initialize();
        devices.add(target.getDeviceId());
        return healthChecker;
    }

    private List<BaseHealthChecker<C, T>> flattenHealthCheckers() {
        List<BaseHealthChecker<C, T>> flattened = new ArrayList<>();
        for (BaseHealthChecker<C, T> healthChecker : healthCheckers) {
            flattened.add(healthChecker);
            flattened.addAll(healthChecker.getAssociates().values());
        }
        return flattened;
    }

    public final void runChecks() {
        if (healthCheckers.isEmpty()) {
            scheduleNextCycle();
            return;
        }
        boolean chainStarted = false;
        try {
            log.info("Starting {}", getName());
            probeMetricsRecorder.startCycle();
            probeMetricsRecorder.recordHeartbeat();

            String accessToken;
            boolean loginSuccess = false;
            try {
                stopWatch.start();
                accessToken = tbClient.logIn();
                long loginLatencyNanos = stopWatch.getTime();
                reporter.reportLatency(Latencies.LOG_IN, loginLatencyNanos);
                probeMetricsRecorder.recordActionDuration(MonitoredServiceKey.LOGIN, ProbeMetricsRecorder.ACTION_REQUEST, loginLatencyNanos);
                reporter.serviceIsOk(MonitoredServiceKey.LOGIN);
                loginSuccess = true;
            } catch (Exception e) {
                reporter.serviceFailure(MonitoredServiceKey.LOGIN, e);
                probeMetricsRecorder.removeActionDuration(MonitoredServiceKey.LOGIN, ProbeMetricsRecorder.ACTION_REQUEST);
                probeMetricsRecorder.removeProbe(MonitoredServiceKey.WS, ProbeMetricsRecorder.Removal.STALE_THIS_CYCLE);
                fallBackToAcceptedChecks();
                return;
            } finally {
                probeMetricsRecorder.recordProbe(MonitoredServiceKey.LOGIN, loginSuccess);
            }

            WsClient ws;
            try {
                ws = wsClientFactory.createClient(accessToken);
                reporter.serviceIsOk(MonitoredServiceKey.WS_CONNECT);
            } catch (Exception e) {
                reporter.serviceFailure(MonitoredServiceKey.WS_CONNECT, e);
                probeMetricsRecorder.removeActionDuration(MonitoredServiceKey.WS, ProbeMetricsRecorder.ACTION_CONNECT);
                probeMetricsRecorder.removeActionDuration(MonitoredServiceKey.WS, ProbeMetricsRecorder.ACTION_SUBSCRIBE);
                probeMetricsRecorder.recordProbe(MonitoredServiceKey.WS, false);
                fallBackToAcceptedChecks();
                return;
            }

            try {
                stopWatch.start();
                ws.subscribeForTelemetry(devices, getTestTelemetryKeys()).waitForReply();
                long subscribeLatencyNanos = stopWatch.getTime();
                reporter.reportLatency(Latencies.WS_SUBSCRIBE, subscribeLatencyNanos);
                probeMetricsRecorder.recordActionDuration(MonitoredServiceKey.WS, ProbeMetricsRecorder.ACTION_SUBSCRIBE, subscribeLatencyNanos);
                reporter.serviceIsOk(MonitoredServiceKey.WS_SUBSCRIBE);
                probeMetricsRecorder.recordProbe(MonitoredServiceKey.WS, true);
            } catch (Exception e) {
                reporter.serviceFailure(MonitoredServiceKey.WS_SUBSCRIBE, e);
                probeMetricsRecorder.removeActionDuration(MonitoredServiceKey.WS, ProbeMetricsRecorder.ACTION_SUBSCRIBE);
                probeMetricsRecorder.recordProbe(MonitoredServiceKey.WS, false);
                ws.close();
                fallBackToAcceptedChecks();
                return;
            }

            List<BaseHealthChecker<C, T>> flattened = flattenHealthCheckers();
            long intervalMs = monitoringRateMs / flattened.size();
            scheduleProbe(flattened, 0, ws, intervalMs);
            chainStarted = true;
        } catch (Throwable error) {
            try {
                reporter.serviceFailure(MonitoredServiceKey.GENERAL, error);
            } catch (Throwable reportError) {
                log.error("Error occurred during service failure reporting", reportError);
            }
        } finally {
            if (!chainStarted) {
                scheduleNextCycle();
            }
        }
    }

    private void scheduleProbe(List<BaseHealthChecker<C, T>> flattened, int index, WsClient ws, long intervalMs) {
        if (index >= flattened.size()) {
            finishCycle(ws);
            return;
        }
        scheduler.schedule(() -> {
            try {
                checkOne(flattened.get(index), ws);
            } catch (Throwable t) {
                log.warn("[{}] Probe failed for {}", getName(), flattened.get(index).getCachedInfo(), t);
            } finally {
                scheduleProbe(flattened, index + 1, ws, intervalMs);
            }
        }, index == 0 ? 0 : intervalMs, TimeUnit.MILLISECONDS);
    }

    private void checkOne(BaseHealthChecker<C, T> healthChecker, WsClient ws) throws Exception {
        healthChecker.check(ws);
        clearAcceptedMetricsForOne(healthChecker);

        T target = healthChecker.getTarget();
        if (target.isCheckDomainIps()) {
            // its own key, not GENERAL - GENERAL's serviceIsOk() still fires at the end of a cycle
            // that reached this point, which would immediately flap a real, persistent DNS failure
            // back to "recovered" even though it never actually cleared
            Object reconciliationKey = new ReconciliationFailureKey(healthChecker.getCachedInfo());
            try {
                reconcileAssociates(healthChecker, target);
                reporter.serviceIsOk(reconciliationKey);
            } catch (Exception e) {
                // check() above already recorded this cycle's data - a reconciliation failure
                // must not look like a failure of the probe itself, so it's reported under its
                // own key rather than the target's info
                reporter.serviceFailure(reconciliationKey, e);
                log.warn("Failed to reconcile associate IPs for {}", target.getBaseUrl(), e);
            }
        }
    }

    private void clearAcceptedMetricsForOne(BaseHealthChecker<C, T> healthChecker) {
        probeMetricsRecorder.removeAcceptedProbe(healthChecker.getCachedInfo(), ProbeMetricsRecorder.Removal.STALE_THIS_CYCLE);
    }

    private void finishCycle(WsClient ws) {
        try {
            if (checkEdqs) {
                try {
                    stopWatch.start();
                    checkEdqs();
                    reporter.reportLatency(Latencies.EDQS_QUERY, stopWatch.getTime());
                    reporter.serviceIsOk(MonitoredServiceKey.EDQS);
                } catch (ServiceFailureException e) {
                    reporter.serviceFailure(e.getServiceKey(), e);
                    return;
                } catch (Exception e) {
                    reporter.serviceFailure(MonitoredServiceKey.EDQS, e);
                    return;
                }
            }
            reporter.reportLatencies();
            reporter.serviceIsOk(MonitoredServiceKey.GENERAL);
            log.debug("Finished {}", getName());
        } finally {
            try {
                ws.close();
            } catch (Exception e) {
                log.warn("Failed to close WS client for {}", getName(), e);
            }
            scheduleNextCycle();
        }
    }

    private void scheduleNextCycle() {
        scheduler.schedule(this::runChecks, monitoringRateMs, TimeUnit.MILLISECONDS);
    }

    private record ReconciliationFailureKey(Object delegate) implements ShortNameProvider {
        @Override
        public String getShortName() {
            String base = delegate instanceof ShortNameProvider provider ? provider.getShortName() : String.valueOf(delegate);
            return base + " (DNS)";
        }

        @Override
        public String toString() {
            return delegate + " (DNS)";
        }
    }

    private void reconcileAssociates(BaseHealthChecker<C, T> healthChecker, T target) throws Exception {
        Set<String> associatedUrls = getAssociatedUrls(target.getBaseUrl());
        Map<String, BaseHealthChecker<C, T>> associates = healthChecker.getAssociates();
        Set<String> prevAssociatedUrls = new HashSet<>(associates.keySet());

        boolean changed = false;
        for (String url : associatedUrls) {
            if (!prevAssociatedUrls.contains(url)) {
                BaseHealthChecker<C, T> associate = initHealthChecker(createTarget(url), healthChecker.getConfig());
                associates.put(url, associate);
                changed = true;
            }
        }
        for (String url : prevAssociatedUrls) {
            if (!associatedUrls.contains(url)) {
                BaseHealthChecker<C, T> retiredAssociate = associates.get(url);
                // remove the metric before stopHealthChecker(), which can throw and skip everything after it
                probeMetricsRecorder.removeProbe(retiredAssociate.getCachedInfo(), ProbeMetricsRecorder.Removal.PERMANENT);
                probeMetricsRecorder.removeAcceptedProbe(retiredAssociate.getCachedInfo(), ProbeMetricsRecorder.Removal.PERMANENT);
                stopHealthChecker(retiredAssociate);
                associates.remove(url);
                changed = true;
            }
        }
        if (changed) {
            log.info("Updated IPs for {}: {} (old list: {})", target.getBaseUrl(), associatedUrls, prevAssociatedUrls);
        }
    }

    private void checkEdqs() {
        if (devices.isEmpty()) {
            return;
        }
        // Scoped to exactly the devices this instance monitors, rather than an EntityTypeFilter over
        // every device in the tenant - on a tenant not dedicated to monitoring, a type-wide query
        // would mean a full (and possibly paginated) scan just to confirm a handful of devices exist.
        EntityListFilter entityListFilter = new EntityListFilter();
        entityListFilter.setEntityType(EntityType.DEVICE);
        entityListFilter.setEntityList(devices.stream().map(UUID::toString).toList());
        EntityDataPageLink pageLink = new EntityDataPageLink(devices.size(), 0, null, new EntityDataSortOrder(new EntityKey(EntityKeyType.ENTITY_FIELD, "name")));
        EntityDataQuery entityDataQuery = new EntityDataQuery(entityListFilter, pageLink,
                List.of(new EntityKey(EntityKeyType.ENTITY_FIELD, "name"), new EntityKey(EntityKeyType.ENTITY_FIELD, "type")),
                List.of(new EntityKey(EntityKeyType.TIME_SERIES, TEST_TELEMETRY_KEY)),
                Collections.emptyList());
        List<EntityData> data = tbClient.findEntityDataByQuery(entityDataQuery).getData();

        Set<UUID> foundDevices = data.stream()
                .map(entityData -> entityData.getEntityId().getId())
                .collect(Collectors.toSet());
        Set<UUID> missing = Sets.difference(new HashSet<>(devices), foundDevices);
        if (!missing.isEmpty()) {
            throw new ServiceFailureException(MonitoredServiceKey.EDQS, "Missing devices in the response: " + missing);
        }

        data.forEach(entityData -> {
            Map<String, TsValue> values = new HashMap<>(entityData.getLatest().get(EntityKeyType.ENTITY_FIELD));
            values.putAll(entityData.getLatest().get(EntityKeyType.TIME_SERIES));

            Stream.of("name", "type", TEST_TELEMETRY_KEY).forEach(key -> {
                TsValue value = values.get(key);
                if (value == null || StringUtils.isBlank(value.getValue())) {
                    throw new ServiceFailureException(MonitoredServiceKey.EDQS, "Missing " + key + " for device " + entityData.getEntityId());
                }
            });
        });
    }

    @Value("${monitoring.dns_resolution_timeout_ms}")
    private long dnsResolutionTimeoutMs;

    // InetAddress.getAllByName() isn't interruptible, so a hung resolver leaves its thread blocked
    // forever even after the timeout below gives up on it. A dedicated pool keeps that blast radius
    // local to DNS lookups instead of pinning threads in the shared ForkJoinPool.commonPool(), which
    // every other unrelated CompletableFuture in the JVM (including this app's own) also draws from.
    private static final ExecutorService DNS_RESOLUTION_EXECUTOR = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "dns-resolution");
        t.setDaemon(true);
        return t;
    });

    @SneakyThrows
    private Set<String> getAssociatedUrls(String baseUrl) {
        URI url = new URI(baseUrl);
        InetAddress[] addresses = resolveWithTimeout(url.getHost(), dnsResolutionTimeoutMs);
        return Arrays.stream(addresses)
                .map(InetAddress::getHostAddress)
                .map(ip -> {
                    try {
                        return new URI(url.getScheme(), null, ip, url.getPort(), "", null, null).toString();
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toSet());
    }

    static InetAddress[] resolveWithTimeout(String host, long timeoutMs) {
        return resolveWithTimeout(host, timeoutMs, () -> resolveHost(host));
    }

    // resolver is a separate parameter (rather than always resolveHost(host)) so the timeout branch
    // itself is directly testable with a short timeout against a deliberately slow supplier, instead
    // of waiting on a real DNS hang
    @SneakyThrows
    static InetAddress[] resolveWithTimeout(String host, long timeoutMs, Supplier<InetAddress[]> resolver) {
        try {
            return CompletableFuture.supplyAsync(resolver, DNS_RESOLUTION_EXECUTOR)
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ServiceFailureException(MonitoredServiceKey.GENERAL,
                    "DNS resolution for " + host + " timed out after " + timeoutMs + " ms");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    private static InetAddress[] resolveHost(String host) {
        try {
            return InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new CompletionException(e);
        }
    }

    private List<String> getTestTelemetryKeys() {
        return checkCalculatedFields ? List.of(TEST_TELEMETRY_KEY, TEST_CF_TELEMETRY_KEY) : List.of(TEST_TELEMETRY_KEY);
    }

    // shared by the login/WS-connect/WS-subscribe failure branches: none of the healthCheckers ran
    // this cycle, so their regular probe gauges go stale and the WS-independent accepted fallback
    // takes over instead (covers both transport and integration health checkers - this method runs
    // for either subclass, not just TransportsMonitoringService)
    private void fallBackToAcceptedChecks() {
        flattenHealthCheckers().forEach(healthChecker ->
                probeMetricsRecorder.removeProbe(healthChecker.getCachedInfo(), ProbeMetricsRecorder.Removal.STALE_THIS_CYCLE));
        checkAllAccepted();
    }

    // always runs regardless of OTLP/Prometheus export; deliberately serial (see freshThisCycle)
    private void checkAllAccepted() {
        healthCheckers.forEach(healthChecker -> healthChecker.checkAccepted());
    }

    private void stopHealthChecker(BaseHealthChecker<C, T> healthChecker) throws Exception {
        healthChecker.destroyClient();
        devices.remove(healthChecker.getTarget().getDeviceId());
        log.info("Stopped {} for {}", healthChecker.getClass().getSimpleName(), healthChecker.getTarget().getBaseUrl());
    }

    protected abstract BaseHealthChecker<?, ?> createHealthChecker(C config, T target);

    protected abstract T createTarget(String baseUrl);

    protected abstract String getName();

}

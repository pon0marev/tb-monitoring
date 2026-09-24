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
package org.thingsboard.monitoring.client;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.thingsboard.monitoring.util.RestTemplateUtils;
import org.thingsboard.rest.client.RestClient;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DashboardId;

import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class TbClient extends RestClient {

    public enum AuthMode {LOGIN, API_KEY}

    private final AuthMode authMode;
    private final String apiKey;
    private final String username;
    private final String password;

    public TbClient(@Value("${monitoring.rest.base_url}") String baseUrl,
                    @Value("${monitoring.rest.request_timeout_ms}") int requestTimeoutMs,
                    @Value("${monitoring.rest.auth_mode:LOGIN}") AuthMode authMode,
                    @Value("${monitoring.rest.api_key:}") String apiKey,
                    @Value("${monitoring.rest.username:}") String username,
                    @Value("${monitoring.rest.password:}") String password) {
        // an api_key takes priority over auth_mode: LOGIN whenever both happen to be configured -
        // no need to also flip auth_mode to API_KEY by hand
        super(RestTemplateUtils.build(requestTimeoutMs), baseUrl,
                StringUtils.isNotBlank(apiKey) ? AuthType.API_KEY : AuthType.JWT,
                StringUtils.isNotBlank(apiKey) ? apiKey : null);
        this.authMode = StringUtils.isNotBlank(apiKey) ? AuthMode.API_KEY : authMode;
        this.apiKey = apiKey;
        this.username = username;
        this.password = password;
        if (this.authMode == AuthMode.API_KEY && StringUtils.isBlank(apiKey)) {
            throw new IllegalStateException("monitoring.rest.api_key must be set when monitoring.rest.auth_mode is API_KEY");
        }
        if (this.authMode == AuthMode.LOGIN && (StringUtils.isBlank(username) || StringUtils.isBlank(password))) {
            throw new IllegalStateException("monitoring.rest.username and monitoring.rest.password must be set when monitoring.rest.auth_mode is LOGIN");
        }
        log.info("Starting TbClient with auth mode: {}", this.authMode);
    }

    @PostConstruct
    private void init() {
        getWsCredential();
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    // returns a JWT for WsClient#authenticate - the server only accepts a token there, not an api
    // key (see AuthCmd), so in API_KEY mode this impersonates ourselves via the already-established
    // api key session to mint one, instead of a real username/password login - which is exactly what
    // API_KEY mode exists to avoid (e.g. when the account has forced 2FA or password expiration)
    public String getWsCredential() {
        if (authMode == AuthMode.API_KEY) {
            Optional<User> user = getUser();
            if (user.isEmpty()) {
                throw new IllegalStateException("API key authentication failed - no user returned");
            }
            Optional<JsonNode> tokenInfo = getUserToken(user.get().getId());
            if (tokenInfo.isEmpty()) {
                throw new IllegalStateException("Failed to obtain a WS token for the API key user - check that token access is enabled for it");
            }
            return tokenInfo.get().get("token").asText();
        }
        login(username, password);
        return getToken();
    }

    // RestClient.baseURL is protected but has no accessor of its own.
    public String getBaseUrl() {
        return baseURL;
    }

    // A dedicated "type": "CE"/"PE" field, put there specifically to answer this question - not a
    // heuristic. Requires auth (any role, including CUSTOMER_USER) but that's already established
    // by the time this is called. The PE RestClient's getSystemInfo() DTO doesn't carry "type", so
    // the endpoint is hit directly here instead.
    public Optional<Edition> getEdition() {
        try {
            JsonNode info = restTemplate.getForObject(baseURL + "/api/system/info", JsonNode.class);
            String type = info != null ? info.path("type").asText("") : "";
            return type.isEmpty() ? Optional.empty() : Optional.of("PE".equalsIgnoreCase(type) ? Edition.PE : Edition.CE);
        } catch (Exception e) {
            log.debug("Failed to fetch /api/system/info", e);
            return Optional.empty();
        }
    }

    // CE-only REST calls, added directly here for CE targets - see PublicSharingService.
    public void assignAssetToPublicCustomer(AssetId assetId) {
        postForPublicCustomer("/api/customer/public/asset/{id}", assetId.getId());
    }

    public void assignDashboardToPublicCustomer(DashboardId dashboardId) {
        postForPublicCustomer("/api/customer/public/dashboard/{id}", dashboardId.getId());
    }

    private void postForPublicCustomer(String path, UUID id) {
        try {
            restTemplate.postForEntity(baseURL + path, null, Void.class, id);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
                throw e;
            }
            // CE always has a public customer - a 404 here means the entity itself is gone, not that
            // the public-customer feature is missing. Silently leaving it non-public would only
            // surface later as a missing public dashboard link, with nothing pointing back to why.
            log.warn("Failed to assign {} to the public customer - got 404 from {}", id, path);
        }
    }

}

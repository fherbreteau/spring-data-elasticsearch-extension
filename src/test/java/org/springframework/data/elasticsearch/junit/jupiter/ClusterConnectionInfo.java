package org.springframework.data.elasticsearch.junit.jupiter;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class ClusterConnectionInfo {
    private final boolean useSsl;
    private final String host;
    private final int httpPort;
    private final String clusterName;
    @Nullable
    private final ElasticsearchContainer elasticsearchContainer;

    public static Builder builder() {
        return new Builder();
    }

    private ClusterConnectionInfo(String host, int httpPort,
                                  boolean useSsl, @Nullable ElasticsearchContainer elasticsearchContainer) {
        this.host = host;
        this.httpPort = httpPort;
        this.useSsl = useSsl;
        this.elasticsearchContainer = elasticsearchContainer;
        this.clusterName = "docker-cluster";
    }

    @Override
    public String toString() {
        return "ClusterConnectionInfo{" + //
                "useSsl=" + useSsl + //
                ", host='" + host + '\'' + //
                ", httpPort=" + httpPort + //
                '}'; //
    }

    public String getHost() {
        return host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    @Nullable
    public ElasticsearchContainer getElasticsearchContainer() {
        return elasticsearchContainer;
    }

    public static class Builder {
        private boolean useSsl = false;
        private String host;
        private int httpPort;
        @Nullable private ElasticsearchContainer elasticsearchContainer;

        public Builder withHostAndPort(String host, int httpPort) {

            Assert.hasLength(host, "host must not be empty");

            this.host = host;
            this.httpPort = httpPort;
            return this;
        }

        public Builder useSsl(boolean useSsl) {
            this.useSsl = useSsl;
            return this;
        }

        public Builder withElasticsearchContainer(ElasticsearchContainer elasticsearchContainer) {
            this.elasticsearchContainer = elasticsearchContainer;
            return this;
        }

        public ClusterConnectionInfo build() {
            return new ClusterConnectionInfo( host, httpPort, useSsl, elasticsearchContainer);
        }
    }}

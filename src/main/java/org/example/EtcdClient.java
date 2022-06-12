package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.Util;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;

@Component
public class EtcdClient {
    public static final ByteSequence KEY = ByteSequence.from("test".getBytes(StandardCharsets.UTF_8));

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdClient.class);

    private final Client client;

    public EtcdClient() throws Exception {
        List<URI> endpoints = Util.toURIs(Collections.singletonList("https://127.0.0.1:2379"));

        File caFile = new File("src/main/resources/ca.cer");
        File certFile = new File("src/main/resources/etcd.cer");
        File keyFile = new File("src/main/resources/etcd-pkcs8.pem");
        ClientBuilder clientBuilder = Client.builder()
            .endpoints(endpoints)
            .sslContext(nettySslContextBuilder ->
                nettySslContextBuilder.trustManager(caFile).keyManager(certFile, keyFile)
            )
            // .authority("localhost")
            .retryMaxDuration(Duration.ofSeconds(10));

        this.client = clientBuilder.build();

        watch();

        updatePeriodically();
    }

    private void watch() {
        WatchOption watchOption = WatchOption.newBuilder().isPrefix(true).build();
        client.getWatchClient().watch(
            KEY,
            watchOption,
            this::handleResponse,
            this::handleError
        );
    }

    private void handleResponse(WatchResponse watchResponse) {
        for (WatchEvent event : watchResponse.getEvents()) {
            KeyValue keyValue = event.getKeyValue();
            LOGGER.info("watch response, key: {}, value: {}", keyValue.getKey(), keyValue.getValue());
        }
    }

    private void handleError(Throwable error) {
        LOGGER.info("error happened when watching!", error);
    }

    private void updatePeriodically() throws InterruptedException {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            client.getKVClient().put(KEY, ByteSequence.from(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
            TimeUnit.SECONDS.sleep(5);
        }
    }
}

package com.github.shoreviewanalytics.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.driver.core.*;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class CassandraConnector {

    private static SSLContext loadCaCert() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        InputStream fis = null;
        X509Certificate caCert;
        try {
            fis = CassandraConnector.class.getResourceAsStream("/client.pem");
            caCert = (X509Certificate) cf.generateCertificate(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        ks.setCertificateEntry("caCert", caCert);
        tmf.init(ks);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);
        return sslContext; // RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
    }

    private CqlSession session;

    public void connect(String node, Integer port, String dataCenter) throws Exception {

        String username = "cassandra";
        String password = "cassandra";
        CqlSessionBuilder builder = CqlSession.builder();
        builder.withAuthCredentials(username,password);
        builder.addContactPoint(new InetSocketAddress(node, port));
        builder.withLocalDatacenter(dataCenter);
        builder.withKeyspace("KAFKA_EXAMPLES");
        builder.withSslContext(loadCaCert());


        session = builder.build();


    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }

}
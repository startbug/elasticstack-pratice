package com.ggs.domain;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.function.Function;

/**
 * @Author lianghaohui
 * @Date 2022/7/7 19:00
 * @Description
 */
public class TestDemo {

    private static ElasticsearchClient client = null;

    private static ElasticsearchAsyncClient asyncClient = null;

    private static synchronized void makeConnection() throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("starbug", "123456"));

        Path caCertificatePath = Paths.get("http_ca.crt");
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }

        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();

        RestClientBuilder builder = RestClient.builder(new HttpHost("192.168.131.134", 9200, "https"))
                .setHttpClientConfigCallback((httpClientBuilder) ->
                        httpClientBuilder.setSSLContext(sslContext).setDefaultCredentialsProvider(credentialsProvider)
                );

        RestClient restClient = builder.build();

        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        client = new ElasticsearchClient(transport);

        asyncClient = new ElasticsearchAsyncClient(transport);
    }


    public static void main(String[] args) throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        makeConnection();

        Product product = new Product().setId("1").setName("lucy").setPrice(10);

        IndexRequest<Object> indexRequest = new IndexRequest.Builder<>()
                .index("products")
                .id("abc")
                .document(product)
                .build();

        client.index(indexRequest);

        Product product1 = new Product("efg", "Bag", 42);

        client.index(builder -> builder
                .index("products")
                .id(product1.getId())
                .document(product1));

        TermQuery query = QueryBuilders.term()
                .field("name")
                .value("bag")
                .build();

        SearchRequest request = new SearchRequest.Builder()
                .index("products")
                .query(query._toQuery())
                .build();

        SearchResponse<Product> search = client.search(request, Product.class);

        for (Hit<Product> hit : search.hits().hits()) {
            Product pd = hit.source();
            System.out.println(pd);
        }

        // 匹配查询
        String searchText = "Bag";
        SearchResponse<Product> response1 = client.search(s -> s
                        .index("products")
                        .query(q -> q
                                .match(t -> t
                                        .field("name")
                                        .query(searchText)
                                )
                        ),
                Product.class
        );

        TotalHits total1 = response1.hits().total();
        boolean isExactResult = total1.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            System.out.println("There are " + total1.value() + " results");
        } else {
            System.out.println("There are more than " + total1.value() + "results");
        }

        List<Hit<Product>> hits1 = response1.hits().hits();
        for (Hit<Product> hit : hits1) {
            Product pd2 = hit.source();
            System.out.println("Found product " + pd2.getId() + ", score " + hit.score());
        }

    }

}

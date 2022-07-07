package com.ggs.domain;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.util.function.Function;

/**
 * @Author lianghaohui
 * @Date 2022/7/7 19:00
 * @Description
 */
public class TestDemo {

    private static ElasticsearchClient client = null;

    private static ElasticsearchAsyncClient asyncClient = null;

    private static synchronized void makeConnection() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("starbug", "123456"));

        RestClientBuilder builder = RestClient.builder(new HttpHost("192.168.131.134", 9200))
                .setHttpClientConfigCallback((httpClientBuilder) ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                );

        RestClient restClient = builder.build();

        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        client = new ElasticsearchClient(transport);

        asyncClient = new ElasticsearchAsyncClient(transport);
    }

    public static void main(String[] args) throws IOException {
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
        String searchText = "bag";
        SearchResponse<Product> search1 = client.search(s -> s.index("products").query(q -> q.match(t -> t.field("name").query(searchText))), Product.class);
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


    }

}

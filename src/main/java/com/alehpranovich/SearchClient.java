package com.alehpranovich;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.SearchTemplateResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alehpranovich.IndexUtil.*;

public class SearchClient {

    public static final String PRICE_FIELD = "price";
    public static final String NAME_FIELD = "name";
    private final ElasticsearchClient client;
    private final Integer elasticPort;


    public SearchClient(Integer elasticPort, String certPath, String userName, String password) throws Exception {
        this.elasticPort = elasticPort;
        this.client = createClient(certPath, userName, password);
    }

    private ElasticsearchClient createClient(String certPath, String userName, String password) throws Exception {
        Path caCertificatePath = Paths.get(certPath);
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);

        final SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(trustStore, null).build();

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClient restClient = RestClient.builder(new HttpHost("localhost", elasticPort, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider))
                .build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    @SneakyThrows(IOException.class)
    public void indexDataSample(String index) {
        List<Movie.MovieRaw> moviesRaw = readCSVFile("movies.csv", Movie.MovieRaw.class);
        List<Movie> movies = moviesRaw.stream()
                .map(movieRaw -> Movie.builder()
                        .movieId(movieRaw.getMovieId())
                        .title(movieRaw.getTitle())
                        .genres(Arrays.asList(movieRaw.getGenres().split("|")))
                        .build())
                .collect(Collectors.toList());

        deleteIndexIfExists(client, index);
        indexMovies(client, movies, index);
    }

    @SneakyThrows(IOException.class)
    public List<Movie> findAllByIndex(String index) {
        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(QueryBuilders.matchAll().build()._toQuery()),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> simpleSearch(String index, String field, String searchText) {
        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(q -> q.match(t -> t
                                .field(field)
                                .query(searchText))),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> searchByNameAndMaxPrice(String index, String productName, Double maxPrice) {
        Query byName = MatchQuery.of(m -> m
                .field(NAME_FIELD)
                .query(productName)
        )._toQuery();

        Query byMaxPrice = RangeQuery.of(r -> r
                .field(PRICE_FIELD)
                .lte(JsonData.of(maxPrice))
        )._toQuery();

        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(query -> query
                                .bool(builder -> builder
                                        .must(byName)
                                        .must(byMaxPrice))),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> searchByTemplate(String index, String fieldName, Object fieldValue) {
        // Create a script
        String queryScriptId = "query-script";

        client.putScript(r -> r
                .id(queryScriptId)
                .script(s -> s
                        .lang("mustache")
                        .source("{\"query\":{\"match\":{\"{{field}}\":\"{{value}}\"}}}")
                ));

        SearchTemplateResponse<Movie> response = client.searchTemplate(r -> r
                        .index(index)
                        .id(queryScriptId)
                        .params("field", JsonData.of(fieldName))
                        .params("value", JsonData.of(fieldValue)),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> searchByPrefix(String index, String fieldName, String fieldPrefix) {
        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(q -> q.matchPhrasePrefix(t -> t
                                .field(fieldName)
                                .query(fieldPrefix))),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> searchByPhrase(String index, String fieldName, String fieldPhrase) {
        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(q -> q.matchPhrase(t -> t
                                .field(fieldName)
                                .query(fieldPhrase))),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> searchByMultipleFields(String index, List<String> fieldNames, String fieldPhrase) {
        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(q -> q.multiMatch(t -> t
                                .fields(fieldNames)
                                .type(TextQueryType.BestFields)
                                .query(fieldPhrase))),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }

    @SneakyThrows(IOException.class)
    public List<Movie> searchIntervals(String index, String fieldName, String fieldIntervals) {
        SearchResponse<Movie> response = client.search(s -> s
                        .index(index)
                        .query(q -> q.intervals(t -> t
                                .field(fieldName)
                                .match(intervalBuilder -> intervalBuilder.query(fieldIntervals)
                                        .maxGaps(10))
                        )),
                Movie.class
        );

        return response.hits().hits().stream()
                .map(Hit::source).collect(Collectors.toList());
    }
}

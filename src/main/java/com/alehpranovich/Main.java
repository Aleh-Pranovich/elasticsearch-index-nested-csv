package com.alehpranovich;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alehpranovich.ClientBuilder.createClient;
import static com.alehpranovich.IndexUtil.*;

/**
 * Write simple indexing application in your language of choice – Java/Python/C#/Javascript/etc...
 * which will index simple csv file
 */
public class Main {

    public static final String MOVIES_INDEX_NAME = "movie_full_nested_index";

    public static void main(String[] args) throws Exception {
        String certPath = System.getenv("ELASTIC_CERT_PATH");
        String userName = System.getenv("ELASTIC_USERNAME");
        String password = System.getenv("ELASTIC_PASSWORD");
        Integer elasticPort = Integer.valueOf(System.getenv("ELASTIC_PORT"));

        ElasticsearchClient client = createClient(certPath, userName, password, elasticPort);

        List<Movie.MovieRaw> moviesRaw = readCSVFile("movies.csv", Movie.MovieRaw.class);

        List<Movie> movies = moviesRaw.stream()
                .map(movieRaw -> Movie.builder()
                        .movieId(movieRaw.getMovieId())
                        .title(movieRaw.getTitle())
                        .genres(Arrays.asList(movieRaw.getGenres().split("\\|")))
                        .tags(Collections.emptyList())
                        .ratings(Collections.emptyList())
                        .build())
                .collect(Collectors.toList());

        deleteIndexIfExists(client, MOVIES_INDEX_NAME);
        createIndexIfNotExists(client, MOVIES_INDEX_NAME);
        System.out.println("Index was created");

        addMapping(client, MOVIES_INDEX_NAME);
        System.out.println("Mapping was added");

        indexMovies(client, movies, MOVIES_INDEX_NAME);
        System.out.println("Movies were indexed");

        indexRatings(client, "ratings_10k.csv", MOVIES_INDEX_NAME);
        System.out.println("Ratings were indexed");

        indexTags(client, "tags_10k.csv", MOVIES_INDEX_NAME);
        System.out.println("Tags were indexed");

        closeClient(client);
    }

    private static void closeClient(ElasticsearchClient client) throws IOException {
        client._transport().close();
    }
}
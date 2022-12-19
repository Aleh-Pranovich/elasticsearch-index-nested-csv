package com.alehpranovich;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexUtil {

    public static final String RESOURCES_PATH = "src/main/resources/";

    public static <T> List<T> readCSVFile(String filePath, Class<T> type) throws IOException {
        File file = new File(IndexUtil.class.getClassLoader().getResource(filePath).getFile());
        InputStream inputStream = new FileInputStream(file);
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        return new CsvToBeanBuilder<T>(streamReader)
                .withType(type)
                .build().parse();
    }

    public static void createIndexIfNotExists(ElasticsearchClient client, String index) throws IOException {
        BooleanResponse existsResponse = client.indices().exists(builder -> builder.index(index));
        if (!existsResponse.value()) {
            client.indices().create(CreateIndexRequest.of(
                    builder -> builder.index(index)));
        }
    }

    public static void deleteIndexIfExists(ElasticsearchClient client, String index) throws IOException {
        BooleanResponse existsResponse = client.indices().exists(builder -> builder.index(index));
        if (existsResponse.value()) {
            client.indices().delete(DeleteIndexRequest.of(
                    builder -> builder.index(index)));
        }
    }

    public static void indexMovies(ElasticsearchClient client, List<Movie> movies, String index) throws IOException {
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (Movie movie : movies) {
            br.operations(op -> op
                    .index(idx -> idx
                            .index(index)
                            .id(movie.getMovieId().toString())
                            .document(movie)
                    )
            );
        }

        BulkResponse result = client.bulk(br.build());

        if (result.errors()) {
            System.out.println("Bulk had errors");
            for (BulkResponseItem item : result.items()) {
                if (item.error() != null) {
                    System.out.println(item.error().reason());
                }
            }
        }
    }

    /**
     * <pre>
     * POST movie_lens_nested_index/_update/{movieId}
     * {
     *   "script": {
     *     "source": "ctx._source.ratings.add(params.user)",
     *     "params": {"ratings": {"movieId": 1, "userId": 3,  "rating": 4.5}}
     *   }
     * }
     * </pre>
     */
    public static void indexRatings(ElasticsearchClient client, String fileName, String index) throws IOException, CsvValidationException {
        long i = 0;
        CSVReader reader = new CSVReader(new FileReader(RESOURCES_PATH + fileName));
        String[] line;
        while ((line = reader.readNext()) != null) {
            if (i == 0) { // skip header
                i++;
                continue;
            }
            Movie.Rating rating = Movie.Rating.builder()
                    .userId(Long.valueOf(line[0]))
                    .movieId(Long.valueOf(line[1]))
                    .rating(Double.valueOf(line[2]))
                    .build();

            BulkResponse response = client.bulk(
                    BulkRequest.of(bulkRequest -> bulkRequest
                            .index(index)
                            .operations(bulkOperation -> bulkOperation
                                    .update(updateOperation -> updateOperation
                                            .id(String.valueOf(rating.getMovieId()))
                                            .action(action -> action
                                                    .script(script -> script
                                                            .inline(inlineScript -> inlineScript
                                                                    .lang("painless")
                                                                    .source("ctx._source.ratings.add(params.rating)")
                                                                    .params("rating", JsonData.of(rating))
                                                            )
                                                    )
                                            )
                                    )
                            )
                    )
            );

            if (i % 1000 == 0) {
                System.out.println("row = " + i + " " + rating);
            }
            i++;
            if (response.errors()) {
                System.out.println("response.errors: " + response);
            }
        }
    }

    public static void indexTags(ElasticsearchClient client, String fileName, String index) throws IOException, CsvValidationException {
        long i = 0;
        CSVReader reader = new CSVReader(new FileReader("src/main/resources/" + fileName));
        String[] line;
        while ((line = reader.readNext()) != null) {
            if (i == 0) { // skip header
                i++;
                continue;
            }
            Movie.Tag tag = Movie.Tag.builder()
                    .userId(Long.valueOf(line[0]))
                    .movieId(Long.valueOf(line[1]))
                    .tag(line[2])
                    .build();

            BulkResponse response = client.bulk(
                    BulkRequest.of(bulkRequest -> bulkRequest
                            .index(index)
                            .operations(bulkOperation -> bulkOperation
                                    .update(updateOperation -> updateOperation
                                            .id(String.valueOf(tag.getMovieId()))
                                            .action(action -> action
                                                    .script(script -> script
                                                            .inline(inlineScript -> inlineScript
                                                                    .lang("painless")
                                                                    .source("ctx._source.tags.add(params.tag)")
                                                                    .params("tag", JsonData.of(tag))
                                                            )
                                                    )
                                            )
                                    )
                            )
                    )
            );

            if (i % 1000 == 0) {
                System.out.println("row = " + i + " " + tag);
            }
            i++;
            if (response.errors()) {
                System.out.println("response.errors: " + response);
            }
        }
    }


    /**
     * Document Mapping
     *
     * <pre>
     * {
     *   "mappings": {
     *     "properties": {
     *       "movieId": {"type": "long"},
     *       "title": {"type": "text"},
     *       "genres": {"type": "keyword"},
     *       "rating": {
     *         "type": "nested",
     *         "properties": {
     *           "userId": {"type": "long"},
     *           "movieId": {"type": "long"},
     *           "rating": {"type": "double"}
     *         }
     *       },
     *       "tags": {
     *         "type": "nested",
     *         "properties": {
     *               "userId": {"type": "long"},
     *           "movieId": {"type": "long"},
     *           "tag": {"type": "keyword"}
     *         }
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    public static void addMapping(ElasticsearchClient client, String index) throws IOException {
        Map<String, Property> ratingProps = new HashMap<>();
        ratingProps.put("userId", Property.of(p -> p.long_(v -> v)));
        ratingProps.put("movieId", Property.of(p -> p.long_(v -> v)));
        ratingProps.put("rating", Property.of(p -> p.double_(v -> v)));

        Map<String, Property> tagsProps = new HashMap<>();
        tagsProps.put("userId", Property.of(p -> p.long_(v -> v)));
        tagsProps.put("movieId", Property.of(p -> p.long_(v -> v)));
        tagsProps.put("tag", Property.of(p -> p.keyword(v -> v)));

        Map<String, Property> props = new HashMap<>();
        props.put("movieId", Property.of(p -> p.long_(v -> v)));
        props.put("title", Property.of(p -> p.text(v -> v)));
        props.put("genres", Property.of(p -> p.keyword(v -> v)));
        props.put("ratings", Property.of(p -> p.nested(v -> v.properties(ratingProps))));
        props.put("tags", Property.of(p -> p.nested(v -> v.properties(tagsProps))));

        client.indices().putMapping(m -> m
                .index(index)
                .properties(props)
        );
    }
}

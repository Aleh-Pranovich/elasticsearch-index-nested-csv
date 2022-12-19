package com.alehpranovich;

import com.opencsv.bean.CsvBindByName;
import lombok.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString()
public class Movie {
    private Long movieId;
    private String title;
    private List<String> genres;
    private List<Rating> ratings = Collections.emptyList();
    private List<Tag> tags = Collections.emptyList();

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class MovieRaw {
        @CsvBindByName
        private Long movieId;
        @CsvBindByName
        private String title;
        @CsvBindByName
        private String genres;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Rating {
        private Long movieId;
        private Long userId;
        private Double rating;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Tag {
        private Long userId;
        private Long movieId;
        private String tag;
    }
}

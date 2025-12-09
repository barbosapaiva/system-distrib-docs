// src/main/java/io/sd/brain/api/SearchController.java
package io.sd.brain.rest;

import io.sd.brain.search.SearchService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/search")
public class SearchController {

    private final SearchService searchService;

    public SearchController(SearchService searchService) {
        this.searchService = searchService;
    }

    public record SearchResponseItem(String cid, String name, double score) { }

    @GetMapping
    public List<SearchResponseItem> search(
            @RequestParam("q") String query,
            @RequestParam(name = "top_k", defaultValue = "5") int topK
    ) {
        var items = searchService.search(query, topK);
        return items.stream()
                .map(i -> new SearchResponseItem(i.cid, i.name, i.score))
                .toList();
    }
}
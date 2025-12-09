package io.sd.brain.search;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SearchService {

    private final InMemoryIndex index;

    public SearchService(InMemoryIndex index) {
        this.index = index;
    }

    public List<SearchMessages.SearchItem> search(String prompt, int topK) {
        return index.search(prompt, topK);
    }
}
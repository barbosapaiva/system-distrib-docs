package io.sd.brain.search;

import io.sd.brain.emb.EmbeddingService;
import org.springframework.stereotype.Component;

@Component
public class InMemoryIndexImpl extends InMemoryIndex {

    public InMemoryIndexImpl(EmbeddingService embedService) {
        super(embedService);
    }

    @Override
    public void addOrUpdate(String cid, float[] vector) {
        super.addOrUpdate(cid, vector);
    }

}
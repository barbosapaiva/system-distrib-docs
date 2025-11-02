package io.sd.brain.emb;

import ai.djl.Application;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

@Component
public class EmbeddingService {

    private final ZooModel<String, float[]> model;
    private final Predictor<String, float[]> predictor;

    public EmbeddingService() throws ModelException {
        try {
            Criteria<String, float[]> criteria = Criteria.<String, float[]>builder()
                    .optApplication(Application.NLP.TEXT_EMBEDDING)
                    .setTypes(String.class, float[].class)
                    .optModelUrls("djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2")
                    .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                    .optProgress(new ProgressBar())
                    .build();

            this.model = ModelZoo.loadModel(criteria);
            this.predictor = model.newPredictor();
        } catch (Exception e) {
            throw new ModelException("Falha a carregar modelo DJL", e);
        }
    }

    public float[] embed(String text) throws TranslateException {
        return predictor.predict(text == null ? "" : text);
    }

    @PreDestroy
    public void close() {
        predictor.close();
        model.close();
    }
}
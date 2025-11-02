package io.sd.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexUpdate {
    @JsonProperty public String cid;
    @JsonProperty public String name;
    @JsonProperty public long version;
    @JsonProperty("vector_dim") public int vector_dim;
    @JsonProperty public float[] vector;

    public IndexUpdate() { }

    @Override
    public String toString() {
        return "IndexUpdate{cid='%s', name='%s', version=%d, dim=%d, vector=[...]}".formatted(
                cid, name, version, vector_dim
        );
    }

    public String version() {
        return String.valueOf(version);
    }

    public String cid() {
        return cid;
    }
}
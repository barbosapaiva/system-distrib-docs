package io.sd.brain.search;

import java.util.List;

public final class SearchMessages {

    public static final class SearchItem {
        public String cid;
        public String name;
        public double score;
    }

    // pedido do cliente (HTTP) -> líder cria job_id
    public static final class SearchReq {
        public String job_id;
        public String prompt;
        public int top_k;
        public String assign_peer;
        public long ts;
    }

    // líder -> broadcast para workers
    public static final class SearchAssign {
        public String kind = "search_req";
        public String job_id;
        public String prompt;
        public int top_k = 5;
        public String target_peer;
        public long ts;
    }

    // worker -> broadcast resultado
    public static final class SearchResult {
        public String kind = "search_result";
        public String job_id;
        public String worker_id;
        public List<SearchItem> items;
        public long ts;
    }
}
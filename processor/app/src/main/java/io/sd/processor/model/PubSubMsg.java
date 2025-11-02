package io.sd.processor.model;

import java.util.List;

public record PubSubMsg(
        String from,
        String data,
        String seqno,
        List<String> topicIDs
) {}
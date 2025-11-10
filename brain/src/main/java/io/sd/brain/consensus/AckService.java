package io.sd.brain.consensus;

import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Mantém coletores de ACK por versão.
 */
@Service
public class AckService {

    private final ConcurrentMap<Long, AckCollector> byVersion = new ConcurrentHashMap<>();

    /** Cria (ou devolve) o coletor para a versão indicada, com o quorum pretendido. */
    public AckCollector startWait(long version, int quorum) {
        return byVersion.compute(version, (v, existing) ->
                (existing == null || existing.quorum() != quorum) ? new AckCollector(v, quorum) : existing);
    }

    /**
     * Regista um ACK vindo do PubSub.
     * @return true se, após este ACK, já existe maioria consistente.
     */
    public boolean register(long version, String peerId, String hash) {
        AckCollector col = byVersion.get(version);
        if (col == null) {
            col = byVersion.computeIfAbsent(version, v -> new AckCollector(v, 1));
        }
        return col.onAck(peerId, hash);
    }

    /** Limpa o coletor depois do commit (opcional). */
    public void clear(long version) {
        byVersion.remove(version);
    }
}
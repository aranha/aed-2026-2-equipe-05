package br.pucminas.aed.risco.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ReservaDeLimiteCanceladaEvent {
    private final String solicitacaoId;
    private final String motivo;

    @JsonCreator
    public ReservaDeLimiteCanceladaEvent(@JsonProperty("solicitacaoId") String solicitacaoId,
                                         @JsonProperty("motivo") String motivo) {
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
        this.motivo = motivo;
    }

    public String getSolicitacaoId() { return solicitacaoId; }
    public String getMotivo() { return motivo; }
}

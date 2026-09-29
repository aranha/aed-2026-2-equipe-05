package br.pucminas.aed.risco.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class LimiteDeCreditoReservadoEvent {
    private final String solicitacaoId;

    @JsonCreator
    public LimiteDeCreditoReservadoEvent(@JsonProperty("solicitacaoId") String solicitacaoId) {
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
    }

    public String getSolicitacaoId() { return solicitacaoId; }
}

package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class PropostaDeCreditoExpiradaEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final OffsetDateTime dataExpiracao;

    @JsonCreator
    public PropostaDeCreditoExpiradaEvent(@JsonProperty("eventoId") String eventoId,
                                           @JsonProperty("solicitacaoId") String solicitacaoId,
                                           @JsonProperty("dataExpiracao") OffsetDateTime dataExpiracao) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
        this.dataExpiracao = Objects.requireNonNull(dataExpiracao, "dataExpiracao e obrigatoria");
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public OffsetDateTime getDataExpiracao() { return dataExpiracao; }
}

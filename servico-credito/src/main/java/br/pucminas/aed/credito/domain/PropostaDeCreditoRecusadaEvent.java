package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class PropostaDeCreditoRecusadaEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final String motivo;
    private final OffsetDateTime dataRecusa;

    @JsonCreator
    public PropostaDeCreditoRecusadaEvent(@JsonProperty("eventoId") String eventoId,
                                           @JsonProperty("solicitacaoId") String solicitacaoId,
                                           @JsonProperty("motivo") String motivo,
                                           @JsonProperty("dataRecusa") OffsetDateTime dataRecusa) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
        this.motivo = motivo;
        this.dataRecusa = Objects.requireNonNull(dataRecusa, "dataRecusa e obrigatoria");
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getMotivo() { return motivo; }
    public OffsetDateTime getDataRecusa() { return dataRecusa; }
}

package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ElegibilidadeAprovadaEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorAprovado;
    private final OffsetDateTime dataAprovacao;

    @JsonCreator
    public ElegibilidadeAprovadaEvent(@JsonProperty("eventoId") String eventoId,
                                      @JsonProperty("solicitacaoId") String solicitacaoId,
                                      @JsonProperty("clienteId") String clienteId,
                                      @JsonProperty("valorAprovado") BigDecimal valorAprovado,
                                      @JsonProperty("dataAprovacao") OffsetDateTime dataAprovacao) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
        this.clienteId = Objects.requireNonNull(clienteId, "clienteId e obrigatorio");
        this.valorAprovado = Objects.requireNonNull(valorAprovado, "valorAprovado e obrigatorio");
        this.dataAprovacao = Objects.requireNonNull(dataAprovacao, "dataAprovacao e obrigatoria");
        if (valorAprovado.signum() <= 0) {
            throw new IllegalArgumentException("valorAprovado deve ser positivo");
        }
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getClienteId() { return clienteId; }
    public BigDecimal getValorAprovado() { return valorAprovado; }
    public OffsetDateTime getDataAprovacao() { return dataAprovacao; }
}

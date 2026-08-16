package br.pucminas.aed.risco.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class CreditoSolicitadoEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final OffsetDateTime dataSolicitacao;

    @JsonCreator
    public CreditoSolicitadoEvent(@JsonProperty("eventoId") String eventoId,
                                  @JsonProperty("solicitacaoId") String solicitacaoId,
                                  @JsonProperty("clienteId") String clienteId,
                                  @JsonProperty("valorSolicitado") BigDecimal valorSolicitado,
                                  @JsonProperty("dataSolicitacao") OffsetDateTime dataSolicitacao) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
        this.clienteId = Objects.requireNonNull(clienteId, "clienteId e obrigatorio");
        this.valorSolicitado = Objects.requireNonNull(valorSolicitado, "valorSolicitado e obrigatorio");
        this.dataSolicitacao = Objects.requireNonNull(dataSolicitacao, "dataSolicitacao e obrigatoria");
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getClienteId() { return clienteId; }
    public BigDecimal getValorSolicitado() { return valorSolicitado; }
    public OffsetDateTime getDataSolicitacao() { return dataSolicitacao; }
}

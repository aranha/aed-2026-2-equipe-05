package com.aed20262equipe05.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;

public final class CreditoSolicitadoEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final OffsetDateTime dataSolicitacao;
    private final String canalOrigem;

    @JsonCreator
    public CreditoSolicitadoEvent(@JsonProperty("eventoId") String eventoId,
                                  @JsonProperty("solicitacaoId") String solicitacaoId,
                                  @JsonProperty("clienteId") String clienteId,
                                  @JsonProperty("valorSolicitado") BigDecimal valorSolicitado,
                                  @JsonProperty("dataSolicitacao") OffsetDateTime dataSolicitacao,
                                  @JsonProperty("canalOrigem") String canalOrigem) {
        this.eventoId = Objects.requireNonNull(eventoId);
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId);
        this.clienteId = Objects.requireNonNull(clienteId);
        this.valorSolicitado = Objects.requireNonNull(valorSolicitado);
        this.dataSolicitacao = Objects.requireNonNull(dataSolicitacao);
        this.canalOrigem = Objects.requireNonNull(canalOrigem);
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getClienteId() { return clienteId; }
    public BigDecimal getValorSolicitado() { return valorSolicitado; }
    public OffsetDateTime getDataSolicitacao() { return dataSolicitacao; }
    public String getCanalOrigem() { return canalOrigem; }
}

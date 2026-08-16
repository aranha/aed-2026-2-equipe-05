package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public final class SolicitacaoCreditoVO {
    private final String eventoId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final String canalOrigem;

    @JsonCreator
    public SolicitacaoCreditoVO(@JsonProperty("eventoId") String eventoId,  @JsonProperty("solicitacaoId") String solicitacaoId,  @JsonProperty("clienteId") String clienteId,  @JsonProperty("valorSolicitado") BigDecimal valorSolicitado,  @JsonProperty("canalOrigem") String canalOrigem) {
        this.eventoId = eventoId;
        this.solicitacaoId = solicitacaoId;
        this.clienteId = clienteId;
        this.valorSolicitado = valorSolicitado;
        this.canalOrigem = canalOrigem;
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getClienteId() { return clienteId; }
    public BigDecimal getValorSolicitado() { return valorSolicitado; }
    public String getCanalOrigem() { return canalOrigem; }
}

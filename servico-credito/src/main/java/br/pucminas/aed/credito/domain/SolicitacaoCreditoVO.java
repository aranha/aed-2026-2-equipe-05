package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public final class SolicitacaoCreditoVO {
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final String canalOrigem;

    @JsonCreator
    public SolicitacaoCreditoVO(@JsonProperty("clienteId") String clienteId,  @JsonProperty("valorSolicitado") BigDecimal valorSolicitado,  @JsonProperty("canalOrigem") String canalOrigem) {
        this.clienteId = clienteId;
        this.valorSolicitado = valorSolicitado;
        this.canalOrigem = canalOrigem;
    }

    public String getClienteId() { return clienteId; }
    public BigDecimal getValorSolicitado() { return valorSolicitado; }
    public String getCanalOrigem() { return canalOrigem; }
}

package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public final class SolicitacaoCreditoVO {
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final String canalOrigem;
    private final OffsetDateTime dataSolicitacao;

    @JsonCreator
    public SolicitacaoCreditoVO(@JsonProperty("clienteId") String clienteId,
                                @JsonProperty("valorSolicitado") BigDecimal valorSolicitado,
                                @JsonProperty("canalOrigem") String canalOrigem,
                                @JsonProperty("dataSolicitacao") OffsetDateTime dataSolicitacao) {
        this.clienteId = clienteId;
        this.valorSolicitado = valorSolicitado;
        this.canalOrigem = canalOrigem;
        this.dataSolicitacao = dataSolicitacao;
    }

    public String getClienteId() { return clienteId; }
    public BigDecimal getValorSolicitado() { return valorSolicitado; }
    public String getCanalOrigem() { return canalOrigem; }
    public OffsetDateTime getDataSolicitacao() { return dataSolicitacao; }
}

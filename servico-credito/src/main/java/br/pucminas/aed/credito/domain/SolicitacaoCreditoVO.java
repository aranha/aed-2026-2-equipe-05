package br.pucminas.aed.credito.domain;

import java.math.BigDecimal;

public final class SolicitacaoCreditoVO {
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final String canalOrigem;

    public SolicitacaoCreditoVO(String clienteId, BigDecimal valorSolicitado, String canalOrigem) {
        this.clienteId = clienteId;
        this.valorSolicitado = valorSolicitado;
        this.canalOrigem = canalOrigem;
    }

    public String getClienteId() { return clienteId; }
    public BigDecimal getValorSolicitado() { return valorSolicitado; }
    public String getCanalOrigem() { return canalOrigem; }
}

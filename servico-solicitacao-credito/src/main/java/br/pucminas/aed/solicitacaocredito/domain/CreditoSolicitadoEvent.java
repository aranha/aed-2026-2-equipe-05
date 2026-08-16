package br.pucminas.aed.solicitacaocredito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

public final class CreditoSolicitadoEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorSolicitado;

    @JsonCreator
    public CreditoSolicitadoEvent(@JsonProperty("eventoId") String eventoId, @JsonProperty("ocorridoEm") Instant ocorridoEm, @JsonProperty("solicitacaoId") String solicitacaoId, @JsonProperty("clienteId") String clienteId, @JsonProperty("valorSolicitado") BigDecimal valorSolicitado) {

        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = Objects.requireNonNull(ocorridoEm, "ocorridoEm e obrigatorio");
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId, "solicitacaoId e obrigatorio");
        this.clienteId = Objects.requireNonNull(solicitacaoId, "clienteId e obrigatorio");
        this.valorSolicitado = valorSolicitado;

    }

    public String getEventoId() {
        return eventoId;
    }

    public Instant getOcorridoEm() {
        return ocorridoEm;
    }

    public String getSolicitacaoId() {
        return solicitacaoId;
    }

    public String getClienteId() {
        return clienteId;
    }

    public BigDecimal getValorSolicitado() {
        return valorSolicitado;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        CreditoSolicitadoEvent that = (CreditoSolicitadoEvent) o;
        return Objects.equals(solicitacaoId, that.solicitacaoId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(solicitacaoId);
    }

    @Override
    public String toString() {
        return "CreditoSolicitadoEvent{solicitacaoId=" + solicitacaoId + "}";
    }

}
package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;

public final class LimiteDeCreditoReservadoEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorReservado;
    private final BigDecimal limiteDisponivel;
    private final OffsetDateTime dataReserva;

    @JsonCreator
    public LimiteDeCreditoReservadoEvent(@JsonProperty("eventoId") String eventoId,
                                         @JsonProperty("solicitacaoId") String solicitacaoId,
                                         @JsonProperty("clienteId") String clienteId,
                                         @JsonProperty("valorReservado") BigDecimal valorReservado,
                                         @JsonProperty("limiteDisponivel") BigDecimal limiteDisponivel,
                                         @JsonProperty("dataReserva") OffsetDateTime dataReserva) {
        this.eventoId = Objects.requireNonNull(eventoId);
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId);
        this.clienteId = Objects.requireNonNull(clienteId);
        this.valorReservado = Objects.requireNonNull(valorReservado);
        this.limiteDisponivel = Objects.requireNonNull(limiteDisponivel);
        this.dataReserva = Objects.requireNonNull(dataReserva);
    }

    public String getEventoId() { return eventoId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getClienteId() { return clienteId; }
    public BigDecimal getValorReservado() { return valorReservado; }
    public BigDecimal getLimiteDisponivel() { return limiteDisponivel; }
    public OffsetDateTime getDataReserva() { return dataReserva; }
}

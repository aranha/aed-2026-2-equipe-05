package br.pucminas.aed.credito.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;

public final class ReservaDeLimiteCanceladaEvent {
    private final String eventoId;
    private final String eventoOrigemId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorDevolvido;
    private final BigDecimal limiteDisponivel;
    private final String motivo;
    private final OffsetDateTime dataCancelamento;

    @JsonCreator
    public ReservaDeLimiteCanceladaEvent(@JsonProperty("eventoId") String eventoId,
                                          @JsonProperty("eventoOrigemId") String eventoOrigemId,
                                          @JsonProperty("solicitacaoId") String solicitacaoId,
                                          @JsonProperty("clienteId") String clienteId,
                                          @JsonProperty("valorDevolvido") BigDecimal valorDevolvido,
                                          @JsonProperty("limiteDisponivel") BigDecimal limiteDisponivel,
                                          @JsonProperty("motivo") String motivo,
                                          @JsonProperty("dataCancelamento") OffsetDateTime dataCancelamento) {
        this.eventoId = Objects.requireNonNull(eventoId);
        this.eventoOrigemId = Objects.requireNonNull(eventoOrigemId);
        this.solicitacaoId = Objects.requireNonNull(solicitacaoId);
        this.clienteId = Objects.requireNonNull(clienteId);
        this.valorDevolvido = Objects.requireNonNull(valorDevolvido);
        this.limiteDisponivel = Objects.requireNonNull(limiteDisponivel);
        this.motivo = Objects.requireNonNull(motivo);
        this.dataCancelamento = Objects.requireNonNull(dataCancelamento);
    }

    public String getEventoId() { return eventoId; }
    public String getEventoOrigemId() { return eventoOrigemId; }
    public String getSolicitacaoId() { return solicitacaoId; }
    public String getClienteId() { return clienteId; }
    public BigDecimal getValorDevolvido() { return valorDevolvido; }
    public BigDecimal getLimiteDisponivel() { return limiteDisponivel; }
    public String getMotivo() { return motivo; }
    public OffsetDateTime getDataCancelamento() { return dataCancelamento; }
}

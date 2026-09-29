package br.pucminas.aed.risco.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class CreditoSolicitadoEvent {
    private final String eventoId;
    private final String solicitacaoId;
    private final String clienteId;
    private final BigDecimal valorSolicitado;
    private final OffsetDateTime dataSolicitacao;
    // Campos do contrato que o servico-risco nao usa (ex.: canalOrigem). Sao ignorados no
    // processamento, mas guardados para que o evento reserializado na DLQ, e republicado no
    // reprocessamento, continue completo.
    private final Map<String, Object> camposNaoUsados = new LinkedHashMap<>();

    @JsonCreator
    public CreditoSolicitadoEvent(@JsonProperty("eventoId") String eventoId,
                                  @JsonProperty("solicitacaoId") String solicitacaoId,
                                  @JsonProperty("clienteId") String clienteId,
                                  @JsonProperty("valorSolicitado") BigDecimal valorSolicitado,
                                  // Mantem o offset recebido (-03:00, pelo contrato) em vez de
                                  // converter para UTC, para que a DLQ e a republicacao o preservem.
                                  @JsonProperty("dataSolicitacao")
                                  @JsonFormat(without = JsonFormat.Feature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
                                  OffsetDateTime dataSolicitacao) {
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

    @JsonAnyGetter
    private Map<String, Object> getCamposNaoUsados() { return camposNaoUsados; }

    @JsonAnySetter
    private void guardarCampoNaoUsado(String nome, Object valor) { camposNaoUsados.put(nome, valor); }
}

package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.credito.domain.SolicitacaoCreditoVO;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CreditoService {
    private static final String TIPO_EVENTO = "credito.solicitacao.solicitada.v1";
    private static final String FONTE_EVENTO = "/credito/solicitacoes";
    private static final int TAMANHO_MAXIMO_DO_IDENTIFICADOR = 36;
    private final KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker;
    private final ResultadoPublicacaoService resultadoPublicacaoService;
    private final String topico;

    public CreditoService(KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker,
                          ResultadoPublicacaoService resultadoPublicacaoService,
                          @Value("${app.kafka.topico.credito-solicitado}") String topico) {
        this.clienteDoBroker = clienteDoBroker;
        this.resultadoPublicacaoService = resultadoPublicacaoService;
        this.topico = topico;
    }

    public CreditoSolicitadoEvent solicitar(SolicitacaoCreditoVO solicitacao) {
        validar(solicitacao);
        var agora = OffsetDateTime.now();
        var evento = new CreditoSolicitadoEvent(
                identificarOuGerar(solicitacao.getEventoId()), identificarOuGerar(solicitacao.getSolicitacaoId()),
                solicitacao.getClienteId(), solicitacao.getValorSolicitado(), agora, solicitacao.getCanalOrigem());
        var registro = new ProducerRecord<String, CreditoSolicitadoEvent>(
                topico, evento.getSolicitacaoId(), evento);
        adicionarCabecalho(registro, "ce_specversion", "1.0");
        adicionarCabecalho(registro, "ce_id", evento.getEventoId());
        adicionarCabecalho(registro, "ce_source", FONTE_EVENTO);
        adicionarCabecalho(registro, "ce_type", TIPO_EVENTO);
        adicionarCabecalho(registro, "ce_time", agora.toString());
        clienteDoBroker.send(registro).whenComplete(
                (resultado, falha) -> resultadoPublicacaoService.registrar(evento.getEventoId(), resultado, falha));
        return evento;
    }

    private String identificarOuGerar(String identificador) {
        if (identificador == null || identificador.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return identificador;
    }

    private boolean excedeOTamanhoMaximo(String identificador) {
        return identificador != null && identificador.length() > TAMANHO_MAXIMO_DO_IDENTIFICADOR;
    }

    private void validar(SolicitacaoCreditoVO solicitacao) {
        if (solicitacao.getClienteId() == null || solicitacao.getClienteId().isBlank()) {
            throw new IllegalArgumentException("clienteId e obrigatorio");
        }
        if (solicitacao.getValorSolicitado() == null || solicitacao.getValorSolicitado().signum() <= 0) {
            throw new IllegalArgumentException("valorSolicitado deve ser positivo");
        }
        if (solicitacao.getCanalOrigem() == null || solicitacao.getCanalOrigem().isBlank()) {
            throw new IllegalArgumentException("canalOrigem e obrigatorio");
        }
        if (excedeOTamanhoMaximo(solicitacao.getEventoId())) {
            throw new IllegalArgumentException(
                    "eventoId deve ter no maximo " + TAMANHO_MAXIMO_DO_IDENTIFICADOR + " caracteres");
        }
        if (excedeOTamanhoMaximo(solicitacao.getSolicitacaoId())) {
            throw new IllegalArgumentException(
                    "solicitacaoId deve ter no maximo " + TAMANHO_MAXIMO_DO_IDENTIFICADOR + " caracteres");
        }
    }

    private void adicionarCabecalho(ProducerRecord<String, CreditoSolicitadoEvent> registro,
                                    String nome, String valor) {
        registro.headers().add(new RecordHeader(nome, valor.getBytes(StandardCharsets.UTF_8)));
    }
}

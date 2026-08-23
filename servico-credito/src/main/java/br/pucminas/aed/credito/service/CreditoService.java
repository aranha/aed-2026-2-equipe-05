package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.credito.domain.SolicitacaoCreditoVO;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CreditoService {

    private static final ZoneOffset OFFSET_BRASILIA = ZoneOffset.of("-03:00");

    private static final String TIPO_EVENTO = "credito.solicitacao.solicitada.v1";
    private static final String FONTE_EVENTO = "/credito/solicitacoes";
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
        var dataSolicitacao = solicitacao.getDataSolicitacao() != null
                ? solicitacao.getDataSolicitacao().withOffsetSameInstant(OFFSET_BRASILIA)
                : OffsetDateTime.now(OFFSET_BRASILIA);
        var evento = new CreditoSolicitadoEvent(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), solicitacao.getClienteId(),
                solicitacao.getValorSolicitado(), dataSolicitacao, solicitacao.getCanalOrigem());
        var registro = new ProducerRecord<String, CreditoSolicitadoEvent>(
                topico, evento.getSolicitacaoId(), evento);
        adicionarCabecalho(registro, "ce_specversion", "1.0");
        adicionarCabecalho(registro, "ce_id", evento.getEventoId());
        adicionarCabecalho(registro, "ce_source", FONTE_EVENTO);
        adicionarCabecalho(registro, "ce_type", TIPO_EVENTO);
        adicionarCabecalho(registro, "ce_time", dataSolicitacao.toString());
        clienteDoBroker.send(registro).whenComplete(
                (resultado, falha) -> resultadoPublicacaoService.registrar(evento.getEventoId(), resultado, falha));
        return evento;
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
    }

    private void adicionarCabecalho(ProducerRecord<String, CreditoSolicitadoEvent> registro,
                                    String nome, String valor) {
        registro.headers().add(new RecordHeader(nome, valor.getBytes(StandardCharsets.UTF_8)));
    }
}

package br.pucminas.aed.solicitacaocredito.service;

import br.pucminas.aed.solicitacaocredito.domain.CreditoSolicitadoEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

import java.util.function.BiConsumer;

public class SolicitacaoCreditoCallbackService implements BiConsumer<SendResult<String, CreditoSolicitadoEvent>, Throwable> {

    private static final Logger log = LoggerFactory.getLogger(SolicitacaoCreditoCallbackService.class);

    private final CreditoSolicitadoEvent evento;

    public SolicitacaoCreditoCallbackService(CreditoSolicitadoEvent evento) {
        this.evento = evento;
    }

    @Override
    public void accept(SendResult<String, CreditoSolicitadoEvent> resultado, Throwable erro) {
        if (erro != null) {
            log.error("FALHA ao publicar evento {}", evento.getEventoId(), erro);
            return;
        }
        RecordMetadata metadados = resultado.getRecordMetadata();
        log.info("publicado  evento={}  solicitacao={}  particao={}  offset={}", evento.getEventoId(), evento.getSolicitacaoId(), Integer.valueOf(metadados.partition()), Long.valueOf(metadados.offset()));
    }

}

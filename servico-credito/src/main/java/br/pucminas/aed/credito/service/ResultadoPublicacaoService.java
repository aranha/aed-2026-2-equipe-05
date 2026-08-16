package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.CreditoSolicitadoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ResultadoPublicacaoService {
    public void registrar(String eventoId, SendResult<String, CreditoSolicitadoEvent> resultado,
                          Throwable falha) {
        if (falha != null) {
            log.error("Falha ao publicar o evento {}", eventoId, falha);
            return;
        }
        log.info("Evento {} publicado na particao {} e offset {}", eventoId,
                resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
    }
}

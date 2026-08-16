package com.aed20262equipe05.credito.service;

import com.aed20262equipe05.credito.domain.CreditoSolicitadoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class ResultadoPublicacaoService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultadoPublicacaoService.class);

    public void registrar(String eventoId, SendResult<String, CreditoSolicitadoEvent> resultado,
                          Throwable falha) {
        if (falha != null) {
            LOGGER.error("Falha ao publicar o evento {}", eventoId, falha);
            return;
        }
        LOGGER.info("Evento {} publicado na particao {} e offset {}", eventoId,
                resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
    }
}

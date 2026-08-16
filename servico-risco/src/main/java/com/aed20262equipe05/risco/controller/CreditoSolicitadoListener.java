package com.aed20262equipe05.risco.controller;

import com.aed20262equipe05.risco.domain.CreditoSolicitadoEvent;
import com.aed20262equipe05.risco.service.AnaliseCreditoService;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class CreditoSolicitadoListener {
    private final AnaliseCreditoService analiseCreditoService;

    public CreditoSolicitadoListener(AnaliseCreditoService analiseCreditoService) {
        this.analiseCreditoService = analiseCreditoService;
    }

    @KafkaListener(topics = "${app.kafka.topico.credito-solicitado}")
    public void receber(ConsumerRecord<String, CreditoSolicitadoEvent> registro,
                        Acknowledgment confirmacao) {
        String eventoId = lerCabecalhoObrigatorio(registro, "ce_id");
        analiseCreditoService.processar(eventoId, registro.value());
        confirmacao.acknowledge();
    }

    private String lerCabecalhoObrigatorio(ConsumerRecord<String, CreditoSolicitadoEvent> registro,
                                           String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            throw new IllegalArgumentException("cabecalho " + nome + " e obrigatorio");
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}

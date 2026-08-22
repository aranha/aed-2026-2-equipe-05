package br.pucminas.aed.risco.controller;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.risco.service.FluxoCreditoSolicitadoService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FluxoCreditoSolicitadoListener {

    private final FluxoCreditoSolicitadoService fluxoCreditoSolicitadoService;

    @KafkaListener(topics = "${app.kafka.topico.credito-solicitado}", groupId = "risco-fluxo-creditos-v1")
    public void receber(ConsumerRecord<String, CreditoSolicitadoEvent> registro, Acknowledgment confirmacao) {
        fluxoCreditoSolicitadoService.agregar(registro.value());
        confirmacao.acknowledge();
    }

}

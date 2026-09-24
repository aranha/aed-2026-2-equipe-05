package br.pucminas.aed.risco.controller;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.risco.service.FluxoCreditoSolicitadoService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
public class FluxoCreditoSolicitadoListener {

    private final FluxoCreditoSolicitadoService fluxoCreditoSolicitadoService;

    @KafkaListener(topics = "${app.kafka.topico.credito-solicitado}", groupId = "${app.kafka.grupo.fluxo-credito}")
    public void receber(ConsumerRecord<String, CreditoSolicitadoEvent> registro, Acknowledgment confirmacao) {
        String eventoId = lerCabecalhoObrigatorio(registro, "ce_id");
        fluxoCreditoSolicitadoService.agregar(eventoId, registro.value());
        confirmacao.acknowledge();
    }

    private String lerCabecalhoObrigatorio(ConsumerRecord<String, CreditoSolicitadoEvent> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            throw new IllegalArgumentException("cabecalho " + nome + " e obrigatorio");
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }

}

package br.pucminas.aed.risco.controller;

import br.pucminas.aed.risco.domain.LimiteDeCreditoReservadoEvent;
import br.pucminas.aed.risco.domain.ReservaDeLimiteCanceladaEvent;
import br.pucminas.aed.risco.service.AcompanhamentoReservaService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SagaReservaListener {
    private final AcompanhamentoReservaService acompanhamentoReservaService;
    private final ObjectMapper objectMapperDosEventos;

    @Value("${app.kafka.topico.limite-reservado}")
    private String topicoLimiteReservado;

    @Value("${app.kafka.topico.reserva-cancelada}")
    private String topicoReservaCancelada;

    @KafkaListener(
            topics = {"${app.kafka.topico.limite-reservado}", "${app.kafka.topico.reserva-cancelada}"},
            groupId = "${app.kafka.grupo.saga-reserva}",
            containerFactory = "clienteDaSagaKafkaListenerContainerFactory")
    public void receber(ConsumerRecord<String, String> registro,
                        Acknowledgment confirmacao) throws JsonProcessingException {
        String eventoId = lerCabecalhoObrigatorio(registro, "ce_id");
        if (topicoLimiteReservado.equals(registro.topic())) {
            var evento = objectMapperDosEventos.readValue(registro.value(), LimiteDeCreditoReservadoEvent.class);
            acompanhamentoReservaService.registrarReserva(eventoId, evento.getSolicitacaoId());
        } else if (topicoReservaCancelada.equals(registro.topic())) {
            var evento = objectMapperDosEventos.readValue(registro.value(), ReservaDeLimiteCanceladaEvent.class);
            acompanhamentoReservaService.registrarCancelamento(eventoId, evento.getSolicitacaoId());
        } else {
            throw new IllegalArgumentException("topico da saga nao reconhecido: " + registro.topic());
        }
        confirmacao.acknowledge();
    }

    private String lerCabecalhoObrigatorio(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            throw new IllegalArgumentException("cabecalho " + nome + " e obrigatorio");
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}

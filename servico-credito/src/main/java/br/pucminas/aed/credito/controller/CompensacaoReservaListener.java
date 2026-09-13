package br.pucminas.aed.credito.controller;

import br.pucminas.aed.credito.domain.PropostaDeCreditoExpiradaEvent;
import br.pucminas.aed.credito.domain.PropostaDeCreditoRecusadaEvent;
import br.pucminas.aed.credito.service.CompensacaoReservaService;
import br.pucminas.aed.credito.service.ReservaCanceladaPublicacaoService;
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
public class CompensacaoReservaListener {
    private final CompensacaoReservaService compensacaoReservaService;
    private final ReservaCanceladaPublicacaoService reservaCanceladaPublicacaoService;
    private final ObjectMapper objectMapperDosEventos;

    @Value("${app.kafka.topico.proposta-recusada}")
    private String topicoPropostaRecusada;

    @Value("${app.kafka.topico.proposta-expirada}")
    private String topicoPropostaExpirada;

    @KafkaListener(
            topics = {"${app.kafka.topico.proposta-recusada}", "${app.kafka.topico.proposta-expirada}"},
            groupId = "${app.kafka.grupo.compensacao-reserva}",
            containerFactory = "clienteDeCompensacaoKafkaListenerContainerFactory")
    public void receber(ConsumerRecord<String, String> registro,
                        Acknowledgment confirmacao) throws JsonProcessingException {
        String eventoId = lerCabecalhoObrigatorio(registro, "ce_id");
        String solicitacaoId;
        String motivo;

        if (topicoPropostaRecusada.equals(registro.topic())) {
            var evento = objectMapperDosEventos.readValue(
                    registro.value(), PropostaDeCreditoRecusadaEvent.class);
            solicitacaoId = evento.getSolicitacaoId();
            motivo = "PROPOSTA_RECUSADA";
        } else if (topicoPropostaExpirada.equals(registro.topic())) {
            var evento = objectMapperDosEventos.readValue(
                    registro.value(), PropostaDeCreditoExpiradaEvent.class);
            solicitacaoId = evento.getSolicitacaoId();
            motivo = "PROPOSTA_EXPIRADA";
        } else {
            throw new IllegalArgumentException("topico de compensacao nao reconhecido: " + registro.topic());
        }

        var cancelamento = compensacaoReservaService.cancelar(eventoId, solicitacaoId, motivo);
        reservaCanceladaPublicacaoService.publicar(cancelamento);
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

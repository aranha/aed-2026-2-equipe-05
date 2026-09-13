package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ReservaCanceladaPublicacaoService {
    private static final String TIPO_EVENTO = "credito.reserva-limite.cancelada.v1";
    private static final String FONTE_EVENTO = "/credito/limites";

    private final KafkaTemplate<String, ReservaDeLimiteCanceladaEvent> clienteDoBroker;
    private final ResultadoPublicacaoService resultadoPublicacaoService;
    private final String topico;

    public ReservaCanceladaPublicacaoService(
            @Qualifier("clienteDoBrokerReservaCancelada")
            KafkaTemplate<String, ReservaDeLimiteCanceladaEvent> clienteDoBroker,
            ResultadoPublicacaoService resultadoPublicacaoService,
            @Value("${app.kafka.topico.reserva-cancelada}") String topico) {
        this.clienteDoBroker = clienteDoBroker;
        this.resultadoPublicacaoService = resultadoPublicacaoService;
        this.topico = topico;
    }

    public void publicar(ReservaDeLimiteCanceladaEvent evento) {
        var registro = new ProducerRecord<String, ReservaDeLimiteCanceladaEvent>(
                topico, evento.getSolicitacaoId(), evento);
        adicionarCabecalho(registro, "ce_specversion", "1.0");
        adicionarCabecalho(registro, "ce_id", evento.getEventoId());
        adicionarCabecalho(registro, "ce_source", FONTE_EVENTO);
        adicionarCabecalho(registro, "ce_type", TIPO_EVENTO);
        adicionarCabecalho(registro, "ce_time", evento.getDataCancelamento().toString());

        try {
            var resultado = clienteDoBroker.send(registro).join();
            resultadoPublicacaoService.registrar(evento.getEventoId(), resultado, null);
        } catch (RuntimeException falha) {
            resultadoPublicacaoService.registrar(evento.getEventoId(), null, falha);
            throw falha;
        }
    }

    private void adicionarCabecalho(ProducerRecord<String, ReservaDeLimiteCanceladaEvent> registro,
                                    String nome, String valor) {
        registro.headers().add(new RecordHeader(nome, valor.getBytes(StandardCharsets.UTF_8)));
    }
}

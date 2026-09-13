package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.LimiteDeCreditoReservadoEvent;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class LimiteReservadoPublicacaoService {
    private static final String TIPO_EVENTO = "credito.limite.reservado.v1";
    private static final String FONTE_EVENTO = "/credito/limites";

    private final KafkaTemplate<String, LimiteDeCreditoReservadoEvent> clienteDoBroker;
    private final ResultadoPublicacaoService resultadoPublicacaoService;
    private final String topico;

    public LimiteReservadoPublicacaoService(
            @Qualifier("clienteDoBrokerLimiteReservado")
            KafkaTemplate<String, LimiteDeCreditoReservadoEvent> clienteDoBroker,
            ResultadoPublicacaoService resultadoPublicacaoService,
            @Value("${app.kafka.topico.limite-reservado}") String topico) {
        this.clienteDoBroker = clienteDoBroker;
        this.resultadoPublicacaoService = resultadoPublicacaoService;
        this.topico = topico;
    }

    public void publicar(LimiteDeCreditoReservadoEvent evento) {
        var registro = new ProducerRecord<String, LimiteDeCreditoReservadoEvent>(
                topico, evento.getSolicitacaoId(), evento);
        adicionarCabecalho(registro, "ce_specversion", "1.0");
        adicionarCabecalho(registro, "ce_id", evento.getEventoId());
        adicionarCabecalho(registro, "ce_source", FONTE_EVENTO);
        adicionarCabecalho(registro, "ce_type", TIPO_EVENTO);
        adicionarCabecalho(registro, "ce_time", evento.getDataReserva().toString());
        clienteDoBroker.send(registro).whenComplete(
                (resultado, falha) -> resultadoPublicacaoService.registrar(
                        evento.getEventoId(), resultado, falha));
    }

    private void adicionarCabecalho(ProducerRecord<String, LimiteDeCreditoReservadoEvent> registro,
                                    String nome, String valor) {
        registro.headers().add(new RecordHeader(nome, valor.getBytes(StandardCharsets.UTF_8)));
    }
}

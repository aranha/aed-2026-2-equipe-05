package br.pucminas.aed.solicitacaocredito.service;

import br.pucminas.aed.solicitacaocredito.domain.CreditoSolicitadoEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class SolicitacaoCreditoService {

    private static final String TIPO_DO_EVENTO = "banco.credito.solicitado.v1";
    private static final String ORIGEM = "/banco/servico-solicitacao-credito";
    private static final String VERSAO_CLOUDEVENTS = "1.0";

    private final KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker;
    private final String topico;

    public SolicitacaoCreditoService(KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker, @Value("${demo.topico}") String topico) {
        this.clienteDoBroker = clienteDoBroker;
        this.topico = topico;
    }

    public void solicitar(CreditoSolicitadoEvent evento) {
        ProducerRecord<String, CreditoSolicitadoEvent> registro = new ProducerRecord<String, CreditoSolicitadoEvent>(topico, evento.getSolicitacaoId(), evento);

        registro.headers().add("ce_specversion", VERSAO_CLOUDEVENTS.getBytes(UTF_8));
        registro.headers().add("ce_id", evento.getEventoId().getBytes(UTF_8));
        registro.headers().add("ce_source", ORIGEM.getBytes(UTF_8));
        registro.headers().add("ce_type", TIPO_DO_EVENTO.getBytes(UTF_8));
        registro.headers().add("ce_time", evento.getOcorridoEm().toString().getBytes(UTF_8));

        SolicitacaoCreditoCallbackService aoConcluir = new SolicitacaoCreditoCallbackService(evento);
        clienteDoBroker.send(registro).whenComplete(aoConcluir);
    }

}
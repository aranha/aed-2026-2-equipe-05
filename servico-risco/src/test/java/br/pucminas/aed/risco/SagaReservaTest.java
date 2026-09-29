package br.pucminas.aed.risco;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import br.pucminas.aed.risco.service.AnaliseCreditoRepository;
import br.pucminas.aed.risco.service.EventoProcessadoRepository;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {
        "credito.solicitacao.solicitada.v1",
        "credito.limite.reservado.v1",
        "credito.reserva-limite.cancelada.v1"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:risco-saga;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.retentativa.tentativas=4",
        "app.kafka.retentativa.intervalo-inicial-ms=500",
        "app.kafka.retentativa.intervalo-maximo-ms=2000"
})
class SagaReservaTest {
    private static final String TOPICO_SOLICITADO = "credito.solicitacao.solicitada.v1";
    private static final String TOPICO_RESERVADO = "credito.limite.reservado.v1";
    private static final String TOPICO_CANCELADA = "credito.reserva-limite.cancelada.v1";
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private AnaliseCreditoRepository analiseCreditoRepository;
    @Autowired private EventoProcessadoRepository eventoProcessadoRepository;
    @Value("${spring.embedded.kafka.brokers}") private String servidores;

    private KafkaProducer<String, String> publicador;

    @BeforeEach
    void preparar() {
        analiseCreditoRepository.excluirTodos();
        eventoProcessadoRepository.excluirTodos();
        Properties propriedades = new Properties();
        propriedades.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedades.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propriedades.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propriedades.put(ProducerConfig.ACKS_CONFIG, "all");
        publicador = new KafkaProducer<>(propriedades);
    }

    @AfterEach
    void encerrar() {
        publicador.close();
    }

    @Test
    void estadoFinalDaSagaFicaObservavelNaAnaliseDeCredito() {
        publicarSolicitado("evt-sol-201", "sol-201");
        aguardarStatus("sol-201", "PENDENTE");

        publicarReservado("evt-res-201", "sol-201");
        aguardarStatus("sol-201", "RESERVADA");

        publicarCancelado("evt-can-201", "sol-201");
        aguardarStatus("sol-201", "CANCELADA");
    }

    @Test
    void cancelamentoProcessadoAntesDaReservaNaoEDesfeitoPelaReservaAtrasada() {
        publicarSolicitado("evt-sol-202", "sol-202");
        aguardarStatus("sol-202", "PENDENTE");

        publicarCancelado("evt-can-202", "sol-202");
        aguardarStatus("sol-202", "CANCELADA");

        publicarReservado("evt-res-202", "sol-202");
        Awaitility.await().atMost(PRAZO).until(() -> eventoProcessadoRepository.contar() == 3);
        assertThat(status("sol-202")).isEqualTo("CANCELADA");
    }

    @Test
    void reservaQueChegaAntesDaAnaliseEAplicadaPelaRetentativa() {
        publicarReservado("evt-res-203", "sol-203");
        publicador.flush();
        sleep(Duration.ofMillis(700));
        publicarSolicitado("evt-sol-203", "sol-203");

        aguardarStatus("sol-203", "RESERVADA");
    }

    @Test
    void mesmoEventoDaSagaEntregueDuasVezesAplicaUmaSo() {
        publicarSolicitado("evt-sol-204", "sol-204");
        aguardarStatus("sol-204", "PENDENTE");

        publicarReservado("evt-res-204", "sol-204");
        publicarReservado("evt-res-204", "sol-204");

        aguardarStatus("sol-204", "RESERVADA");
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(6))
                .untilAsserted(() -> assertThat(eventoProcessadoRepository.contar()).isEqualTo(2));
    }

    private void aguardarStatus(String solicitacaoId, String esperado) {
        publicador.flush();
        Awaitility.await().atMost(PRAZO).untilAsserted(() -> assertThat(status(solicitacaoId)).isEqualTo(esperado));
    }

    private String status(String solicitacaoId) {
        return analiseCreditoRepository.buscarStatus(solicitacaoId).orElse(null);
    }

    private void publicarSolicitado(String eventoId, String solicitacaoId) {
        publicar(TOPICO_SOLICITADO, eventoId, solicitacaoId, """
                {"eventoId":"%s","solicitacaoId":"%s","clienteId":"cli-ficticio-001",
                 "valorSolicitado":3000.00,"dataSolicitacao":"2026-09-27T10:00:00-03:00","canalOrigem":"APP"}
                """.formatted(eventoId, solicitacaoId));
    }

    private void publicarReservado(String eventoId, String solicitacaoId) {
        publicar(TOPICO_RESERVADO, eventoId, solicitacaoId, """
                {"eventoId":"%s","solicitacaoId":"%s","clienteId":"cli-ficticio-001","valorReservado":3000.00,
                 "limiteDisponivel":7000.00,"dataReserva":"2026-09-27T10:01:00-03:00"}
                """.formatted(eventoId, solicitacaoId));
    }

    private void publicarCancelado(String eventoId, String solicitacaoId) {
        publicar(TOPICO_CANCELADA, eventoId, solicitacaoId, """
                {"eventoId":"%s","eventoOrigemId":"evt-recusa","solicitacaoId":"%s","clienteId":"cli-ficticio-001",
                 "valorDevolvido":3000.00,"limiteDisponivel":10000.00,"motivo":"PROPOSTA_RECUSADA",
                 "dataCancelamento":"2026-09-27T10:02:00-03:00"}
                """.formatted(eventoId, solicitacaoId));
    }

    private void publicar(String topico, String eventoId, String solicitacaoId, String json) {
        var registro = new ProducerRecord<String, String>(topico, solicitacaoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/credito/limites".getBytes(UTF_8));
        registro.headers().add("ce_type", topico.getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-09-27T10:00:00-03:00".getBytes(UTF_8));
        publicador.send(registro);
    }

    private static void sleep(Duration duracao) {
        try {
            Thread.sleep(duracao.toMillis());
        } catch (InterruptedException interrompido) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.aed20262equipe05.risco;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.aed20262equipe05.risco.service.AnaliseCreditoRepository;
import com.aed20262equipe05.risco.service.EventoProcessadoRepository;
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
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "credito.solicitacao.solicitada.v1")
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:risco;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class IdempotenciaTest {
    private static final String TOPICO = "credito.solicitacao.solicitada.v1";
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private EventoProcessadoRepository eventoProcessadoRepository;
    @Autowired private AnaliseCreditoRepository analiseCreditoRepository;
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
    void mesmoEventoEntregueTresVezesProduzUmUnicoEfeito() {
        String eventoId = "evt-reentrega-001";
        String json = eventoJson("sol-001");

        publicar(eventoId, "sol-001", json);
        publicar(eventoId, "sol-001", json);
        publicar(eventoId, "sol-001", json);
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(eventoProcessadoRepository.contar()).isEqualTo(1);
            assertThat(analiseCreditoRepository.contar()).isEqualTo(1);
        });
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(analiseCreditoRepository.contar()).isEqualTo(1));
    }

    @Test
    void consumidorIgnoraCampoQueNaoDeclarou() {
        publicar("evt-tolerante-001", "sol-002", eventoJson("sol-002"));
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(eventoProcessadoRepository.contar()).isEqualTo(1);
            assertThat(analiseCreditoRepository.contar()).isEqualTo(1);
        });
    }

    private void publicar(String eventoId, String solicitacaoId, String json) {
        var registro = new ProducerRecord<String, String>(TOPICO, solicitacaoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/credito/solicitacoes".getBytes(UTF_8));
        registro.headers().add("ce_type",
                "credito.solicitacao.credito-solicitado.v1".getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-08-15T20:30:00-03:00".getBytes(UTF_8));
        publicador.send(registro);
    }

    private String eventoJson(String solicitacaoId) {
        return """
                {
                  "eventoId": "id-do-corpo-nao-usado-para-deduplicacao",
                  "solicitacaoId": "%s",
                  "clienteId": "cli-ficticio-001",
                  "valorSolicitado": 15000.00,
                  "dataSolicitacao": "2026-08-15T20:30:00-03:00",
                  "canalOrigem": "CAMPO_DESCONHECIDO_PELO_CONSUMIDOR"
                }
                """.formatted(solicitacaoId);
    }
}

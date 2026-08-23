package br.pucminas.aed.risco;

import br.pucminas.aed.risco.service.AnaliseCreditoRepository;
import br.pucminas.aed.risco.service.EventoProcessadoRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "credito.solicitacao.solicitada.v1")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:risco-fluxo;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest"})
@ExtendWith(OutputCaptureExtension.class)
class FluxoCreditoSolicitadoTest {

    private static final String TOPICO = "credito.solicitacao.solicitada.v1";
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Autowired
    private EventoProcessadoRepository eventoProcessadoRepository;
    @Autowired
    private AnaliseCreditoRepository analiseCreditoRepository;
    private KafkaProducer<String, String> publicador;

    @BeforeEach
    void preparar() {
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
    void agregaValorSolicitadoPorJanelaDeCincoMinutos(CapturedOutput saida) {
        publicar("evt-fluxo-001", "sol-fluxo-001", eventoJson("sol-fluxo-001", "1000.00", "2026-08-22T12:02:34-03:00"));
        publicar("evt-fluxo-002", "sol-fluxo-002", eventoJson("sol-fluxo-002", "2000.00", "2026-08-22T12:04:10-03:00"));
        publicar("evt-fluxo-003", "sol-fluxo-003", eventoJson("sol-fluxo-003", "5000.00", "2026-08-22T12:07:00-03:00"));
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(saida)
                    .contains("Fluxo de credito solicitado | janela=2026-08-22T12:00-03:00 | quantidade=2 | totalSolicitado=3000.00")
                    .contains("Fluxo de credito solicitado | janela=2026-08-22T12:05-03:00 | quantidade=1 | totalSolicitado=5000.00");
            assertThat(eventoProcessadoRepository.contar()).isEqualTo(3);
            assertThat(analiseCreditoRepository.contar()).isEqualTo(3);
        });
    }

    private void publicar(String eventoId, String solicitacaoId, String json) {
        var registro = new ProducerRecord<String, String>(TOPICO, solicitacaoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/credito/solicitacoes".getBytes(UTF_8));
        registro.headers().add("ce_type", "credito.solicitacao.solicitada.v1".getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-08-22T12:00:00-03:00".getBytes(UTF_8));
        publicador.send(registro);
    }

    private String eventoJson(String solicitacaoId, String valorSolicitado, String dataSolicitacao) {
        return """
                {
                  "eventoId": "id-do-corpo-nao-usado-no-teste-de-fluxo",
                  "solicitacaoId": "%s",
                  "clienteId": "cli-ficticio-001",
                  "valorSolicitado": %s,
                  "dataSolicitacao": "%s",
                  "canalOrigem": "APP"
                }
                """.formatted(solicitacaoId, valorSolicitado, dataSolicitacao);
    }

}

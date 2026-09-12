package br.pucminas.aed.credito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {
        "credito.solicitacao.solicitada.v1",
        "credito.elegibilidade.aprovada.v1",
        "credito.limite.reservado.v1"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:credito;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class ReservaLimiteIntegracaoTest {
    private static final String TOPICO_ENTRADA = "credito.elegibilidade.aprovada.v1";
    private static final String TOPICO_SAIDA = "credito.limite.reservado.v1";
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private JdbcTemplate bancoDeDados;
    @Value("${spring.embedded.kafka.brokers}") private String servidores;

    private KafkaProducer<String, String> publicador;
    private KafkaConsumer<String, String> consumidor;

    @BeforeEach
    void preparar() {
        bancoDeDados.update("delete from reserva_limite");
        bancoDeDados.update("delete from limite_credito");
        bancoDeDados.update("""
                insert into limite_credito (cliente_id, limite_total, limite_disponivel)
                values (?, ?, ?)
                """, "cli-001", new BigDecimal("10000.00"), new BigDecimal("10000.00"));

        Properties propriedadesPublicador = new Properties();
        propriedadesPublicador.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedadesPublicador.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propriedadesPublicador.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propriedadesPublicador.put(ProducerConfig.ACKS_CONFIG, "all");
        publicador = new KafkaProducer<>(propriedadesPublicador);

        Properties propriedadesConsumidor = new Properties();
        propriedadesConsumidor.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedadesConsumidor.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-limite-" + UUID.randomUUID());
        propriedadesConsumidor.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propriedadesConsumidor.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propriedadesConsumidor.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumidor = new KafkaConsumer<>(propriedadesConsumidor);
        consumidor.subscribe(List.of(TOPICO_SAIDA));
    }

    @AfterEach
    void encerrar() {
        publicador.close();
        consumidor.close();
    }

    @Test
    void reservaLimiteEPublicaEventoQuandoElegibilidadeEaprovada() {
        assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");

        publicarElegibilidadeAprovada("evt-eleg-001", "sol-001", "3000.00");
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("7000.00");
            assertThat(statusDaReserva("sol-001")).isEqualTo("RESERVADA");
        });

        var eventoPublicado = aguardarEventoDeReserva();
        assertThat(eventoPublicado.key()).isEqualTo("sol-001");
        assertThat(eventoPublicado.value())
                .contains("\"solicitacaoId\":\"sol-001\"")
                .contains("\"clienteId\":\"cli-001\"")
                .contains("\"valorReservado\":3000.00")
                .contains("\"limiteDisponivel\":7000.00");
        assertThat(new String(eventoPublicado.headers().lastHeader("ce_type").value(), UTF_8))
                .isEqualTo("credito.limite.reservado.v1");
    }

    private void publicarElegibilidadeAprovada(String eventoId, String solicitacaoId,
                                                String valorAprovado) {
        String json = """
                {
                  "eventoId": "%s",
                  "solicitacaoId": "%s",
                  "clienteId": "cli-001",
                  "valorAprovado": %s,
                  "dataAprovacao": "2026-09-12T15:00:00-03:00"
                }
                """.formatted(eventoId, solicitacaoId, valorAprovado);

        var registro = new ProducerRecord<String, String>(TOPICO_ENTRADA, solicitacaoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/risco/elegibilidade".getBytes(UTF_8));
        registro.headers().add("ce_type", "credito.elegibilidade.aprovada.v1".getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-09-12T15:00:00-03:00".getBytes(UTF_8));
        publicador.send(registro);
    }

    private org.apache.kafka.clients.consumer.ConsumerRecord<String, String> aguardarEventoDeReserva() {
        long limite = System.nanoTime() + PRAZO.toNanos();
        while (System.nanoTime() < limite) {
            var registros = consumidor.poll(Duration.ofMillis(500));
            for (var registro : registros) {
                if (TOPICO_SAIDA.equals(registro.topic()) && "sol-001".equals(registro.key())) {
                    return registro;
                }
            }
        }
        throw new AssertionError("evento LimiteDeCreditoReservado nao foi publicado");
    }

    private BigDecimal limiteDisponivel() {
        return bancoDeDados.queryForObject(
                "select limite_disponivel from limite_credito where cliente_id = 'cli-001'",
                BigDecimal.class);
    }

    private String statusDaReserva(String solicitacaoId) {
        return bancoDeDados.queryForObject(
                "select status from reserva_limite where solicitacao_id = ?",
                String.class, solicitacaoId);
    }
}

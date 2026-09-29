package br.pucminas.aed.credito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
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
        "credito.limite.reservado.v1",
        "credito.proposta.recusada.v1",
        "credito.proposta.expirada.v1",
        "credito.reserva-limite.cancelada.v1",
        "credito.elegibilidade.aprovada.v1.dlq",
        "credito.proposta.recusada.v1.dlq",
        "credito.proposta.expirada.v1.dlq"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:credito-falha-saga;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.retentativa.tentativas=2",
        "app.kafka.retentativa.intervalo-inicial-ms=50",
        "app.kafka.retentativa.intervalo-maximo-ms=100"
})
class FalhaDaSagaIntegracaoTest {
    private static final String TOPICO_ELEGIBILIDADE = "credito.elegibilidade.aprovada.v1";
    private static final String TOPICO_RECUSADA = "credito.proposta.recusada.v1";
    private static final String TOPICO_EXPIRADA = "credito.proposta.expirada.v1";
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private JdbcTemplate bancoDeDados;
    @Value("${spring.embedded.kafka.brokers}") private String servidores;

    private KafkaProducer<String, String> publicador;
    private KafkaConsumer<String, String> leitorDaDlq;
    private final List<ConsumerRecord<String, String>> lidosDaDlq = new ArrayList<>();

    @BeforeEach
    void preparar() {
        bancoDeDados.update("delete from cancelamento_reserva");
        bancoDeDados.update("delete from reserva_limite");
        bancoDeDados.update("delete from limite_credito");
        bancoDeDados.update("""
                insert into limite_credito (cliente_id, limite_total, limite_disponivel)
                values (?, ?, ?)
                """, "cli-001", new BigDecimal("10000.00"), new BigDecimal("10000.00"));
        lidosDaDlq.clear();

        Properties doPublicador = new Properties();
        doPublicador.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        doPublicador.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        doPublicador.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        doPublicador.put(ProducerConfig.ACKS_CONFIG, "all");
        publicador = new KafkaProducer<>(doPublicador);

        Properties doLeitor = new Properties();
        doLeitor.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        doLeitor.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-leitor-dlq-" + System.nanoTime());
        doLeitor.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        doLeitor.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        doLeitor.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        leitorDaDlq = new KafkaConsumer<>(doLeitor);
        leitorDaDlq.subscribe(List.of(TOPICO_ELEGIBILIDADE + ".dlq", TOPICO_RECUSADA + ".dlq", TOPICO_EXPIRADA + ".dlq"));
    }

    @AfterEach
    void encerrar() {
        publicador.close();
        leitorDaDlq.close();
    }

    @Test
    void recusaQueChegaAntesDaReservaVaiParaADlqComoFalhaTransitoria() {
        publicarRecusa("evt-recusa-401", "sol-401");

        var naDlq = aguardarNaDlq("evt-recusa-401");
        assertThat(cabecalho(naDlq, "classificacao")).isEqualTo("TRANSITORIA");
        assertThat(cabecalho(naDlq, "kafka_dlt-exception-message")).contains("reserva de limite nao encontrada");
    }

    @Test
    void recusaEExpiracaoDaMesmaSolicitacaoDevolvemOLimiteUmaVezSo() {
        publicarElegibilidade("evt-eleg-402", "sol-402", "3000.00");
        Awaitility.await().atMost(PRAZO).untilAsserted(() ->
                assertThat(limiteDisponivel()).isEqualByComparingTo("7000.00"));

        publicarRecusa("evt-recusa-402", "sol-402");
        publicarExpiracao("evt-expiracao-402", "sol-402");

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(statusDaReserva("sol-402")).isEqualTo("CANCELADA");
            assertThat(canceladaEmPreenchido("sol-402")).isTrue();
        });
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(6)).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(quantidadeCancelamentos("sol-402")).isEqualTo(1);
            assertThat(eventosNaDlq("evt-recusa-402", "evt-expiracao-402")).isZero();
        });
    }

    @Test
    void limiteInsuficienteVaiDiretoParaADlqComoFalhaPermanente() throws Exception {
        publicarElegibilidade("evt-eleg-403", "sol-403", "50000.00");

        var naDlq = aguardarNaDlq("evt-eleg-403");
        assertThat(cabecalho(naDlq, "classificacao")).isEqualTo("PERMANENTE");
        assertThat(cabecalho(naDlq, "kafka_dlt-exception-message")).contains("limite de credito insuficiente");
        assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
    }

    private void publicarElegibilidade(String eventoId, String solicitacaoId, String valor) {
        publicar(TOPICO_ELEGIBILIDADE, eventoId, solicitacaoId, """
                {"eventoId":"%s","solicitacaoId":"%s","clienteId":"cli-001","valorAprovado":%s,
                 "dataAprovacao":"2026-09-27T10:00:00-03:00"}
                """.formatted(eventoId, solicitacaoId, valor));
    }

    private void publicarRecusa(String eventoId, String solicitacaoId) {
        publicar(TOPICO_RECUSADA, eventoId, solicitacaoId, """
                {"eventoId":"%s","solicitacaoId":"%s","motivo":"POLITICA_DE_CREDITO",
                 "dataRecusa":"2026-09-27T10:05:00-03:00"}
                """.formatted(eventoId, solicitacaoId));
    }

    private void publicarExpiracao(String eventoId, String solicitacaoId) {
        publicar(TOPICO_EXPIRADA, eventoId, solicitacaoId, """
                {"eventoId":"%s","solicitacaoId":"%s","dataExpiracao":"2026-09-27T10:30:00-03:00"}
                """.formatted(eventoId, solicitacaoId));
    }

    private void publicar(String topico, String eventoId, String solicitacaoId, String json) {
        var registro = new ProducerRecord<String, String>(topico, solicitacaoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/credito/propostas".getBytes(UTF_8));
        registro.headers().add("ce_type", topico.getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-09-27T10:00:00-03:00".getBytes(UTF_8));
        publicador.send(registro);
        publicador.flush();
    }

    private ConsumerRecord<String, String> aguardarNaDlq(String eventoId) {
        long limite = System.currentTimeMillis() + PRAZO.toMillis();
        while (System.currentTimeMillis() < limite) {
            leitorDaDlq.poll(Duration.ofMillis(300)).forEach(lidosDaDlq::add);
            for (var registro : lidosDaDlq) {
                if (eventoId.equals(cabecalho(registro, "ce_id"))) {
                    return registro;
                }
            }
        }
        throw new AssertionError("evento " + eventoId + " nao chegou a DLQ");
    }

    private long eventosNaDlq(String... eventoIds) {
        leitorDaDlq.poll(Duration.ofMillis(300)).forEach(lidosDaDlq::add);
        var procurados = List.of(eventoIds);
        return lidosDaDlq.stream().filter(registro -> procurados.contains(cabecalho(registro, "ce_id"))).count();
    }

    private String cabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : new String(cabecalho.value(), UTF_8);
    }

    private BigDecimal limiteDisponivel() {
        return bancoDeDados.queryForObject(
                "select limite_disponivel from limite_credito where cliente_id = 'cli-001'", BigDecimal.class);
    }

    private String statusDaReserva(String solicitacaoId) {
        return bancoDeDados.queryForList(
                "select status from reserva_limite where solicitacao_id = ?", String.class, solicitacaoId)
                .stream().findFirst().orElse(null);
    }

    private boolean canceladaEmPreenchido(String solicitacaoId) {
        Integer quantidade = bancoDeDados.queryForObject(
                "select count(*) from reserva_limite where solicitacao_id = ? and cancelada_em is not null",
                Integer.class, solicitacaoId);
        return quantidade != null && quantidade == 1;
    }

    private Integer quantidadeCancelamentos(String solicitacaoId) {
        return bancoDeDados.queryForObject(
                "select count(*) from cancelamento_reserva where solicitacao_id = ?", Integer.class, solicitacaoId);
    }
}

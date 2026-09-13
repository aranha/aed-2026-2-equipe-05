package br.pucminas.aed.credito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import br.pucminas.aed.credito.service.ReservaCanceladaPublicacaoService;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
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
import org.springframework.boot.test.mock.mockito.SpyBean;
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
        "credito.reserva-limite.cancelada.v1"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:credito-compensacao;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class CompensacaoReservaIntegracaoTest {
    private static final String TOPICO_RECUSADA = "credito.proposta.recusada.v1";
    private static final String TOPICO_EXPIRADA = "credito.proposta.expirada.v1";
    private static final String TOPICO_SAIDA = "credito.reserva-limite.cancelada.v1";
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private JdbcTemplate bancoDeDados;
    @Value("${spring.embedded.kafka.brokers}") private String servidores;

    @SpyBean
    private ReservaCanceladaPublicacaoService reservaCanceladaPublicacaoService;

    private KafkaProducer<String, String> publicador;
    private KafkaConsumer<String, String> consumidor;

    @BeforeEach
    void preparar() {
        bancoDeDados.update("delete from cancelamento_reserva");
        bancoDeDados.update("delete from reserva_limite");
        bancoDeDados.update("delete from limite_credito");
        bancoDeDados.update("""
                insert into limite_credito (cliente_id, limite_total, limite_disponivel)
                values (?, ?, ?)
                """, "cli-001", new BigDecimal("10000.00"), new BigDecimal("7000.00"));
        bancoDeDados.update("""
                insert into reserva_limite
                    (solicitacao_id, evento_origem_id, evento_reserva_id, cliente_id,
                     valor_reservado, limite_disponivel_apos, status, reservada_em)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """, "sol-001", "evt-eleg-001", "evt-reserva-001", "cli-001",
                new BigDecimal("3000.00"), new BigDecimal("7000.00"), "RESERVADA",
                OffsetDateTime.parse("2026-09-12T15:00:00-03:00"));

        Properties propriedadesPublicador = new Properties();
        propriedadesPublicador.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedadesPublicador.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propriedadesPublicador.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        propriedadesPublicador.put(ProducerConfig.ACKS_CONFIG, "all");
        publicador = new KafkaProducer<>(propriedadesPublicador);

        Properties propriedadesConsumidor = new Properties();
        propriedadesConsumidor.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedadesConsumidor.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-cancelamento-" + UUID.randomUUID());
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
    void devolveLimiteEPublicaCancelamentoQuandoPropostaERecusada() {
        String eventoOrigemId = "evt-recusa-001";
        assertThat(limiteDisponivel()).isEqualByComparingTo("7000.00");

        publicarPropostaRecusada(eventoOrigemId, "sol-001");
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(motivoDoCancelamento("sol-001")).isEqualTo("PROPOSTA_RECUSADA");
        });

        var eventoPublicado = aguardarEventoDeCancelamento(eventoOrigemId);
        assertThat(eventoPublicado.key()).isEqualTo("sol-001");
        assertThat(eventoPublicado.value())
                .contains("\"eventoOrigemId\":\"evt-recusa-001\"")
                .contains("\"solicitacaoId\":\"sol-001\"")
                .contains("\"clienteId\":\"cli-001\"")
                .contains("\"valorDevolvido\":3000.00")
                .contains("\"limiteDisponivel\":10000.00")
                .contains("\"motivo\":\"PROPOSTA_RECUSADA\"");
        assertThat(new String(eventoPublicado.headers().lastHeader("ce_type").value(), UTF_8))
                .isEqualTo("credito.reserva-limite.cancelada.v1");
    }

    @Test
    void devolveLimiteEPublicaCancelamentoQuandoPropostaExpira() {
        String eventoOrigemId = "evt-expiracao-001";
        publicarPropostaExpirada(eventoOrigemId, "sol-001");
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(motivoDoCancelamento("sol-001")).isEqualTo("PROPOSTA_EXPIRADA");
        });

        var eventoPublicado = aguardarEventoDeCancelamento(eventoOrigemId);
        assertThat(eventoPublicado.value())
                .contains("\"eventoOrigemId\":\"evt-expiracao-001\"")
                .contains("\"motivo\":\"PROPOSTA_EXPIRADA\"");
    }

    @Test
    void mesmoEventoPodeSerReexecutadoSemDevolverLimiteDuasVezes() {
        String eventoOrigemId = "evt-recusa-idempotente-001";

        publicarPropostaRecusada(eventoOrigemId, "sol-001");
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(quantidadeCancelamentos(eventoOrigemId)).isEqualTo(1);
        });

        var primeiraPublicacao = aguardarEventoDeCancelamento(eventoOrigemId);
        String eventoCancelamentoId = eventoCancelamentoId(eventoOrigemId);
        assertThat(new String(primeiraPublicacao.headers().lastHeader("ce_id").value(), UTF_8))
                .isEqualTo(eventoCancelamentoId);

        publicarPropostaRecusada(eventoOrigemId, "sol-001");
        publicador.flush();

        var segundaPublicacao = aguardarEventoDeCancelamento(eventoOrigemId);
        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(quantidadeCancelamentos(eventoOrigemId)).isEqualTo(1);
        });

        assertThat(new String(segundaPublicacao.headers().lastHeader("ce_id").value(), UTF_8))
                .isEqualTo(eventoCancelamentoId);
    }

    @Test
    void reexecutaMesmoEventoQuandoAPublicacaoFalhaAposACompensacao() {
        String eventoOrigemId = "evt-recusa-retry-001";

        doThrow(new IllegalStateException("falha simulada ao publicar compensacao"))
                .doCallRealMethod()
                .when(reservaCanceladaPublicacaoService)
                .publicar(any(ReservaDeLimiteCanceladaEvent.class));

        publicarPropostaRecusada(eventoOrigemId, "sol-001");
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            verify(reservaCanceladaPublicacaoService, atLeast(2))
                    .publicar(any(ReservaDeLimiteCanceladaEvent.class));
            assertThat(limiteDisponivel()).isEqualByComparingTo("10000.00");
            assertThat(quantidadeCancelamentos(eventoOrigemId)).isEqualTo(1);
        });

        var eventoPublicado = aguardarEventoDeCancelamento(eventoOrigemId);
        String eventoCancelamentoId = eventoCancelamentoId(eventoOrigemId);
        assertThat(new String(eventoPublicado.headers().lastHeader("ce_id").value(), UTF_8))
                .isEqualTo(eventoCancelamentoId);
    }

    private void publicarPropostaRecusada(String eventoId, String solicitacaoId) {
        String json = """
                {
                  "eventoId": "%s",
                  "solicitacaoId": "%s",
                  "motivo": "POLITICA_DE_CREDITO",
                  "dataRecusa": "2026-09-12T16:00:00-03:00"
                }
                """.formatted(eventoId, solicitacaoId);
        publicar(TOPICO_RECUSADA, eventoId, solicitacaoId, json, "credito.proposta.recusada.v1");
    }

    private void publicarPropostaExpirada(String eventoId, String solicitacaoId) {
        String json = """
                {
                  "eventoId": "%s",
                  "solicitacaoId": "%s",
                  "dataExpiracao": "2026-09-12T16:30:00-03:00"
                }
                """.formatted(eventoId, solicitacaoId);
        publicar(TOPICO_EXPIRADA, eventoId, solicitacaoId, json, "credito.proposta.expirada.v1");
    }

    private void publicar(String topico, String eventoId, String solicitacaoId,
                           String json, String tipo) {
        var registro = new ProducerRecord<String, String>(topico, solicitacaoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/credito/propostas".getBytes(UTF_8));
        registro.headers().add("ce_type", tipo.getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-09-12T16:00:00-03:00".getBytes(UTF_8));
        publicador.send(registro);
    }

    private org.apache.kafka.clients.consumer.ConsumerRecord<String, String>
            aguardarEventoDeCancelamento(String eventoOrigemId) {
        long limite = System.nanoTime() + PRAZO.toNanos();
        String trechoEsperado = "\"eventoOrigemId\":\"" + eventoOrigemId + "\"";
        while (System.nanoTime() < limite) {
            var registros = consumidor.poll(Duration.ofMillis(500));
            for (var registro : registros) {
                if (TOPICO_SAIDA.equals(registro.topic())
                        && "sol-001".equals(registro.key())
                        && registro.value().contains(trechoEsperado)) {
                    return registro;
                }
            }
        }
        throw new AssertionError(
                "evento ReservaDeLimiteCancelada nao foi publicado para " + eventoOrigemId);
    }

    private BigDecimal limiteDisponivel() {
        return bancoDeDados.queryForObject(
                "select limite_disponivel from limite_credito where cliente_id = 'cli-001'",
                BigDecimal.class);
    }

    private String motivoDoCancelamento(String solicitacaoId) {
        return bancoDeDados.queryForObject(
                "select motivo from cancelamento_reserva where solicitacao_id = ?",
                String.class, solicitacaoId);
    }

    private Integer quantidadeCancelamentos(String eventoOrigemId) {
        return bancoDeDados.queryForObject(
                "select count(*) from cancelamento_reserva where evento_origem_id = ?",
                Integer.class, eventoOrigemId);
    }

    private String eventoCancelamentoId(String eventoOrigemId) {
        return bancoDeDados.queryForObject(
                "select evento_cancelamento_id from cancelamento_reserva where evento_origem_id = ?",
                String.class, eventoOrigemId);
    }
}

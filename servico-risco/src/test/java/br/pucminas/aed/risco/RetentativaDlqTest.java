package br.pucminas.aed.risco;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import br.pucminas.aed.risco.service.AnaliseCreditoRepository;
import br.pucminas.aed.risco.service.AnaliseCreditoService;
import br.pucminas.aed.risco.service.EventoProcessadoRepository;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {
        "credito.solicitacao.solicitada.v1",
        "credito.solicitacao.solicitada.v1.dlq"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:risco-dlq;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.retentativa.tentativas=2",
        "app.kafka.retentativa.intervalo-inicial-ms=50",
        "app.kafka.retentativa.intervalo-maximo-ms=100"
})
class RetentativaDlqTest {
    private static final String TOPICO = "credito.solicitacao.solicitada.v1";
    private static final String TOPICO_DLQ = "credito.solicitacao.solicitada.v1.dlq";
    private static final int TENTATIVAS = 2;
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private EventoProcessadoRepository eventoProcessadoRepository;
    @Autowired private AnaliseCreditoRepository analiseCreditoRepository;
    @Value("${spring.embedded.kafka.brokers}") private String servidores;

    @SpyBean private AnaliseCreditoService analiseCreditoService;

    private KafkaProducer<String, String> publicador;
    private KafkaConsumer<String, String> consumidorDaDlq;

    @BeforeEach
    void preparar() {
        analiseCreditoRepository.excluirTodos();
        eventoProcessadoRepository.excluirTodos();

        Properties doPublicador = new Properties();
        doPublicador.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        doPublicador.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        doPublicador.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        doPublicador.put(ProducerConfig.ACKS_CONFIG, "all");
        publicador = new KafkaProducer<>(doPublicador);

        Properties doConsumidor = new Properties();
        doConsumidor.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        doConsumidor.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-dlq-" + System.nanoTime());
        doConsumidor.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        doConsumidor.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumidorDaDlq = new KafkaConsumer<>(doConsumidor);

        // Atribuicao explicita no fim da fila: cada teste enxerga apenas o que ele proprio
        // produziu na DLQ, sem herdar os registros dos testes anteriores da classe.
        var particoes = consumidorDaDlq.partitionsFor(TOPICO_DLQ).stream()
                .map(informacao -> new TopicPartition(TOPICO_DLQ, informacao.partition()))
                .toList();
        consumidorDaDlq.assign(particoes);
        consumidorDaDlq.seekToEnd(particoes);
        particoes.forEach(consumidorDaDlq::position);
    }

    @AfterEach
    void encerrar() {
        publicador.close();
        consumidorDaDlq.close();
    }

    @Test
    void falhaTransitoriaQueSeRecuperaNaoVaiParaADlq() {
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .doCallRealMethod()
                .when(analiseCreditoService).processar(any(), any());

        publicar("evt-transitorio-recupera", "sol-101", eventoJson("sol-101"));
        publicador.flush();

        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            verify(analiseCreditoService, times(2)).processar(any(), any());
            assertThat(analiseCreditoRepository.contar()).isEqualTo(1);
        });
        assertThat(lerDaDlq(Duration.ofSeconds(3))).isNull();
    }

    @Test
    void falhaTransitoriaEsgotaAsTentativasEVaiParaADlq() {
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .when(analiseCreditoService).processar(any(), any());

        publicar("evt-transitorio-esgota", "sol-102", eventoJson("sol-102"));
        publicador.flush();

        ConsumerRecord<String, String> naDlq = lerDaDlq(PRAZO);
        assertThat(naDlq).isNotNull();
        assertThat(naDlq.key()).isEqualTo("sol-102");
        assertThat(cabecalho(naDlq, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPICO);
        assertThat(cabecalho(naDlq, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(DataAccessResourceFailureException.class.getName());
        assertThat(cabecalho(naDlq, "ce_id")).isEqualTo("evt-transitorio-esgota");

        // Falha de listener: o evento ja desserializado e reserializado pelo publicador da DLQ.
        // A data sai em ISO-8601, e nao como epoch. O offset aparece como UTC porque o
        // JsonDeserializer do consumidor normaliza OffsetDateTime para o fuso do contexto --
        // o instante e o mesmo, e o offset original de Brasilia continua intacto no ce_time.
        assertThat(naDlq.value())
                .contains("\"solicitacaoId\":\"sol-102\"")
                .contains("\"dataSolicitacao\":\"2026-08-15T23:30:00Z\"");
        assertThat(cabecalho(naDlq, "ce_time")).isEqualTo("2026-08-15T20:30:00-03:00");

        verify(analiseCreditoService, times(TENTATIVAS + 1)).processar(any(), any());
        assertThat(analiseCreditoRepository.contar()).isZero();
        confirmarOffsetDoRegistroRecuperado(naDlq);
    }

    @Test
    void payloadMalformadoVaiDiretoParaADlqComOConteudoOriginal() {
        String payloadInvalido = "{ isso nao e json valido";
        publicar("evt-payload-invalido", "sol-103", payloadInvalido);
        publicador.flush();

        // Os dois grupos de consumo do servico-risco leem o mesmo topico, entao os dois
        // rejeitam o payload e cada um gera o seu proprio registro na DLQ. Sao distinguiveis
        // pelo cabecalho de grupo de origem.
        var naDlq = lerTodosDaDlq(Duration.ofSeconds(8));
        assertThat(naDlq).hasSize(2);
        assertThat(naDlq).allSatisfy(registro -> {
            assertThat(registro.value()).isEqualTo(payloadInvalido);
            assertThat(cabecalho(registro, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPICO);
        });
        assertThat(naDlq)
                .extracting(registro -> cabecalho(registro, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP))
                .containsExactlyInAnyOrder("risco-credito-v1", "risco-fluxo-creditos-v1");

        verify(analiseCreditoService, never()).processar(any(), any());
        assertThat(analiseCreditoRepository.contar()).isZero();
    }

    @Test
    void cabecalhoObrigatorioAusenteVaiDiretoParaADlqSemRetentar() {
        var registro = new ProducerRecord<String, String>(TOPICO, "sol-104", eventoJson("sol-104"));
        registro.headers().add("ce_type", TOPICO.getBytes(UTF_8));
        publicador.send(registro);
        publicador.flush();

        ConsumerRecord<String, String> naDlq = lerDaDlq(PRAZO);
        assertThat(naDlq).isNotNull();
        assertThat(cabecalho(naDlq, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(IllegalArgumentException.class.getName());

        verify(analiseCreditoService, never()).processar(any(), any());
        assertThat(analiseCreditoRepository.contar()).isZero();
    }

    private ConsumerRecord<String, String> lerDaDlq(Duration prazo) {
        long limite = System.currentTimeMillis() + prazo.toMillis();
        while (System.currentTimeMillis() < limite) {
            for (ConsumerRecord<String, String> registro : consumidorDaDlq.poll(Duration.ofMillis(500))) {
                return registro;
            }
        }
        return null;
    }

    /** Coleta durante toda a janela, em vez de parar no primeiro registro. */
    private List<ConsumerRecord<String, String>> lerTodosDaDlq(Duration janela) {
        var coletados = new ArrayList<ConsumerRecord<String, String>>();
        long limite = System.currentTimeMillis() + janela.toMillis();
        while (System.currentTimeMillis() < limite) {
            consumidorDaDlq.poll(Duration.ofMillis(500)).forEach(coletados::add);
        }
        return coletados;
    }

    private String cabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : new String(cabecalho.value(), UTF_8);
    }

    /**
     * O registro recuperado precisa ter o offset confirmado, senao ele volta na proxima
     * atribuicao de particao e duplica na DLQ. Como o container usa ack manual e o listener
     * lancou excecao, quem confirma e o {@code setCommitRecovered(true)} do tratador de falha.
     */
    private void confirmarOffsetDoRegistroRecuperado(ConsumerRecord<String, String> naDlq) {
        var particao = new TopicPartition(
                TOPICO, ByteBuffer.wrap(naDlq.headers()
                        .lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value()).getInt());
        long offsetOriginal = ByteBuffer.wrap(naDlq.headers()
                .lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong();

        Properties propriedades = new Properties();
        propriedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedades.put(ConsumerConfig.GROUP_ID_CONFIG, "risco-credito-v1");
        propriedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propriedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // committed() e uma consulta direta ao broker: nao entra no grupo nem provoca rebalanceamento.
        try (var leitor = new KafkaConsumer<String, String>(propriedades)) {
            Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
                var confirmado = leitor.committed(Set.of(particao)).get(particao);
                assertThat(confirmado).isNotNull();
                assertThat(confirmado.offset()).isGreaterThan(offsetOriginal);
            });
        }
    }

    private void publicar(String eventoId, String solicitacaoId, String valor) {
        var registro = new ProducerRecord<String, String>(TOPICO, solicitacaoId, valor);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/credito/solicitacoes".getBytes(UTF_8));
        registro.headers().add("ce_type", TOPICO.getBytes(UTF_8));
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
                  "canalOrigem": "APP"
                }
                """.formatted(solicitacaoId);
    }
}

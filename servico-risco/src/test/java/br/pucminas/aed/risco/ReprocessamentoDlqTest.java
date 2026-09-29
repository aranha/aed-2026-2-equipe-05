package br.pucminas.aed.risco;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import br.pucminas.aed.risco.service.AnaliseCreditoRepository;
import br.pucminas.aed.risco.service.AnaliseCreditoService;
import br.pucminas.aed.risco.service.EventoProcessadoRepository;
import br.pucminas.aed.risco.service.FluxoCreditoSolicitadoService;
import br.pucminas.aed.risco.service.ReprocessamentoDlqService;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
        "credito.solicitacao.solicitada.v1.dlq",
        "credito.limite.reservado.v1",
        "credito.limite.reservado.v1.dlq"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:risco-reprocessamento;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.retentativa.tentativas=2",
        "app.kafka.retentativa.intervalo-inicial-ms=50",
        "app.kafka.retentativa.intervalo-maximo-ms=100"
})
class ReprocessamentoDlqTest {
    private static final String TOPICO = "credito.solicitacao.solicitada.v1";
    private static final String TOPICO_DLQ = "credito.solicitacao.solicitada.v1.dlq";
    private static final String TOPICO_RESERVADO = "credito.limite.reservado.v1";
    private static final String TOPICO_RESERVADO_DLQ = "credito.limite.reservado.v1.dlq";
    private static final String TOPICO_CANCELADA_DLQ = "credito.reserva-limite.cancelada.v1.dlq";
    private static final String GRUPO_REPROCESSAMENTO = "risco-reprocessamento-dlq-v1";
    private static final int EXECUCOES_ATE_A_DLQ = 3;
    private static final Duration PRAZO = Duration.ofSeconds(20);

    @Autowired private EventoProcessadoRepository eventoProcessadoRepository;
    @Autowired private AnaliseCreditoRepository analiseCreditoRepository;
    @Autowired private ReprocessamentoDlqService reprocessamento;
    @Value("${spring.embedded.kafka.brokers}") private String servidores;

    @SpyBean private AnaliseCreditoService analiseCreditoService;
    @SpyBean private FluxoCreditoSolicitadoService fluxoCreditoSolicitadoService;

    private KafkaProducer<String, String> publicador;
    private KafkaConsumer<String, String> consumidorDaDlq;
    private KafkaConsumer<String, String> consumidorDoTopico;
    private KafkaConsumer<String, String> consumidorDaDlqDoReservado;

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

        // Cada teste enxerga e reprocessa apenas o que ele proprio produziu: os leitores
        // comecam no fim dos topicos e o grupo de reprocessamento, no fim de cada DLQ.
        consumidorDaDlq = leitorNoFim(TOPICO_DLQ);
        consumidorDoTopico = leitorNoFim(TOPICO);
        consumidorDaDlqDoReservado = leitorNoFim(TOPICO_RESERVADO_DLQ);
        try (var grupo = consumidor(GRUPO_REPROCESSAMENTO)) {
            var fim = new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
            for (String dlq : List.of(TOPICO_DLQ, TOPICO_RESERVADO_DLQ, TOPICO_CANCELADA_DLQ)) {
                grupo.endOffsets(particoes(grupo, dlq)).forEach((particao, offset) ->
                        fim.put(particao, new OffsetAndMetadata(offset)));
            }
            grupo.commitSync(fim);
        }
    }

    @AfterEach
    void encerrar() {
        publicador.close();
        consumidorDaDlq.close();
        consumidorDoTopico.close();
        consumidorDaDlqDoReservado.close();
    }

    @Test
    void reprocessamentoRepublicaNoTopicoOriginalComMesmoCeIdEMesmaChave() {
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .when(analiseCreditoService).processar(any(), any());
        publicar("evt-reprocessa-001", "sol-201", eventoJson("sol-201"));
        publicador.flush();
        ConsumerRecord<String, String> original = lerDe(consumidorDoTopico);
        assertThat(lerDe(consumidorDaDlq)).isNotNull();
        assertThat(analiseCreditoRepository.contar()).isZero();

        doCallRealMethod().when(analiseCreditoService).processar(any(), any());
        assertThat(reprocessamento.reprocessar()).isEqualTo(1);

        ConsumerRecord<String, String> republicado = lerDe(consumidorDoTopico);
        assertThat(republicado).isNotNull();
        assertThat(republicado.key()).isEqualTo("sol-201");
        assertThat(republicado.partition()).isEqualTo(original.partition());
        // Os mesmos cabecalhos ce_* do original, e nenhum kafka_dlt-* ou classificacao da
        // passagem pela DLQ.
        assertThat(cabecalhos(republicado)).isEqualTo(cabecalhos(original));
        // O campo que o servico-risco nao declara e o offset de Brasilia sobrevivem a
        // reserializacao da DLQ.
        assertThat(republicado.value())
                .contains("\"canalOrigem\":\"APP\"")
                .contains("\"dataSolicitacao\":\"2026-08-15T20:30:00-03:00\"");

        Awaitility.await().atMost(PRAZO).untilAsserted(() ->
                assertThat(analiseCreditoRepository.contar()).isEqualTo(1));
    }

    @Test
    void mesmoRegistroReprocessadoDuasVezesProduzUmUnicoEfeito() {
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .when(analiseCreditoService).processar(any(), any());
        publicar("evt-reprocessa-002", "sol-202", eventoJson("sol-202"));
        publicador.flush();
        ConsumerRecord<String, String> naDlq = lerDe(consumidorDaDlq);
        assertThat(naDlq).isNotNull();

        doCallRealMethod().when(analiseCreditoService).processar(any(), any());
        assertThat(reprocessamento.reprocessar()).isEqualTo(1);
        // Como um operador que roda o reprocessamento de novo depois de voltar o grupo.
        voltarGrupoDeReprocessamentoPara(naDlq);
        assertThat(reprocessamento.reprocessar()).isEqualTo(1);

        // O evento foi entregue e executado de novo nas duas vezes, e o dedup por ce_id
        // descartou a segunda.
        Awaitility.await().atMost(PRAZO).untilAsserted(() ->
                verify(analiseCreditoService, times(EXECUCOES_ATE_A_DLQ + 2)).processar(any(), any()));
        Awaitility.await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(eventoProcessadoRepository.contar()).isEqualTo(1);
            assertThat(analiseCreditoRepository.contar()).isEqualTo(1);
        });
    }

    @Test
    void registroQueVoltaAFalharRetornaADlqSemLoop() {
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .when(analiseCreditoService).processar(any(), any());
        publicar("evt-reprocessa-003", "sol-203", eventoJson("sol-203"));
        publicador.flush();
        lerDe(consumidorDoTopico);
        assertThat(lerDe(consumidorDaDlq)).isNotNull();

        assertThat(reprocessamento.reprocessar()).isEqualTo(1);

        ConsumerRecord<String, String> republicado = lerDe(consumidorDoTopico);
        ConsumerRecord<String, String> deVoltaNaDlq = lerDe(consumidorDaDlq);
        assertThat(deVoltaNaDlq).isNotNull();
        assertThat(deVoltaNaDlq.key()).isEqualTo("sol-203");
        assertThat(cabecalho(deVoltaNaDlq, "ce_id")).isEqualTo("evt-reprocessa-003");
        // Um unico conjunto de cabecalhos de origem, apontando para o registro republicado.
        assertThat(deVoltaNaDlq.headers().headers(KafkaHeaders.DLT_ORIGINAL_OFFSET)).hasSize(1);
        assertThat(ByteBuffer.wrap(deVoltaNaDlq.headers()
                .lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong())
                .isEqualTo(republicado.offset());
        assertThat(cabecalho(deVoltaNaDlq, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPICO);

        // Passou de novo pela retentativa completa, e nada o traz de volta sozinho.
        assertThat(lerDe(consumidorDaDlq, Duration.ofSeconds(3))).isNull();
        verify(analiseCreditoService, times(2 * EXECUCOES_ATE_A_DLQ)).processar(any(), any());
        assertThat(analiseCreditoRepository.contar()).isZero();
    }

    @Test
    void falhaComumAosDoisGruposERepublicadaUmaUnicaVez() {
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .when(analiseCreditoService).processar(any(), any());
        doThrow(new DataAccessResourceFailureException("banco indisponivel"))
                .when(fluxoCreditoSolicitadoService).agregar(any(), any());
        publicar("evt-reprocessa-004", "sol-204", eventoJson("sol-204"));
        publicador.flush();
        lerDe(consumidorDoTopico);
        assertThat(lerTodosDe(consumidorDaDlq, Duration.ofSeconds(8))).hasSize(2);

        doCallRealMethod().when(analiseCreditoService).processar(any(), any());
        doCallRealMethod().when(fluxoCreditoSolicitadoService).agregar(any(), any());
        assertThat(reprocessamento.reprocessar()).isEqualTo(1);

        // Uma unica republicacao, lida pelos dois grupos.
        assertThat(lerTodosDe(consumidorDoTopico, Duration.ofSeconds(3))).hasSize(1);
        Awaitility.await().atMost(PRAZO).untilAsserted(() -> {
            assertThat(analiseCreditoRepository.contar()).isEqualTo(1);
            verify(fluxoCreditoSolicitadoService, times(EXECUCOES_ATE_A_DLQ + 1)).agregar(any(), any());
        });
    }

    @Test
    void falhaPermanenteFicaNaDlqSemSerRepublicada() {
        publicar("evt-reprocessa-005", "sol-205", "{ isso nao e json valido");
        publicador.flush();
        lerDe(consumidorDoTopico);
        assertThat(lerTodosDe(consumidorDaDlq, Duration.ofSeconds(8))).hasSize(2).allSatisfy(registro ->
                assertThat(cabecalho(registro, "classificacao")).isEqualTo("PERMANENTE"));

        assertThat(reprocessamento.reprocessar()).isZero();

        assertThat(lerDe(consumidorDoTopico, Duration.ofSeconds(3))).isNull();
    }

    @Test
    void desfechoDaSagaQueFoiParaADlqEAplicadoPeloReprocessamento() {
        // LimiteDeCreditoReservado antes do CreditoSolicitado: a analise ainda nao existe, as
        // tentativas se esgotam e o evento vai para a DLQ da saga.
        var reserva = new ProducerRecord<String, String>(TOPICO_RESERVADO, "sol-206", """
                {"eventoId":"evt-reserva-206","solicitacaoId":"sol-206","clienteId":"cli-ficticio-001",
                 "valorReservado":15000.00,"limiteDisponivel":5000.00,"dataReserva":"2026-08-15T20:31:00-03:00"}
                """);
        adicionarCabecalhosCe(reserva, "evt-reserva-206", "/credito/limites");
        publicador.send(reserva);
        publicador.flush();
        ConsumerRecord<String, String> naDlq = lerDe(consumidorDaDlqDoReservado);
        assertThat(naDlq).isNotNull();
        assertThat(cabecalho(naDlq, "classificacao")).isEqualTo("TRANSITORIA");

        publicar("evt-reprocessa-006", "sol-206", eventoJson("sol-206"));
        publicador.flush();
        Awaitility.await().atMost(PRAZO).untilAsserted(() ->
                assertThat(analiseCreditoRepository.buscarStatus("sol-206")).contains("PENDENTE"));

        assertThat(reprocessamento.reprocessar()).isEqualTo(1);

        Awaitility.await().atMost(PRAZO).untilAsserted(() ->
                assertThat(analiseCreditoRepository.buscarStatus("sol-206")).contains("RESERVADA"));
    }

    private KafkaConsumer<String, String> consumidor(String grupo) {
        Properties propriedades = new Properties();
        propriedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        propriedades.put(ConsumerConfig.GROUP_ID_CONFIG, grupo);
        propriedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propriedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(propriedades);
    }

    private KafkaConsumer<String, String> leitorNoFim(String topico) {
        var leitor = consumidor("teste-reprocessamento-" + System.nanoTime());
        var particoes = particoes(leitor, topico);
        leitor.assign(particoes);
        leitor.seekToEnd(particoes);
        particoes.forEach(leitor::position);
        return leitor;
    }

    private List<TopicPartition> particoes(KafkaConsumer<String, String> consumidor, String topico) {
        return consumidor.partitionsFor(topico).stream()
                .map(informacao -> new TopicPartition(topico, informacao.partition()))
                .toList();
    }

    private void voltarGrupoDeReprocessamentoPara(ConsumerRecord<String, String> naDlq) {
        try (var grupo = consumidor(GRUPO_REPROCESSAMENTO)) {
            grupo.commitSync(Map.of(new TopicPartition(naDlq.topic(), naDlq.partition()),
                    new OffsetAndMetadata(naDlq.offset())));
        }
    }

    private ConsumerRecord<String, String> lerDe(KafkaConsumer<String, String> leitor) {
        return lerDe(leitor, PRAZO);
    }

    private ConsumerRecord<String, String> lerDe(KafkaConsumer<String, String> leitor, Duration prazo) {
        long limite = System.currentTimeMillis() + prazo.toMillis();
        while (System.currentTimeMillis() < limite) {
            for (ConsumerRecord<String, String> registro : leitor.poll(Duration.ofMillis(500))) {
                return registro;
            }
        }
        return null;
    }

    /** Coleta durante toda a janela, em vez de parar no primeiro registro. */
    private List<ConsumerRecord<String, String>> lerTodosDe(KafkaConsumer<String, String> leitor,
                                                            Duration janela) {
        var coletados = new ArrayList<ConsumerRecord<String, String>>();
        long limite = System.currentTimeMillis() + janela.toMillis();
        while (System.currentTimeMillis() < limite) {
            leitor.poll(Duration.ofMillis(500)).forEach(coletados::add);
        }
        return coletados;
    }

    private Map<String, String> cabecalhos(ConsumerRecord<String, String> registro) {
        var cabecalhos = new LinkedHashMap<String, String>();
        registro.headers().forEach(cabecalho ->
                cabecalhos.put(cabecalho.key(), new String(cabecalho.value(), UTF_8)));
        return cabecalhos;
    }

    private String cabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : new String(cabecalho.value(), UTF_8);
    }

    private void publicar(String eventoId, String solicitacaoId, String valor) {
        var registro = new ProducerRecord<String, String>(TOPICO, solicitacaoId, valor);
        adicionarCabecalhosCe(registro, eventoId, "/credito/solicitacoes");
        publicador.send(registro);
    }

    private void adicionarCabecalhosCe(ProducerRecord<String, String> registro, String eventoId, String origem) {
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", origem.getBytes(UTF_8));
        registro.headers().add("ce_type", registro.topic().getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-08-15T20:30:00-03:00".getBytes(UTF_8));
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

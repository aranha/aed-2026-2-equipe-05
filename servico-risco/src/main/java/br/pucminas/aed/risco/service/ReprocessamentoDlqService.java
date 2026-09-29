package br.pucminas.aed.risco.service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

/**
 * Reprocessamento manual da DLQ de credito solicitado.
 *
 * <p>Republica no topico original cada registro que estava na DLQ quando a execucao comecou,
 * com a mesma chave e os mesmos cabecalhos {@code ce_*}. A idempotencia vem do dedup por
 * {@code ce_id} dos consumidores. Um registro que falhar de novo passa pela retentativa normal
 * e volta para a DLQ depois do fim lido aqui, entao fica para a proxima execucao: nao ha loop.
 */
@Service
public class ReprocessamentoDlqService {
    private static final Logger log = LoggerFactory.getLogger(ReprocessamentoDlqService.class);
    private static final String PREFIXO_CABECALHO_DLT = KafkaHeaders.PREFIX + "dlt-";
    private static final Duration ESPERA_DA_LEITURA = Duration.ofSeconds(1);

    private final ConsumerFactory<?, ?> fabricaDeConsumidores;
    private final KafkaTemplate<String, Object> clienteDaDlq;
    private final String topico;
    private final String topicoDlq;
    private final String grupo;

    public ReprocessamentoDlqService(
            ConsumerFactory<?, ?> fabricaDeConsumidores,
            KafkaTemplate<String, Object> clienteDaDlq,
            @Value("${app.kafka.topico.credito-solicitado}") String topico,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixoDlq,
            @Value("${app.kafka.grupo.reprocessamento-dlq}") String grupo) {
        this.fabricaDeConsumidores = fabricaDeConsumidores;
        this.clienteDaDlq = clienteDaDlq;
        this.topico = topico;
        this.topicoDlq = topico + sufixoDlq;
        this.grupo = grupo;
    }

    /**
     * Le a DLQ a partir do offset confirmado pelo grupo de reprocessamento ate o fim registrado
     * no inicio e confirma o offset so depois que o broker aceitou a republicacao.
     *
     * @return quantidade de registros republicados.
     */
    public int reprocessar() {
        int republicados = 0;
        int ignorados = 0;
        try (Consumer<String, byte[]> consumidor = criarConsumidor()) {
            List<TopicPartition> particoes = consumidor.partitionsFor(topicoDlq).stream()
                    .map(informacao -> new TopicPartition(topicoDlq, informacao.partition()))
                    .toList();
            Map<TopicPartition, Long> fim = consumidor.endOffsets(particoes);
            // Os dois grupos do servico-risco escrevem na mesma DLQ: uma falha comum aos dois
            // gera um registro por grupo. Basta republicar a origem uma vez, os dois grupos a
            // leem de novo.
            Set<String> origensRepublicadas = new HashSet<>();

            for (TopicPartition particao : particoes) {
                long limite = fim.get(particao);
                consumidor.assign(List.of(particao));
                while (consumidor.position(particao) < limite) {
                    var envios = new ArrayList<CompletableFuture<?>>();
                    for (ConsumerRecord<String, byte[]> registro : consumidor.poll(ESPERA_DA_LEITURA)) {
                        if (registro.offset() >= limite) {
                            break;
                        }
                        String origem = origem(registro);
                        String eventoId = cabecalho(registro, "ce_id");
                        if (!origensRepublicadas.add(origem)) {
                            ignorados++;
                            log.info("Registro da DLQ ignorado, origem ja republicada | ce_id={} | origem={}",
                                    eventoId, origem);
                            continue;
                        }
                        republicados++;
                        envios.add(clienteDaDlq.send(republicacao(registro)).thenAccept(resultado ->
                                log.info("Registro da DLQ republicado | ce_id={} | origem={} | destino={}@{}",
                                        eventoId, origem,
                                        new TopicPartition(topico, resultado.getRecordMetadata().partition()),
                                        resultado.getRecordMetadata().offset())));
                    }
                    CompletableFuture.allOf(envios.toArray(CompletableFuture[]::new)).join();
                    long confirmado = Math.min(consumidor.position(particao), limite);
                    consumidor.commitSync(Map.of(particao, new OffsetAndMetadata(confirmado)));
                }
            }
        }
        log.info("Reprocessamento da DLQ concluido | topico={} | republicados={} | ignorados={}",
                topicoDlq, republicados, ignorados);
        return republicados;
    }

    @SuppressWarnings("unchecked")
    private Consumer<String, byte[]> criarConsumidor() {
        var sobrescritas = new Properties();
        sobrescritas.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        sobrescritas.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return (Consumer<String, byte[]>) fabricaDeConsumidores.createConsumer(grupo, null, null, sobrescritas);
    }

    /**
     * Sem particao explicita: a chave decide, como na publicacao original, e o registro cai na
     * mesma particao dos demais eventos da solicitacao. Os cabecalhos {@code kafka_dlt-*}
     * descrevem a falha anterior e nao fazem parte do contrato do topico. Se ficassem, uma nova
     * falha levaria para a DLQ dois conjuntos de {@code kafka_dlt-original-*}, e o primeiro
     * apontaria para o offset antigo.
     */
    private ProducerRecord<String, Object> republicacao(ConsumerRecord<String, byte[]> registro) {
        var cabecalhos = new RecordHeaders();
        for (Header cabecalho : registro.headers()) {
            if (!cabecalho.key().startsWith(PREFIXO_CABECALHO_DLT)) {
                cabecalhos.add(cabecalho);
            }
        }
        return new ProducerRecord<>(topico, null, registro.key(), registro.value(), cabecalhos);
    }

    /** Posicao do registro no topico original; sem os cabecalhos da DLQ, a posicao na propria DLQ. */
    private String origem(ConsumerRecord<String, byte[]> registro) {
        Header particao = registro.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Header offset = registro.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);
        if (particao == null || offset == null) {
            return new TopicPartition(registro.topic(), registro.partition()) + "@" + registro.offset();
        }
        return new TopicPartition(topico, ByteBuffer.wrap(particao.value()).getInt())
                + "@" + ByteBuffer.wrap(offset.value()).getLong();
    }

    private String cabecalho(ConsumerRecord<String, byte[]> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        return cabecalho == null ? null : new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}

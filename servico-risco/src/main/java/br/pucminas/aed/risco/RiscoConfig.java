package br.pucminas.aed.risco;

import br.pucminas.aed.risco.service.CabecalhosDeFalhaFunction;
import br.pucminas.aed.risco.service.ReprocessamentoDlqService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Politica de falha dos consumidores do servico-risco.
 *
 * <p>Falha transitoria (banco indisponivel, timeout de conexao) e retentada com espera
 * exponencial e limite de tentativas. Falha permanente (payload malformado, cabecalho
 * obrigatorio ausente, violacao de integridade) vai direto para a DLQ, sem gastar
 * tentativa: reprocessar o mesmo registro produziria exatamente o mesmo erro.
 */
@Configuration
public class RiscoConfig {

    @Bean
    public NewTopic topicoDeCreditoSolicitadoDlq(
            @Value("${app.kafka.topico.credito-solicitado}") String topico,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixo) {
        return new NewTopic(topico + sufixo, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDeLimiteReservadoDlq(
            @Value("${app.kafka.topico.limite-reservado}") String topico,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixo) {
        return new NewTopic(topico + sufixo, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDeReservaCanceladaDlq(
            @Value("${app.kafka.topico.reserva-cancelada}") String topico,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixo) {
        return new NewTopic(topico + sufixo, 3, (short) 1);
    }

    @Bean
    public ObjectMapper objectMapperDosEventos() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
    }

    /**
     * Publicador da DLQ. O valor do registro pode chegar de duas formas: como {@code byte[]},
     * quando a desserializacao falhou e o payload original foi preservado pelo
     * {@code ErrorHandlingDeserializer}, ou como o evento ja desserializado, quando quem
     * falhou foi o listener.
     */
    @Bean
    public KafkaTemplate<String, Object> clienteDaDlq(KafkaProperties propriedades,
                                                      ObjectMapper objectMapperDosEventos) {
        var serializadorJson = new JsonSerializer<>(objectMapperDosEventos);
        serializadorJson.setAddTypeInfo(false);

        Map<Class<?>, Serializer<?>> delegados = new LinkedHashMap<>();
        delegados.put(byte[].class, new ByteArraySerializer());
        delegados.put(String.class, new StringSerializer());
        delegados.put(Object.class, serializadorJson);

        Map<String, Object> configuracao = propriedades.buildProducerProperties(null);
        configuracao.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        configuracao.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        ProducerFactory<String, Object> fabrica = new DefaultKafkaProducerFactory<>(
                configuracao, new StringSerializer(),
                new DelegatingByTypeSerializer(delegados, true));
        return new KafkaTemplate<>(fabrica);
    }

    /**
     * Um unico publicador para todas as DLQs do servico. O destino e {@code <topico>.dlq}; se o
     * registro ja veio de uma DLQ (falha durante o reprocessamento), ele volta para a mesma DLQ
     * com o contador {@code reprocessamentos} incrementado, e nao para {@code .dlq.dlq}.
     */
    @Bean
    public DeadLetterPublishingRecoverer publicadorDaDlq(
            KafkaTemplate<String, Object> clienteDaDlq,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixoDlq) {
        var recuperador = new DeadLetterPublishingRecoverer(clienteDaDlq,
                (registro, falha) -> new TopicPartition(
                        registro.topic().endsWith(sufixoDlq) ? registro.topic() : registro.topic() + sufixoDlq,
                        -1));
        recuperador.setHeadersFunction(new CabecalhosDeFalhaFunction(sufixoDlq));
        recuperador.setAppendOriginalHeaders(false);
        // Sem isso o envio para a DLQ so aparece em DEBUG: a mensagem sumiria do fluxo sem
        // deixar rastro no log da aplicacao.
        recuperador.setLogRecoveryRecord(true);
        return recuperador;
    }

    @Bean
    public DefaultErrorHandler tratadorDeFalhaDoConsumidor(
            DeadLetterPublishingRecoverer publicadorDaDlq,
            @Value("${app.kafka.retentativa.tentativas}") int tentativas,
            @Value("${app.kafka.retentativa.intervalo-inicial-ms}") long intervaloInicialMs,
            @Value("${app.kafka.retentativa.multiplicador}") double multiplicador,
            @Value("${app.kafka.retentativa.intervalo-maximo-ms}") long intervaloMaximoMs) {

        var espera = new ExponentialBackOffWithMaxRetries(tentativas);
        espera.setInitialInterval(intervaloInicialMs);
        espera.setMultiplier(multiplicador);
        espera.setMaxInterval(intervaloMaximoMs);

        var tratador = new DefaultErrorHandler(publicadorDaDlq, espera);
        registrarFalhasPermanentes(tratador);
        tratador.setCommitRecovered(true);
        return tratador;
    }

    /** Consumidores da saga: o valor chega como texto e o listener escolhe a classe pelo topico. */
    @Bean(name = "clienteDaSagaKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> clienteDaSagaKafkaListenerContainerFactory(
            KafkaProperties propriedades, DefaultErrorHandler tratadorDeFalhaDoConsumidor) {
        var container = fabricaDeTexto(propriedades);
        container.setCommonErrorHandler(tratadorDeFalhaDoConsumidor);
        return container;
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> fabricaDeTexto(KafkaProperties propriedades) {
        Map<String, Object> configuracao = propriedades.buildConsumerProperties(null);
        configuracao.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuracao.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuracao.keySet().removeIf(chave -> chave.startsWith("spring.json.") || chave.startsWith("spring.deserializer."));

        ConsumerFactory<String, String> fabrica = new DefaultKafkaConsumerFactory<>(
                configuracao, new StringDeserializer(), new StringDeserializer());
        var container = new ConcurrentKafkaListenerContainerFactory<String, String>();
        container.setConsumerFactory(fabrica);
        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return container;
    }

    @SuppressWarnings("unchecked")
    private void registrarFalhasPermanentes(DefaultErrorHandler tratador) {
        tratador.addNotRetryableExceptions(
                CabecalhosDeFalhaFunction.FALHAS_PERMANENTES.toArray(new Class[0]));
    }

    /** Reprocessa a DLQ uma vez, na inicializacao, somente quando pedido explicitamente. */
    @Bean
    @ConditionalOnProperty("app.kafka.reprocessamento-dlq.habilitado")
    public ApplicationRunner reprocessamentoDaDlqNaInicializacao(ReprocessamentoDlqService reprocessamento) {
        return argumentos -> reprocessamento.reprocessar();
    }
}

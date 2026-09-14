package br.pucminas.aed.risco;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
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
        delegados.put(Object.class, serializadorJson);

        Map<String, Object> configuracao = propriedades.buildProducerProperties(null);
        configuracao.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        configuracao.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        ProducerFactory<String, Object> fabrica = new DefaultKafkaProducerFactory<>(
                configuracao, new StringSerializer(),
                new DelegatingByTypeSerializer(delegados, true));
        return new KafkaTemplate<>(fabrica);
    }

    @Bean
    public DefaultErrorHandler tratadorDeFalhaDoConsumidor(
            KafkaTemplate<String, Object> clienteDaDlq,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixoDlq,
            @Value("${app.kafka.retentativa.tentativas}") int tentativas,
            @Value("${app.kafka.retentativa.intervalo-inicial-ms}") long intervaloInicialMs,
            @Value("${app.kafka.retentativa.multiplicador}") double multiplicador,
            @Value("${app.kafka.retentativa.intervalo-maximo-ms}") long intervaloMaximoMs) {

        var recuperador = new DeadLetterPublishingRecoverer(clienteDaDlq,
                (registro, falha) -> new TopicPartition(registro.topic() + sufixoDlq, -1));
        // Sem isso o envio para a DLQ so aparece em DEBUG: a mensagem sumiria do fluxo sem
        // deixar rastro no log da aplicacao.
        recuperador.setLogRecoveryRecord(true);

        var espera = new ExponentialBackOffWithMaxRetries(tentativas);
        espera.setInitialInterval(intervaloInicialMs);
        espera.setMultiplier(multiplicador);
        espera.setMaxInterval(intervaloMaximoMs);

        var tratador = new DefaultErrorHandler(recuperador, espera);
        tratador.addNotRetryableExceptions(
                IllegalArgumentException.class,
                DataIntegrityViolationException.class);
        tratador.setCommitRecovered(true);
        return tratador;
    }
}

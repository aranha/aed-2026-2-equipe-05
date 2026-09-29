package br.pucminas.aed.credito;

import br.pucminas.aed.credito.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.credito.domain.LimiteDeCreditoReservadoEvent;
import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import br.pucminas.aed.credito.service.CabecalhosDeFalhaFunction;
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

@Configuration
public class CreditoConfig {
    @Bean
    public NewTopic topicoDeCreditoSolicitado(
            @Value("${app.kafka.topico.credito-solicitado}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDeElegibilidadeAprovada(
            @Value("${app.kafka.topico.elegibilidade-aprovada}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDeLimiteReservado(
            @Value("${app.kafka.topico.limite-reservado}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDePropostaRecusada(
            @Value("${app.kafka.topico.proposta-recusada}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDePropostaExpirada(
            @Value("${app.kafka.topico.proposta-expirada}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDeReservaCancelada(
            @Value("${app.kafka.topico.reserva-cancelada}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDeElegibilidadeAprovadaDlq(
            @Value("${app.kafka.topico.elegibilidade-aprovada}") String topico,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixo) {
        return new NewTopic(topico + sufixo, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDePropostaRecusadaDlq(
            @Value("${app.kafka.topico.proposta-recusada}") String topico,
            @Value("${app.kafka.topico.sufixo-dlq}") String sufixo) {
        return new NewTopic(topico + sufixo, 3, (short) 1);
    }

    @Bean
    public NewTopic topicoDePropostaExpiradaDlq(
            @Value("${app.kafka.topico.proposta-expirada}") String topico,
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

    @Bean
    public KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker(
            KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {
        return criarKafkaTemplate(propriedades, objectMapperDosEventos);
    }

    @Bean
    public KafkaTemplate<String, LimiteDeCreditoReservadoEvent> clienteDoBrokerLimiteReservado(
            KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {
        return criarKafkaTemplate(propriedades, objectMapperDosEventos);
    }

    @Bean
    public KafkaTemplate<String, ReservaDeLimiteCanceladaEvent> clienteDoBrokerReservaCancelada(
            KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {
        return criarKafkaTemplate(propriedades, objectMapperDosEventos);
    }

    @Bean(name = "clienteDeCompensacaoKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String>
            clienteDeCompensacaoKafkaListenerContainerFactory(KafkaProperties propriedades,
                                                              DefaultErrorHandler tratadorDeFalhaDoConsumidor) {
        var container = clienteDeCompensacaoSemTratador(propriedades);
        container.setCommonErrorHandler(tratadorDeFalhaDoConsumidor);
        return container;
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> clienteDeCompensacaoSemTratador(
            KafkaProperties propriedades) {
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

    /**
     * Publicador das DLQs da saga. O valor pode ser {@code byte[]} (payload que nao desserializou),
     * texto (listener de compensacao, que le o JSON cru) ou o evento ja desserializado.
     */
    @Bean
    public KafkaTemplate<String, Object> clienteDaDlq(KafkaProperties propriedades,
                                                      ObjectMapper objectMapperDosEventos) {
        // Sem setAddTypeInfo aqui: spring.json.add.type.headers ja vem do application.yml, e o
        // JsonSerializer recusa ser configurado pelos dois caminhos ao mesmo tempo.
        var serializadorJson = new JsonSerializer<>(objectMapperDosEventos);

        Map<Class<?>, Serializer<?>> delegados = new LinkedHashMap<>();
        delegados.put(byte[].class, new ByteArraySerializer());
        delegados.put(String.class, new StringSerializer());
        delegados.put(Object.class, serializadorJson);

        Map<String, Object> configuracao = propriedades.buildProducerProperties(null);
        configuracao.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        configuracao.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        ProducerFactory<String, Object> fabrica = new DefaultKafkaProducerFactory<>(
                configuracao, new StringSerializer(), new DelegatingByTypeSerializer(delegados, true));
        return new KafkaTemplate<>(fabrica);
    }

    /** Destino {@code <topico>.dlq}; falha durante o reprocessamento volta para a mesma DLQ. */
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
        recuperador.setLogRecoveryRecord(true);
        return recuperador;
    }

    /**
     * Mesma politica do servico-risco: retentativa exponencial limitada para falha transitoria
     * (reserva ainda nao processada, banco fora do ar) e DLQ direta para falha permanente
     * (payload invalido, cabecalho ausente, cliente sem limite). Vale para os consumidores de
     * elegibilidade (fabrica padrao do Spring Boot, que usa este bean) e de compensacao.
     */
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

    @SuppressWarnings("unchecked")
    private void registrarFalhasPermanentes(DefaultErrorHandler tratador) {
        tratador.addNotRetryableExceptions(
                CabecalhosDeFalhaFunction.FALHAS_PERMANENTES.toArray(new Class[0]));
    }

    private <T> KafkaTemplate<String, T> criarKafkaTemplate(
            KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {
        var serializadorDoValor = new JsonSerializer<T>(objectMapperDosEventos);
        Map<String, Object> configuracao = propriedades.buildProducerProperties(null);
        configuracao.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        configuracao.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        ProducerFactory<String, T> fabrica = new DefaultKafkaProducerFactory<>(
                configuracao, new StringSerializer(), serializadorDoValor);
        return new KafkaTemplate<>(fabrica);
    }
}

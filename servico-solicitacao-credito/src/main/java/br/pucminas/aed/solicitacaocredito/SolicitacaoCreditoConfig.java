package br.pucminas.aed.solicitacaocredito;

import br.pucminas.aed.solicitacaocredito.domain.CreditoSolicitadoEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class SolicitacaoCreditoConfig {

    private static final int PARTICOES = 3;
    private static final short REPLICAS = 1;

    @Bean
    public NewTopic topicoCreditoSolicitado(@Value("${demo.topico}") String topico) {
        return new NewTopic(topico, PARTICOES, REPLICAS);
    }

    @Bean
    public ObjectMapper objectMapperDosEventos() {
        return JsonMapper.builder().addModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
    }

    @Bean
    public KafkaTemplate<String, CreditoSolicitadoEvent> kafkaTemplate(KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {
        JsonSerializer<CreditoSolicitadoEvent> serializadorDoValor = new JsonSerializer<CreditoSolicitadoEvent>(objectMapperDosEventos);

        Map<String, Object> config = propriedades.buildProducerProperties(null);

        config.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        config.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        ProducerFactory<String, CreditoSolicitadoEvent> fabrica = new DefaultKafkaProducerFactory<String, CreditoSolicitadoEvent>(config, new StringSerializer(), serializadorDoValor);

        return new KafkaTemplate<String, CreditoSolicitadoEvent>(fabrica);
    }

}
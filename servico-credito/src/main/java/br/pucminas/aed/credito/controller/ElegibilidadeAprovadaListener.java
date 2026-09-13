package br.pucminas.aed.credito.controller;

import br.pucminas.aed.credito.domain.ElegibilidadeAprovadaEvent;
import br.pucminas.aed.credito.service.LimiteReservadoPublicacaoService;
import br.pucminas.aed.credito.service.ReservaLimiteService;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ElegibilidadeAprovadaListener {
    private final ReservaLimiteService reservaLimiteService;
    private final LimiteReservadoPublicacaoService limiteReservadoPublicacaoService;

    @KafkaListener(topics = "${app.kafka.topico.elegibilidade-aprovada}")
    public void receber(ConsumerRecord<String, ElegibilidadeAprovadaEvent> registro,
                        Acknowledgment confirmacao) {
        String eventoId = lerCabecalhoObrigatorio(registro, "ce_id");
        reservaLimiteService.reservar(eventoId, registro.value())
                .ifPresent(limiteReservadoPublicacaoService::publicar);
        confirmacao.acknowledge();
    }

    private String lerCabecalhoObrigatorio(ConsumerRecord<String, ElegibilidadeAprovadaEvent> registro,
                                           String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            throw new IllegalArgumentException("cabecalho " + nome + " e obrigatorio");
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}

package br.pucminas.aed.credito.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import br.pucminas.aed.credito.CreditoConfig;
import br.pucminas.aed.credito.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.credito.domain.SolicitacaoCreditoVO;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class CreditoServiceTest {
    @Test
    void devePublicarComChaveEEnvelopeCloudEventsBinario() {
        KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker = mock(KafkaTemplate.class);
        ResultadoPublicacaoService resultadoPublicacaoService = mock(ResultadoPublicacaoService.class);
        CompletableFuture<SendResult<String, CreditoSolicitadoEvent>> futuro = new CompletableFuture<>();
        when(clienteDoBroker.send(any(ProducerRecord.class))).thenReturn(futuro);
        var service = new CreditoService(clienteDoBroker, resultadoPublicacaoService,
                "credito.solicitacao.solicitada.v1");

        var evento = service.solicitar(new SolicitacaoCreditoVO(
                null, null, "cli-001", new BigDecimal("15000.00"), "APP"));

        ArgumentCaptor<ProducerRecord<String, CreditoSolicitadoEvent>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(clienteDoBroker).send(captor.capture());
        var registro = captor.getValue();
        assertThat(registro.key()).isEqualTo(evento.getSolicitacaoId());
        assertThat(cabecalho(registro, "ce_specversion")).isEqualTo("1.0");
        assertThat(cabecalho(registro, "ce_id")).isEqualTo(evento.getEventoId());
        assertThat(cabecalho(registro, "ce_source")).isEqualTo("/credito/solicitacoes");
        assertThat(cabecalho(registro, "ce_type"))
                .isEqualTo("credito.solicitacao.solicitada.v1");
        assertThat(cabecalho(registro, "ce_time")).isEqualTo(evento.getDataSolicitacao().toString());
    }

    @Test
    void deveRepublicarOMesmoEventoQuandoOsIdentificadoresVemNaEntrada() {
        KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker = mock(KafkaTemplate.class);
        ResultadoPublicacaoService resultadoPublicacaoService = mock(ResultadoPublicacaoService.class);
        CompletableFuture<SendResult<String, CreditoSolicitadoEvent>> futuro = new CompletableFuture<>();
        when(clienteDoBroker.send(any(ProducerRecord.class))).thenReturn(futuro);
        var service = new CreditoService(clienteDoBroker, resultadoPublicacaoService,
                "credito.solicitacao.solicitada.v1");

        var evento = service.solicitar(new SolicitacaoCreditoVO(
                "evt-reentrega-manual-001", "sol-reentrega-manual-001", "cli-001",
                new BigDecimal("15000.00"), "APP"));

        ArgumentCaptor<ProducerRecord<String, CreditoSolicitadoEvent>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(clienteDoBroker).send(captor.capture());
        var registro = captor.getValue();
        assertThat(evento.getEventoId()).isEqualTo("evt-reentrega-manual-001");
        assertThat(evento.getSolicitacaoId()).isEqualTo("sol-reentrega-manual-001");
        assertThat(cabecalho(registro, "ce_id")).isEqualTo("evt-reentrega-manual-001");
        assertThat(registro.key()).isEqualTo("sol-reentrega-manual-001");
    }

    private String cabecalho(ProducerRecord<String, CreditoSolicitadoEvent> registro, String nome) {
        return new String(registro.headers().lastHeader(nome).value(), StandardCharsets.UTF_8);
    }

    @Test
    void deveSerializarDataEmIso8601() throws Exception {
        var evento = new CreditoSolicitadoEvent("evt-001", "sol-001", "cli-001",
                new BigDecimal("15000.00"),
                java.time.OffsetDateTime.parse("2026-08-15T20:30:00-03:00"), "APP");

        String json = new CreditoConfig().objectMapperDosEventos().writeValueAsString(evento);

        assertThat(json).contains("\"dataSolicitacao\":\"2026-08-15T20:30:00-03:00\"");
    }
}

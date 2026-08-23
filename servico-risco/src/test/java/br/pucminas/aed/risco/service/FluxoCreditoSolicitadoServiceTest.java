package br.pucminas.aed.risco.service;

import static org.assertj.core.api.Assertions.assertThat;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class FluxoCreditoSolicitadoServiceTest {

    @Test
    void agrupaMesmoInstanteRepresentadoComOffsetsDiferentes(CapturedOutput saida) {
        var service = new FluxoCreditoSolicitadoService(288);

        service.agregar(evento("evt-001", "100.00", "2026-08-22T12:02:00Z"));
        service.agregar(evento("evt-002", "200.00", "2026-08-22T09:03:00-03:00"));

        assertThat(saida).contains(
                "janela=2026-08-22T09:00-03:00 | quantidade=2 | totalSolicitado=300.00");
    }

    @Test
    void separaEventosNoLimiteDaJanela(CapturedOutput saida) {
        var service = new FluxoCreditoSolicitadoService(288);

        service.agregar(evento("evt-003", "100.00", "2026-08-22T12:04:59-03:00"));
        service.agregar(evento("evt-004", "200.00", "2026-08-22T12:05:00-03:00"));

        assertThat(saida)
                .contains("janela=2026-08-22T12:00-03:00 | quantidade=1")
                .contains("janela=2026-08-22T12:05-03:00 | quantidade=1");
    }

    @Test
    void atualizaJanelaCorrespondenteQuandoEventoChegaAtrasado(CapturedOutput saida) {
        var service = new FluxoCreditoSolicitadoService(288);

        service.agregar(evento("evt-005", "500.00", "2026-08-22T12:07:00-03:00"));
        service.agregar(evento("evt-006", "100.00", "2026-08-22T12:02:00-03:00"));
        service.agregar(evento("evt-007", "200.00", "2026-08-22T12:03:00-03:00"));

        assertThat(saida).contains(
                "janela=2026-08-22T12:00-03:00 | quantidade=2 | totalSolicitado=300.00");
    }

    @Test
    void contabilizaNovamenteQuandoMesmoEventoEReentregue(CapturedOutput saida) {
        var service = new FluxoCreditoSolicitadoService(288);
        var evento = evento("evt-008", "100.00", "2026-08-22T13:02:00-03:00");

        service.agregar(evento);
        service.agregar(evento);

        assertThat(saida).contains(
                "janela=2026-08-22T13:00-03:00 | quantidade=2 | totalSolicitado=200.00");
    }

    @Test
    void descartaJanelasMaisAntigasQuandoAtingeLimite(CapturedOutput saida) {
        var service = new FluxoCreditoSolicitadoService(2);

        service.agregar(evento("evt-009", "100.00", "2026-08-22T14:02:00-03:00"));
        service.agregar(evento("evt-010", "100.00", "2026-08-22T14:07:00-03:00"));
        service.agregar(evento("evt-011", "100.00", "2026-08-22T14:12:00-03:00"));
        service.agregar(evento("evt-012", "100.00", "2026-08-22T14:02:00-03:00"));

        assertThat(saida).doesNotContain(
                "janela=2026-08-22T14:00-03:00 | quantidade=2");
    }

    private CreditoSolicitadoEvent evento(String eventoId, String valor, String dataSolicitacao) {
        return new CreditoSolicitadoEvent(
                eventoId,
                "sol-" + eventoId,
                "cli-ficticio-001",
                new BigDecimal(valor),
                OffsetDateTime.parse(dataSolicitacao));
    }
}

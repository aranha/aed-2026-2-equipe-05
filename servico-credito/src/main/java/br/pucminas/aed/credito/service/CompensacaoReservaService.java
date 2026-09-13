package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CompensacaoReservaService {
    private static final ZoneOffset OFFSET_BRASILIA = ZoneOffset.of("-03:00");

    private final LimiteCreditoRepository limiteCreditoRepository;
    private final ReservaLimiteRepository reservaLimiteRepository;
    private final CancelamentoReservaRepository cancelamentoReservaRepository;

    @Transactional
    public ReservaDeLimiteCanceladaEvent cancelar(String eventoOrigemId,
                                                   String solicitacaoId,
                                                   String motivo) {
        var cancelamentoExistente = cancelamentoReservaRepository
                .buscarPorEventoOrigemId(eventoOrigemId);

        if (cancelamentoExistente.isPresent()) {
            return cancelamentoExistente.get();
        }

        var reserva = reservaLimiteRepository.buscarPorSolicitacaoId(solicitacaoId)
                .orElseThrow(() -> new IllegalStateException(
                        "reserva de limite nao encontrada para a solicitacao " + solicitacaoId));

        OffsetDateTime agora = OffsetDateTime.now(OFFSET_BRASILIA);
        boolean devolvido = limiteCreditoRepository.devolver(
                reserva.clienteId(), reserva.valorReservado(), agora);

        if (!devolvido) {
            throw new IllegalStateException(
                    "nao foi possivel devolver o limite da solicitacao " + solicitacaoId);
        }

        var limiteDisponivel = limiteCreditoRepository
                .consultarLimiteDisponivel(reserva.clienteId())
                .orElseThrow();

        var evento = new ReservaDeLimiteCanceladaEvent(
                UUID.randomUUID().toString(),
                eventoOrigemId,
                solicitacaoId,
                reserva.clienteId(),
                reserva.valorReservado(),
                limiteDisponivel,
                motivo,
                agora);

        cancelamentoReservaRepository.criar(evento);
        return evento;
    }
}

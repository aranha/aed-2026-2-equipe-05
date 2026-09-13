package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class CancelamentoReservaRepository {
    private final JdbcTemplate bancoDeDados;

    public void criar(ReservaDeLimiteCanceladaEvent evento) {
        bancoDeDados.update("""
                insert into cancelamento_reserva
                    (evento_origem_id, evento_cancelamento_id, solicitacao_id, cliente_id,
                     valor_devolvido, limite_disponivel_apos, motivo, cancelada_em)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """, evento.getEventoOrigemId(), evento.getEventoId(), evento.getSolicitacaoId(),
                evento.getClienteId(), evento.getValorDevolvido(), evento.getLimiteDisponivel(),
                evento.getMotivo(), evento.getDataCancelamento());
    }
}

package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class CancelamentoReservaRepository {
    private final JdbcTemplate bancoDeDados;

    public Optional<ReservaDeLimiteCanceladaEvent> buscarPorEventoOrigemId(String eventoOrigemId) {
        List<ReservaDeLimiteCanceladaEvent> resultados = bancoDeDados.query("""
                select evento_origem_id, evento_cancelamento_id, solicitacao_id, cliente_id,
                       valor_devolvido, limite_disponivel_apos, motivo, cancelada_em
                  from cancelamento_reserva
                 where evento_origem_id = ?
                """, (resultado, linha) -> new ReservaDeLimiteCanceladaEvent(
                        resultado.getString("evento_cancelamento_id"),
                        resultado.getString("evento_origem_id"),
                        resultado.getString("solicitacao_id"),
                        resultado.getString("cliente_id"),
                        resultado.getBigDecimal("valor_devolvido"),
                        resultado.getBigDecimal("limite_disponivel_apos"),
                        resultado.getString("motivo"),
                        resultado.getObject("cancelada_em", OffsetDateTime.class)),
                eventoOrigemId);
        return resultados.stream().findFirst();
    }

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

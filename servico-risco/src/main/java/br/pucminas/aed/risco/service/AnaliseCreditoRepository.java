package br.pucminas.aed.risco.service;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class AnaliseCreditoRepository {
    private final JdbcTemplate bancoDeDados;

    public void criar(CreditoSolicitadoEvent evento) {
        bancoDeDados.update("""
                insert into analise_credito
                    (solicitacao_id, cliente_id, valor_solicitado, status, solicitada_em)
                values (?, ?, ?, ?, ?)
                """, evento.getSolicitacaoId(), evento.getClienteId(), evento.getValorSolicitado(),
                "PENDENTE", evento.getDataSolicitacao());
    }

    public boolean atualizarStatus(String solicitacaoId, String novoStatus, List<String> statusDeOrigem) {
        String marcadores = String.join(", ", statusDeOrigem.stream().map(status -> "?").toList());
        List<Object> parametros = new ArrayList<>();
        parametros.add(novoStatus);
        parametros.add(solicitacaoId);
        parametros.addAll(statusDeOrigem);
        int linhas = bancoDeDados.update(
                "update analise_credito set status = ? where solicitacao_id = ? and status in (" + marcadores + ")",
                parametros.toArray());
        return linhas == 1;
    }

    public Optional<String> buscarStatus(String solicitacaoId) {
        return bancoDeDados.queryForList(
                "select status from analise_credito where solicitacao_id = ?", String.class, solicitacaoId)
                .stream().findFirst();
    }

    public int contar() {
        return bancoDeDados.queryForObject("select count(*) from analise_credito", Integer.class);
    }

    public void excluirTodos() { bancoDeDados.update("delete from analise_credito"); }
}

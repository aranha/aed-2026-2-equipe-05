package com.aed20262equipe05.risco.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class EventoProcessadoRepository {
    private final JdbcTemplate bancoDeDados;

    public EventoProcessadoRepository(JdbcTemplate bancoDeDados) { this.bancoDeDados = bancoDeDados; }

    public boolean registrarSeNovo(String eventoId) {
        int linhas = bancoDeDados.update(
                "insert into evento_processado (evento_id) values (?) on conflict do nothing", eventoId);
        return linhas == 1;
    }

    public int contar() {
        return bancoDeDados.queryForObject("select count(*) from evento_processado", Integer.class);
    }

    public void excluirTodos() { bancoDeDados.update("delete from evento_processado"); }
}

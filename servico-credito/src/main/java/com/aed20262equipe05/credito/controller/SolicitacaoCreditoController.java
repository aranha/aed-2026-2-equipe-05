package com.aed20262equipe05.credito.controller;

import com.aed20262equipe05.credito.domain.CreditoSolicitadoEvent;
import com.aed20262equipe05.credito.domain.SolicitacaoCreditoVO;
import com.aed20262equipe05.credito.service.CreditoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ExceptionHandler;

@RestController
@RequestMapping("/solicitacoes")
public class SolicitacaoCreditoController {
    private final CreditoService creditoService;

    public SolicitacaoCreditoController(CreditoService creditoService) {
        this.creditoService = creditoService;
    }

    @PostMapping
    public ResponseEntity<CreditoSolicitadoEvent> solicitar(@RequestBody SolicitacaoCreditoVO solicitacao) {
        return ResponseEntity.accepted().body(creditoService.solicitar(solicitacao));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> tratarEntradaInvalida(IllegalArgumentException falha) {
        return ResponseEntity.badRequest().body(falha.getMessage());
    }
}

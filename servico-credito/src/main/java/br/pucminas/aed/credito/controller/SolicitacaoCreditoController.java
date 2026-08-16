package br.pucminas.aed.credito.controller;

import br.pucminas.aed.credito.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.credito.domain.SolicitacaoCreditoVO;
import br.pucminas.aed.credito.service.CreditoService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ExceptionHandler;

@RestController
@RequestMapping("/solicitacoes")
@RequiredArgsConstructor
public class SolicitacaoCreditoController {
    private final CreditoService creditoService;

    @PostMapping
    public ResponseEntity<CreditoSolicitadoEvent> solicitar(@RequestBody SolicitacaoCreditoVO solicitacao) {
        return ResponseEntity.accepted().body(creditoService.solicitar(solicitacao));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> tratarEntradaInvalida(IllegalArgumentException falha) {
        return ResponseEntity.badRequest().body(falha.getMessage());
    }
}

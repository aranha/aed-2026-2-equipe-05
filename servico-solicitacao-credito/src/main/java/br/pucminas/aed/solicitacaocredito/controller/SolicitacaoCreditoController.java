package br.pucminas.aed.solicitacaocredito.controller;

import br.pucminas.aed.solicitacaocredito.domain.CreditoSolicitadoEvent;
import br.pucminas.aed.solicitacaocredito.service.SolicitacaoCreditoService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/solicitacao-credito")
public class SolicitacaoCreditoController {

    private final SolicitacaoCreditoService solicitacaoCreditoService;

    public SolicitacaoCreditoController(SolicitacaoCreditoService solicitacaoCreditoService) {
        this.solicitacaoCreditoService = solicitacaoCreditoService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void solicitarCredito(@RequestBody CreditoSolicitadoEvent evento) {
        solicitacaoCreditoService.solicitar(evento);
    }

}
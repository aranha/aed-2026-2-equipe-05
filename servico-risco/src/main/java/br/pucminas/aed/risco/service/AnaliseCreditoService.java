package br.pucminas.aed.risco.service;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class AnaliseCreditoService {
    private final EventoProcessadoRepository eventoProcessadoRepository;
    private final AnaliseCreditoRepository analiseCreditoRepository;

    @Transactional
    public boolean processar(String eventoId, CreditoSolicitadoEvent evento) {
        if (!eventoProcessadoRepository.registrarSeNovo(eventoId)) {
            return false;
        }
        analiseCreditoRepository.criar(evento);
        return true;
    }
}

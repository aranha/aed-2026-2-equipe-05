# ADR-002 — Domínio do projeto

## Status
Aceita · 2026-08-15 · Equipe 05

## Contexto
O processo de concessão de crédito envolve diferentes responsabilidades de negócio, como
análise de elegibilidade, análise antifraude, geração da proposta, contratação e liberação do crédito. Tendo em vista as propostas sugeridas pelo grupo, esse domínio aparenta ser o mais adequado para o contexto do projeto pois atende aos requisitos exigidos.
O domínio foi apresentado pelo aluno Paulo Aranha, que possui experiência em modelagem de arquitetura para o mercado financeiro.

## Decisão
O processo inicia quando o cliente solicita um empréstimo (gatilho: CréditoSolicitado), disparando análises sequenciais de elegibilidade, prevenção a fraude e cálculo de condições para geração de uma proposta comercial. Antes de apresentar a proposta, o limite aprovado é reservado. Após a aceitação e assinatura do contrato, o fluxo culmina no desembolso pelo Core Bancário (desfecho de sucesso: CréditoLiberado). Se a proposta for recusada ou expirar, a reserva é cancelada para desfazer o efeito anterior.

  - Ponto de decisão: Avaliar Elegibilidade
    Justificativa: Aplica políticas de risco, score e capacidade de pagamento do cliente para decidir se o fluxo bifurca para ElegibilidadeAprovada ou para o encerramento com ElegibilidadeRecusada.

  - Sistema externo: Bureau de Crédito, Sistema Antifraude ou Core Bancário
    Justificativa: O processo depende de integrações com serviços fora do domínio direto da aplicação para validação cadastral/score (Bureau) e execução financeira da transferência do montante (Core Bancário).

  - Caminho de exceção com compensação: PropostaDeCreditoRecusada ou PropostaDeCreditoExpirada após LimiteDeCreditoReservado
    Justificativa: A reserva reduz temporariamente o limite disponível do cliente. Quando a proposta é recusada ou expira, a política Cancelar Reserva de Limite desfaz esse efeito e produz ReservaDeLimiteCancelada. Não se trata apenas de encerrar o fluxo, mas de reverter uma alteração anterior.

  - Algo que valha reprocessar: A projeção do histórico e do estado atual das solicitações de crédito.
    Justificativa: A visão de acompanhamento e auditoria pode ser reconstruída a partir de eventos como CréditoSolicitado, ElegibilidadeAprovada, PropostaDeCreditoAceita, ContratoAssinado e CréditoLiberado. Esse reprocessamento permite corrigir uma projeção ou criar uma nova consulta sem repetir desembolsos nem chamar novamente os sistemas externos.

## Alternativas consideradas
- **Mercado Rápido — ledger de pagamentos:** recusado porque a equipe não possui experiência real compartilhada suficiente sobre conciliação e estornos desse processo; escolher esse recorte exigiria pressupor regras essenciais.
- **Mercado Rápido — avaliação e reputação:** recusado porque, no recorte discutido, a compensação de uma avaliação já utilizada no cálculo de reputação não ficou claramente definida.
- **Processo centralizado de concessão de crédito:** recusado porque concentraria elegibilidade, antifraude, contratação e liberação em um único serviço, aumentando o acoplamento e dificultando a evolução independente das fronteiras.

## Consequencias aceitas

- O que esta decisão custa

  Gestão de estado: Manter o histórico de propostas de longa duração e etapas assíncronas.

  Idempotência complexa: Cuidado redobrado para evitar duplo desembolso financeiro no reprocessamento.

  Dependência externa: Uso intensivo de mocks para simular bureaus e antifraudes.

- O que fica de fora

  Cobrança de parcelas, emissão de boletos, renegociação e faturamento mensal pós-crédito.

  Contabilidade interna detalhada (ledger/partidas dobradas), deixada para o sistema bancário.

- O que fica difícil nas Aulas 04 e 05

  Sagas complexas: Lidar com eventos que dependem de ações humanas demoradas (como o aceite e assinatura do contrato).

  Reprocessamento com risco: Tratar falhas no Core Bancário sem correr o risco de transferir o dinheiro duas vezes por engano (retry cego).

- Plano de mitigação

  A reserva de limite será identificada pela solicitação e terá compensação idempotente, evitando cancelamento duplicado. A projeção de acompanhamento será separada dos efeitos financeiros, para que seu reprocessamento nunca volte a executar uma liberação de crédito.

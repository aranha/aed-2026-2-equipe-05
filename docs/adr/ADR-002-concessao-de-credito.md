# Concessão de Crédito

## Status
Aceita · 2026-08-15 · Equipe 05

## Contexto
O processo de concessão de crédito envolve diferentes responsabilidades de negócio, como
análise de elegibilidade, análise antifraude, geração da proposta, contratação e liberação do crédito. Tendo em vista as propostas sugeridas pelo grupo, esse domínio aparenta ser o mais adequado para o contexto do projeto pois atende aos requisitos exigidos.
O domínio foi apresentado pelo aluno Paulo Aranha, que possui experiência em modelagem de arquitetura para o mercado financeiro.

## Decisão
O processo inicia quando o cliente solicita um empréstimo (gatilho: CréditoSolicitado), disparando análises sequenciais de elegibilidade, prevenção a fraude e cálculo de condições para geração de uma proposta comercial. Após a aceitação e assinatura do contrato pelo cliente, o fluxo culmina na integração com o sistema central para o desembolso do valor acordado (desfecho de sucesso: CréditoLiberado), ou no encerramento da solicitação caso haja recusa em alguma etapa de validação.

  - Ponto de decisão: Avaliar Elegibilidade
    Justificativa: Aplica políticas de risco, score e capacidade de pagamento do cliente para decidir se o fluxo bifurca para ElegibilidadeAprovada ou para o encerramento com ElegibilidadeRecusada.

  - Sistema externo: Bureau de Crédito, Sistema Antifraude ou Core Bancário
    Justificativa: O processo depende de integrações com serviços fora do domínio direto da aplicação para validação cadastral/score (Bureau) e execução financeira da transferência do montante (Core Bancário).

  - Caminho de exceção com compensação: FraudeIdentificada, PropostaDeCréditoRecusada ou ElegibilidadeRecusada
    Justificativa: Desvia do fluxo principal e executa ações de compensação/encerramento de ciclo de vida (Recusar Solicitação de Crédito / Encerrar Solicitação de Crédito), liberando reservas ou marcando o estado da entidade para evitar inconsistências e novas operações indevidas.

  - Algo que valha reprocessar: A falha técnica no desembolso indicada pelo evento LiberaçãoDeCréditoFalhou.
    Justificativa: Como o contrato já foi assinado e aprovado por todas as regras de negócio, a falha decorre de indisponibilidade ou timeout no [Core Bancário]. A política sempre que LiberaçãoDeCréditoFalhou, então Solicitar Nova Liberação implementa um mecanismo de retry/reprocessamento automático (ou via fila/saga) para garantir a entrega final do dinheiro sem obrigar o cliente a reiniciar a proposta.

## Alternativas consideradas
Mercado Rápido(ledger de pagamentos, avaliação)

Motivo da recusa: Recusado pois não traz os critérios apresentados no escopo do projeto de forma tão explicita e densa em regras de negócio como a solução aceita pela equipe.

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
# ADR-003 - Chave de partição do evento CreditoSolicitado

## Status
Aceita · 2026-09-07 · Equipe 05

## Contexto
O `servico-credito` publica `credito.solicitacao.solicitada.v1` num tópico criado por ele com três partições. Dois grupos de consumidores leem esse tópico: `risco-credito-v1`, que registra a análise de forma idempotente, e `risco-fluxo-creditos-v1`, que agrega o volume solicitado por janela fixa de cinco minutos.

No Kafka a ordem só existe dentro de uma partição, e a chave decide a partição. A chave escolhida no primeiro dia define quais perguntas o negócio consegue responder sem republicar o fluxo com outra chave. As quatro perguntas que orientaram a decisão:

1. Qual é a menor unidade cuja ordem o negócio exige?
2. Por qual dimensão o negócio pergunta o tempo todo?
3. Alguma chave concentra volume?
4. Quantas partições, e dá para mudar depois?

## Decisão
A chave de partição é `solicitacaoId`, a identidade da solicitação de crédito. Ela é gerada pelo `CreditoService` no momento em que o fato é afirmado e é enviada como chave do `ProducerRecord`.

- **Ordem (pergunta 1):** a solicitação é a menor unidade cuja ordem importa. Os fatos que virão a seguir no fluxo do ADR-002 (elegibilidade avaliada, proposta gerada, limite reservado, proposta aceita ou recusada, crédito liberado) pertencem à mesma solicitação e precisam ser lidos na ordem em que ocorreram. Com a mesma chave, todos caem na mesma partição. Entre solicitações diferentes o negócio não exige ordem.
- **Pergunta que a chave responde sem repartir (pergunta 2):** "quanto crédito foi solicitado em cada janela de cinco minutos". A agregação atual é da carteira inteira, não depende de nenhuma dimensão do evento, e por isso fecha correta com qualquer distribuição entre partições e com uma instância só do agregador.
- **Pergunta que a chave deixou de responder:** qualquer agregação por cliente, como "quanto este cliente solicitou nesta semana" ou "quantas solicitações por cliente por dia". As solicitações de um mesmo cliente caem em partições diferentes; com mais de uma instância do agregador, nenhuma delas veria o cliente inteiro e cada uma produziria um total parcial que parece correto sozinho. O mesmo vale para agregações por `canalOrigem`.
- **Partição quente (pergunta 3):** `solicitacaoId` é um UUID, e a dispersão distribui as solicitações de forma uniforme. Um cliente ou canal com muito volume não concentra carga numa partição, porque a chave não é o cliente nem o canal.
- **Número de partições (pergunta 4):** três, definidas pelo `servico-credito` na criação do tópico. É o teto de paralelismo dos consumidores. Aumentar depois redistribui as chaves e quebra a ordem por solicitação; portanto o número é decisão de criação, não de operação.

## Alternativas consideradas
- **`clienteId` como chave:** responderia agregações por cliente sem repartir, mas concentraria numa partição todas as solicitações de um cliente de alto volume e daria ao negócio uma ordem entre solicitações distintas do mesmo cliente que ele não pediu. Recusada.
- **Sem chave:** distribuição por rodízio, máximo espalhamento e nenhuma ordem garantida. Os eventos de uma mesma solicitação poderiam ser processados fora de ordem quando o fluxo tiver mais de um fato por solicitação. Recusada.
- **`canalOrigem` como chave:** poucos valores distintos (`APP`, por exemplo) gerariam partições quentes e ociosas, e o canal não é uma unidade de ordem do negócio. Recusada.

## Consequencias aceitas
- Agregar por cliente exige um **repartition topic**: um serviço que lê `credito.solicitacao.solicitada.v1`, republica em `credito.solicitacao.solicitada.v1.por-cliente` chaveado por `clienteId`, copiando os cabeçalhos CloudEvents sem alterar a carga. Custa disco, uma passagem a mais pelo broker e mais um serviço para operar.
- A chave do tópico atual não muda. Trocar a chave em produção redistribuiria o histórico e quebraria a ordem dos eventos já gravados; o caminho é sempre um tópico novo.
- O agregador de fluxo só está correto com uma instância lendo as três partições enquanto a pergunta for da carteira inteira. Se a pergunta passar a ter dono (por cliente), escalar o agregador para várias instâncias sem repartir produz resultados parciais sem nenhum erro na tela.
- Três partições é o teto de paralelismo dos consumidores deste tópico. Se o volume exigir mais, o caminho é um tópico novo com mais partições e migração dos consumidores, não a alteração do tópico existente.

**Se a pergunta por cliente virar prioridade:** a equipe implementa o repartition topic descrito acima como um serviço independente, mantém `solicitacaoId` no tópico de origem e passa o agregador por cliente a ler o tópico repartido. A decisão será registrada em novo ADR, com o custo de operação nomeado.

**Como saberemos que erramos:** se surgir uma pergunta de negócio recorrente por uma dimensão que não seja a solicitação e a equipe estiver pagando repartição para cada uma delas; ou se o `--describe` do grupo mostrar uma partição consistentemente mais carregada que as outras, sinal de que a chave deixou de dispersar.

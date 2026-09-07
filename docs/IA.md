## Aula 02

### Registro da interação com IA

#### 1. Interação usada para gerar e ajustar o `README.md` do projeto.

- Solicitei à IA a criação de um passo a passo para rodar o projeto no `README.md`, usando como referência o controller `SolicitacaoCreditoController`.
- A IA identificou o endpoint `POST /solicitacoes`, os campos esperados no corpo da requisição e criou exemplos de chamada com `curl`.
- Depois, solicitei ajustes no português e na acentuação do texto.
- Também pedi a troca dos comandos com Maven Wrapper (`mvnw`) por comandos usando `mvn`, a fim de manter o padrão do "demo-kafka-idempotencia".
- Durante o teste do exemplo de `curl` no PowerShell, identifiquei problemas de parsing do JSON e pedi ajustes até chegar a um comando direto para execução.

#### Recusa da sugestão da IA

- A IA sugeriu montar o corpo da requisição no PowerShell usando uma variável com `ConvertTo-Json`. Recusei essa abordagem porque, ao copiar e colar no terminal, o comando quebrava a execução. Fiz diferente do proposto e optei por manter o exemplo como um comando `curl.exe` direto no README.

#### 2. Interação usada para auxiliar na redação da ADR-02

- A partir de um mural feito pela equipe, solicitei a elaboração de um resumo refinado e a justificativa de atendimento aos quatro critérios essenciais (regra de negócio, sistema externo, exceção com compensação e reprocessamento). A IA mapeou os elementos do mural correspondentes a cada critério.
- Pedi à IA uma avaliação crítica de trade-offs e impactos arquiteturais para identificar possíveis pontos cegos e riscos técnicos não previstos pela equipe.

#### Recusa da sugestão da IA

- A IA gerou uma resposta excessivamente detalhada sobre a avaliação crítica de trade-offs do domínio escolhido pela equipe. Optei por refinar o texto de forma mais concisa e direta aos pontos essenciais, priorizando o foco nos desafios reais que são relevantes para o projeto.

#### 3. Interação usada para revisar o que faltava para a entrega do que foi solicitado para a aula 2:
- Solicitei à IA a criação de um checklist com todos os itens que foram solicitados no documento disponibilizado com os requisitos para a entrega do que foi pedido para a aula 2.
- A IA identificou os itens que faltavam e as correções e assim as fiz.
- Como se trata de um checklist de requisitos exigidos para a entrega, foi aceito tudo que a IA fez nesse caso.

## Aula 03

### Registro da interação com IA

#### 1. Interação usada para definir o desenho do novo consumidor de fluxo

- Solicitei à IA uma sugestão de implementação para atender ao requisito de criar um novo consumidor no mesmo tópico Kafka, com grupo de consumidores próprio e agregação por janela de tempo.
- A IA analisou o consumidor já existente no `servico-risco` e sugeriu manter o novo consumidor no mesmo serviço, usando o grupo `risco-fluxo-creditos-v1`, sem interferir no consumidor principal da análise de crédito.

#### Recusa da sugestão da IA

- A IA apresentou endpoint, tópico de saída, tabela ou log como possibilidades para tornar o resultado da agregação observável. Recusei a criação de endpoint, tópico de saída ou tabela nesta etapa, porque aumentaria o escopo da entrega. A equipe optou por observar o resultado no log do `servico-risco`, que atende ao requisito com menor complexidade.

## Aula 04

### Registro da interação com IA

#### 1. Interação usada para redigir o ADR-003 a partir do código existente

- Solicitei à IA que analisasse o `servico-credito` e o `servico-risco` contra as Notas de Aula e o código de exemplo da disciplina e redigisse o rascunho do ADR-003 respondendo às quatro perguntas do slide da aula 04: menor unidade de ordem, dimensão pela qual o negócio pergunta, risco de partição quente e número de partições.
- A IA identificou no `CreditoService` que a chave enviada é `solicitacaoId`, que o tópico nasce com três partições no `CreditoConfig` e que o agregador de fluxo não depende de nenhuma dimensão do evento. Nomeou a pergunta que a chave responde sem repartir e a que deixou de responder (agregação por cliente), e propôs o repartition topic como caminho caso essa pergunta vire prioridade.
- Aceitei a estrutura e as justificativas, revisando o texto para manter a linguagem do domínio de crédito.

#### 2. Interação usada para revisar o estado do repositório em relação ao que o professor pede

- Solicitei à IA um levantamento do que faltava no repositório em relação aos enunciados e slides das aulas 02 a 05.
- A IA apontou a ausência do ADR-003, da folha de rosto da aula 04 e da tag correspondente, além de itens da aula 05, e sugeriu a ordem de trabalho: ADR antes do código que ele justifica.

#### Recusa da sugestão da IA

- A IA sugeriu remover a possibilidade de o cliente informar `dataSolicitacao` na requisição HTTP, com o argumento de que o instante do fato deve ser afirmado pelo serviço que o produz, da mesma forma que o `eventoId`. A equipe recusou nesta etapa: o campo é opcional, o serviço assume o instante atual quando ele não vem, e informá-lo é o que permite exercitar janelas e retardatários na demonstração sem esperar o relógio. A consequência aceita é que um cliente pode datar o fato no passado ou no futuro, e isso ficará registrado como decisão quando o contrato de entrada for revisto.
- A IA também propôs incluir nesta entrega uma alteração no `FluxoCreditoSolicitadoListener` para registrar partição, offset e chave no log, como no exemplo da disciplina. A equipe recusou incluir código na entrega da aula 04, que o professor definiu como três documentos, para não misturar a justificativa da chave com mudanças de comportamento do agregador.

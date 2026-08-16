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
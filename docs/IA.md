## Aula 02

### Interação usada para gerar e ajustar o `README.md` do projeto.

#### Registro da interação com IA

- Solicitei à IA a criação de um passo a passo para rodar o projeto no `README.md`, usando como referência o controller `SolicitacaoCreditoController`.
- A IA identificou o endpoint `POST /solicitacoes`, os campos esperados no corpo da requisição e criou exemplos de chamada com `curl`.
- Depois, solicitei ajustes no português e na acentuação do texto.
- Também pedi a troca dos comandos com Maven Wrapper (`mvnw`) por comandos usando `mvn`, a fim de manter o padrão do "demo-kafka-idempotencia".
- Durante o teste do exemplo de `curl` no PowerShell, identifiquei problemas de parsing do JSON e pedi ajustes até chegar a um comando direto para execução.

#### Recusa da sugestão da IA

- A IA sugeriu montar o corpo da requisição no PowerShell usando uma variável com `ConvertTo-Json`. Recusei essa abordagem porque, ao copiar e colar no terminal, o comando quebrava a execução. Fiz diferente do proposto e optei por manter o exemplo como um comando `curl.exe` direto no README.

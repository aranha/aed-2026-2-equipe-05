# AED 2026/2 - Equipe 05

Projeto com serviços para solicitação e análise de crédito usando Spring Boot, Kafka e PostgreSQL.

Para entender a arquitetura, os eventos e as decisões, comece por [docs/arquitetura.md](docs/arquitetura.md).

## Integrantes

| Nome                                     | Matrícula | 
|------------------------------------------|-----------|
| Paulo Euclydes Aranha Junior (**Líder**) | 255171    | 
| Vinícius Eduardo Silva Oliveira          | 1310290   | 
| Marcus Vinicius da Cruz Santos           | 255495    | 
| Rafael Oliveira de Lima                  | 258889    |
| Guilherme Nunes Faria                    | 1474257   |
| Hugo Fontolan Piani                      | 258724    |
| Sesaque de Oliveira da Cruz              | 254176    |

## Pré-requisitos

- Java 21
- Maven
- Docker e Docker Compose
- Git Bash, PowerShell ou terminal equivalente

## Como rodar o projeto

### 1. Subir a infraestrutura

Na raiz do projeto, suba Kafka, PostgreSQL e Kafka UI:

```powershell
docker compose up -d
```

Para conferir se os containers estão rodando:

```powershell
docker compose ps
```

Serviços expostos pelo `docker-compose.yml`:

- Kafka: `localhost:19092`
- PostgreSQL: `localhost:15432`
- Kafka UI: `http://localhost:8081`

### 2. Gerar os arquivos `.jar`

Na raiz do projeto, gere o pacote dos dois serviços:

```powershell
mvn -f servico-credito\pom.xml package -DskipTests
mvn -f servico-risco\pom.xml package -DskipTests
```

No Linux/macOS ou Git Bash:

```bash
mvn -f servico-credito/pom.xml package -DskipTests
mvn -f servico-risco/pom.xml package -DskipTests
```

Os arquivos `.jar` serão gerados nas pastas `target` de cada serviço.

### 3. Rodar o serviço de crédito

Em um terminal, na raiz do projeto:

```powershell
java -jar servico-credito\target\servico-credito-0.0.1-SNAPSHOT.jar
```

No Linux/macOS ou Git Bash:

```bash
java -jar servico-credito/target/servico-credito-0.0.1-SNAPSHOT.jar
```

Esse serviço sobe uma API HTTP na porta `8080`.

### 4. Rodar o serviço de risco

Em outro terminal, na raiz do projeto:

```powershell
java -jar servico-risco\target\servico-risco-0.0.1-SNAPSHOT.jar
```

No Linux/macOS ou Git Bash:

```bash
java -jar servico-risco/target/servico-risco-0.0.1-SNAPSHOT.jar
```

Esse serviço consome eventos do Kafka, grava os resultados da análise no PostgreSQL, acompanha o desfecho da saga de reserva (status `PENDENTE`, `RESERVADA` ou `CANCELADA` em `analise_credito`) e mantém um consumidor separado para observar o fluxo de crédito solicitado por janelas de tempo.

## Como testar

Para rodar os testes do serviço de crédito:

```powershell
mvn -f servico-credito\pom.xml test
```

Para rodar os testes do serviço de risco:

```powershell
mvn -f servico-risco\pom.xml test
```

No Linux/macOS ou Git Bash, use `/` nos caminhos:

```bash
mvn -f servico-credito/pom.xml test
mvn -f servico-risco/pom.xml test
```

## Chamada da API de solicitação de crédito

O controller `SolicitacaoCreditoController` expõe o endpoint:

```http
POST /solicitacoes
```

URL local:

```text
http://localhost:8080/solicitacoes
```

Em outro terminal:

Exemplo usando `curl` no PowerShell:

```powershell
curl.exe --% -X POST "http://localhost:8080/solicitacoes" -H "Content-Type: application/json" -d "{\"clienteId\":\"cliente-123\",\"valorSolicitado\":15000.00,\"canalOrigem\":\"APP\"}"
```

Exemplo usando `curl` no Linux/macOS ou Git Bash:

```bash
curl -X POST "http://localhost:8080/solicitacoes" \
  -H "Content-Type: application/json" \
  -d '{
    "clienteId": "cliente-123",
    "valorSolicitado": 15000.00,
    "canalOrigem": "APP"
  }'
```

Resposta esperada: HTTP `202 Accepted`, retornando o evento publicado no Kafka:

```json
{
  "eventoId": "uuid-do-evento",
  "solicitacaoId": "uuid-da-solicitacao",
  "clienteId": "cliente-123",
  "valorSolicitado": 15000.00,
  "dataSolicitacao": "2026-08-16T10:00:00-03:00",
  "canalOrigem": "APP"
}
```

Campos obrigatórios no corpo da requisição:

- `clienteId`: identificador do cliente
- `valorSolicitado`: valor positivo solicitado
- `canalOrigem`: canal de origem da solicitação

Campo opcional:

- `dataSolicitacao`: data e hora de ocorrência da solicitação. Quando esse campo não é informado, o `servico-credito` usa a data e hora atual. Informar esse campo é útil para simular cenários de janelas de tempo de forma mais fácil.

Se algum campo obrigatório estiver ausente ou inválido, a API retorna HTTP `400 Bad Request` com a mensagem de erro.

## Consumidor de fluxo por janela de tempo

Além do consumidor principal de análise de risco, o `servico-risco` possui o consumidor `FluxoCreditoSolicitadoListener`, que lê o mesmo tópico Kafka `credito.solicitacao.solicitada.v1` usando o grupo próprio `risco-fluxo-creditos-v1`.

Esse consumidor responde à pergunta: **qual foi o volume de crédito solicitado a cada janela fixa de 5 minutos?**

A agregação usa o relógio de ocorrência do evento, ou seja, o campo `dataSolicitacao`. As janelas são alinhadas em blocos fixos de 5 minutos: `12:00`, `12:05`, `12:10`, e assim por diante. O resultado aparece no log do `servico-risco`.

O grupo pode ser alterado pela variável `KAFKA_GRUPO_FLUXO_CREDITO`. Para limitar o uso de memória, o consumidor mantém por padrão as 288 janelas mais recentes. Esse limite é por quantidade de janelas, não por idade: 288 janelas de 5 minutos correspondem a 24 horas apenas quando são consecutivas. O limite pode ser alterado pela variável `FLUXO_CREDITO_MAXIMO_JANELAS_RETIDAS`.

Os IDs usados na deduplicação também são removidos quando suas respectivas janelas saem da retenção. Assim, a proteção contra reentregas pelo mesmo `eventoId` vale enquanto a janela permanece em memória; após seu descarte, o evento pode ser processado novamente.

Para testar, deixe o `servico-credito` e o `servico-risco` rodando e envie solicitações com `dataSolicitacao` informada.

Exemplo usando `curl` no PowerShell, enviando dois eventos para a janela de `12:00`:

```powershell
curl.exe --% -X POST "http://localhost:8080/solicitacoes" -H "Content-Type: application/json" -d "{\"clienteId\":\"cliente-123\",\"valorSolicitado\":1000.00,\"canalOrigem\":\"APP\",\"dataSolicitacao\":\"2026-08-22T12:02:34-03:00\"}"
curl.exe --% -X POST "http://localhost:8080/solicitacoes" -H "Content-Type: application/json" -d "{\"clienteId\":\"cliente-456\",\"valorSolicitado\":2000.00,\"canalOrigem\":\"APP\",\"dataSolicitacao\":\"2026-08-22T12:04:10-03:00\"}"
```

Exemplo usando `curl` no PowerShell, enviando um evento para a janela de `12:05`:

```powershell
curl.exe --% -X POST "http://localhost:8080/solicitacoes" -H "Content-Type: application/json" -d "{\"clienteId\":\"cliente-789\",\"valorSolicitado\":5000.00,\"canalOrigem\":\"APP\",\"dataSolicitacao\":\"2026-08-22T12:07:00-03:00\"}"
```

Exemplo usando `curl` no Linux/macOS ou Git Bash:

```bash
curl -X POST "http://localhost:8080/solicitacoes" \
  -H "Content-Type: application/json" \
  -d '{
    "clienteId": "cliente-123",
    "valorSolicitado": 1000.00,
    "canalOrigem": "APP",
    "dataSolicitacao": "2026-08-22T12:02:34-03:00"
  }'

curl -X POST "http://localhost:8080/solicitacoes" \
  -H "Content-Type: application/json" \
  -d '{
    "clienteId": "cliente-456",
    "valorSolicitado": 2000.00,
    "canalOrigem": "APP",
    "dataSolicitacao": "2026-08-22T12:04:10-03:00"
  }'

curl -X POST "http://localhost:8080/solicitacoes" \
  -H "Content-Type: application/json" \
  -d '{
    "clienteId": "cliente-789",
    "valorSolicitado": 5000.00,
    "canalOrigem": "APP",
    "dataSolicitacao": "2026-08-22T12:07:00-03:00"
  }'
```

No terminal do `servico-risco`, o log esperado será semelhante a:

```text
Fluxo de credito solicitado | janela=2026-08-22T12:00-03:00 | quantidade=1 | totalSolicitado=1000.00
Fluxo de credito solicitado | janela=2026-08-22T12:00-03:00 | quantidade=2 | totalSolicitado=3000.00
Fluxo de credito solicitado | janela=2026-08-22T12:05-03:00 | quantidade=1 | totalSolicitado=5000.00
```

## Saga de reserva de limite

Quando a elegibilidade é aprovada, o `servico-credito` reserva o valor aprovado do limite do cliente e publica `LimiteDeCreditoReservado`. Se a proposta for recusada ou expirar, ele devolve o limite, marca a reserva como `CANCELADA` e publica `ReservaDeLimiteCancelada`. O `servico-risco` consome os dois eventos e atualiza o status da análise. Os contratos estão em [docs/contratos-da-saga.md](docs/contratos-da-saga.md) e o passo a passo detalhado em [docs/saga/](docs/saga/).

`ElegibilidadeAprovada`, `PropostaDeCreditoRecusada` e `PropostaDeCreditoExpirada` são publicados por serviços fora deste recorte; para exercitar a saga, publique-os pela Kafka UI (`http://localhost:8081`), no tópico correspondente, com a chave igual ao `solicitacaoId` e o cabeçalho `{"ce_id": "<id do evento>"}`.

1. Cadastre o limite do cliente:

```bash
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "INSERT INTO limite_credito (cliente_id, limite_total, limite_disponivel) VALUES ('cli-ficticio-001', 10000.00, 10000.00) ON CONFLICT (cliente_id) DO UPDATE SET limite_total = 10000.00, limite_disponivel = 10000.00;"
```

2. Publique em `credito.elegibilidade.aprovada.v1`, com chave `sol-demo-01` e cabeçalho `{"ce_id": "evt-eleg-demo-01"}`:

```json
{"eventoId": "evt-eleg-demo-01", "solicitacaoId": "sol-demo-01", "clienteId": "cli-ficticio-001", "valorAprovado": 3000.00, "dataAprovacao": "2026-09-27T10:00:00-03:00"}
```

3. Publique em `credito.proposta.recusada.v1`, com chave `sol-demo-01` e cabeçalho `{"ce_id": "evt-recusa-demo-01"}`:

```json
{"eventoId": "evt-recusa-demo-01", "solicitacaoId": "sol-demo-01", "motivo": "TAXA_ACIMA_DO_ESPERADO", "dataRecusa": "2026-09-27T10:05:00-03:00"}
```

4. Confira o limite (10.000 depois de 7.000) e o desfecho da reserva:

```bash
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_disponivel FROM limite_credito;" -c "SELECT solicitacao_id, status, cancelada_em FROM reserva_limite;"
```

Publicar a mesma recusa de novo, ou uma expiração para a mesma solicitação, não devolve o limite outra vez.

## Tratamento de falhas: retentativa e DLQ

Os dois serviços tratam falhas da mesma forma (ver [ADR-006](docs/adr/ADR-006-retentativa-dlq-e-falha-da-compensacao.md)):

- **Falha transitória** (banco indisponível, ou um evento que chegou antes daquele de que depende, como a recusa antes da reserva): até 4 retentativas com espera de 0,5 s, 1 s, 2 s e 4 s. Esgotadas, o registro vai para a DLQ.
- **Falha permanente** (payload inválido, cabeçalho `ce_id` ausente, violação de integridade, cliente sem limite ou com limite insuficiente): vai direto para a DLQ, sem retentar.

Cada tópico consumido tem uma DLQ `<tópico>.dlq`. O registro na DLQ leva a carga, os cabeçalhos `ce_*` originais, o motivo da falha (`kafka_dlt-exception-message`), o tópico, a partição, o offset e o grupo de origem (`kafka_dlt-original-*`), e a classificação (`classificacao`: `PERMANENTE` ou `TRANSITORIA`).

Para ver o conteúdo de uma DLQ, com os cabeçalhos:

```bash
docker exec -it aed-equipe-05-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic credito.proposta.recusada.v1.dlq --from-beginning --property print.headers=true --property print.key=true
```

A política é configurável por variável de ambiente: `RETENTATIVA_TENTATIVAS`, `RETENTATIVA_INTERVALO_INICIAL_MS`, `RETENTATIVA_MULTIPLICADOR` e `RETENTATIVA_INTERVALO_MAXIMO_MS`.

## Idempotência

O serviço de risco possui uma classe de teste dedicada para validar idempotência:

```text
servico-risco/src/test/java/br/pucminas/aed/risco/IdempotenciaTest.java
```

Nesse teste, o mesmo evento de solicitação de crédito é publicado três vezes no tópico Kafka. A validação garante que o consumidor processe o evento apenas uma vez, registrando um único evento processado e gerando um único efeito na análise de crédito.

Para executar esse teste:

```powershell
mvn -f servico-risco\pom.xml -Dtest=IdempotenciaTest test
```

No Linux/macOS ou Git Bash:

```bash
mvn -f servico-risco/pom.xml -Dtest=IdempotenciaTest test
```

## Parar o ambiente

Para somente parar os containers:

```powershell
docker compose stop
```

Para remover os containers e apagar os dados persistidos do Kafka e PostgreSQL:
```powershell
docker compose down -v
```

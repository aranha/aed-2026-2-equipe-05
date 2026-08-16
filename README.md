# AED 2026/2 - Equipe 05

Projeto com serviços para solicitação e análise de crédito usando Spring Boot, Kafka e PostgreSQL.

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

Esse serviço consome eventos do Kafka e grava os resultados no PostgreSQL.

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

Se algum campo obrigatório estiver ausente ou inválido, a API retorna HTTP `400 Bad Request` com a mensagem de erro.

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

# Aula 02 - Folha de rosto

## O que foi feito

Nesta etapa, a equipe definiu o domínio de concessão de crédito e implementou uma primeira integração assíncrona entre serviços usando Kafka. O `servico-credito` recebe solicitações de crédito por API HTTP, publica o evento `CreditoSolicitado` no Kafka e o `servico-risco` consome esse evento, registra o processamento e grava a análise no banco PostgreSQL.

Também foram adicionados testes automatizados, incluindo um teste específico de idempotência para garantir que a reentrega do mesmo evento não gere efeitos duplicados.

## Por onde começar a leitura

1. [README do projeto](../../README.md): instruções completas para subir a infraestrutura, empacotar os serviços, rodar os `.jar`, testar e chamar a API.
2. [ADR-002 - Domínio do projeto](../adr/ADR-002-dominio-do-projeto.md): decisão arquitetural sobre o domínio de concessão de crédito.
3. [Serviço de crédito](../../servico-credito): API HTTP e publicação do evento no Kafka.
4. [SolicitacaoCreditoController](../../servico-credito/src/main/java/br/pucminas/aed/credito/controller/SolicitacaoCreditoController.java): endpoint `POST /solicitacoes`.
5. [CreditoService](../../servico-credito/src/main/java/br/pucminas/aed/credito/service/CreditoService.java): validação da solicitação e publicação do evento.
6. [Serviço de risco](../../servico-risco): consumo do evento, idempotência e persistência da análise.
7. [CreditoSolicitadoListener](../../servico-risco/src/main/java/br/pucminas/aed/risco/controller/CreditoSolicitadoListener.java): consumidor Kafka.
8. [AnaliseCreditoService](../../servico-risco/src/main/java/br/pucminas/aed/risco/service/AnaliseCreditoService.java): regra de processamento idempotente.
9. [IdempotenciaTest](../../servico-risco/src/test/java/br/pucminas/aed/risco/IdempotenciaTest.java): teste de reentrega do mesmo evento.
10. [Registro de uso de IA](../IA.md): interações usadas na elaboração da documentação.

## Como rodar

Execute os comandos abaixo a partir da raiz do repositório.

Subir Kafka, PostgreSQL e Kafka UI:

```powershell
docker compose up -d
```

Gerar os `.jar`:

```powershell
mvn -f servico-credito\pom.xml package -DskipTests
mvn -f servico-risco\pom.xml package -DskipTests
```

Rodar o serviço de crédito:

```powershell
java -jar servico-credito\target\servico-credito-0.0.1-SNAPSHOT.jar
```

Rodar o serviço de risco em outro terminal:

```powershell
java -jar servico-risco\target\servico-risco-0.0.1-SNAPSHOT.jar
```

Chamar a API em outro terminal:

```powershell
curl.exe --% -X POST "http://localhost:8080/solicitacoes" -H "Content-Type: application/json" -d "{\"clienteId\":\"cliente-123\",\"valorSolicitado\":15000.00,\"canalOrigem\":\"APP\"}"
```

Rodar os testes:

```powershell
mvn -f servico-credito\pom.xml test
mvn -f servico-risco\pom.xml test
```

Rodar apenas o teste de idempotência:

```powershell
mvn -f servico-risco\pom.xml -Dtest=IdempotenciaTest test
```

Parar o ambiente:

```powershell
docker compose down
```

## Quem fez o quê

| Integrante | Contribuição nesta etapa                                                                          |
|---|---------------------------------------------------------------------------------------------------|
| Paulo Euclydes Aranha Junior | Liderança da equipe, definição do domínio de concessão de crédito, apoio na ADR e commit inicial. |
| Vinícius Eduardo Silva Oliveira | Participação ativa na decisão de domínio e escrita do ADR.                                        |
| Marcus Vinicius da Cruz Santos | Participação ativa na decisão de domínio e escrita do ADR (commit inicial do mesmo).              |
| Rafael Oliveira de Lima | Participação ativa na decisão de domínio e escrita do ADR. Criação do README.md.                  |
| Guilherme Nunes Faria |                                                                                                   |
| Hugo Fontolan Piani |                                                                                                   |
| Sesaque de Oliveira da Cruz |                                                                                                   |

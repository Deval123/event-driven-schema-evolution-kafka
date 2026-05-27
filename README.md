# Event-Driven Schema Evolution avec Kafka

> **Video 14** de la serie "Event-Driven en 5 min"
> Demo de l'evolution de schemas JSON sur Kafka : backward, forward et breaking changes.

---

## Architecture

```
                        +-------------------+
                        |    REST API        |
                        |  /api/publish/*    |
                        |  /api/demo         |
                        |  /api/results      |
                        +--------+----------+
                                 |
                        +--------v----------+
                        | OrderEventProducer |
                        |  publishV1()       |
                        |  publishV2()       |
                        |  publishV3()       |
                        |  publishBreaking() |
                        +--------+----------+
                                 |
                                 | JSON brut (String)
                                 v
                    +------------------------+
                    |   order-events (Kafka)  |
                    +-----+----------+-------+
                          |          |
              +-----------+          +-----------+
              v                                  v
    +-------------------+              +-------------------+
    |   V1 Consumer     |              |   V3 Consumer     |
    | (v1-consumer-grp) |              | (v3-consumer-grp) |
    |                   |              |                   |
    | Lit: orderId,     |              | Lit: orderId,     |
    |   customer,       |              |   customer,       |
    |   amount          |              |   amount, email,  |
    |                   |              |   priority        |
    | IGNORE champs     |              | DEFAULT champs    |
    | inconnus          |              | manquants         |
    | = FORWARD compat  |              | = BACKWARD compat |
    +-------------------+              +-------------------+
```

## Schemas JSON publies

| Version  | Champs                                          | Compatibilite             |
|----------|-------------------------------------------------|---------------------------|
| V1       | orderId, customer, amount                       | Base                      |
| V2       | V1 + email (optionnel)                          | Backward + Forward        |
| V3       | V2 + priority (optionnel)                       | Backward + Forward        |
| BREAKING | customerName (renomme!), amount supprime!        | INCOMPATIBLE              |

## Lancer le projet

### 1. Demarrer Kafka (KRaft)

```bash
docker-compose up -d
```

### 2. Lancer l'application

```bash
./mvnw spring-boot:run
```

L'application demarre sur `http://localhost:8080`.

## Tester les endpoints

### Publier un event V1

```bash
curl -X POST "http://localhost:8080/api/publish/v1?orderId=1&customer=Devalere&amount=99.99"
```

### Publier un event V2 (+ email)

```bash
curl -X POST "http://localhost:8080/api/publish/v2?orderId=2&customer=Alice&amount=149.99&email=alice@mail.com"
```

### Publier un event V3 (+ priority)

```bash
curl -X POST "http://localhost:8080/api/publish/v3?orderId=3&customer=Bob&amount=299.99&email=bob@mail.com&priority=HIGH"
```

### Publier un event BREAKING

```bash
curl -X POST "http://localhost:8080/api/publish/breaking?orderId=4&customerName=Charlie&email=charlie@mail.com&priority=LOW&totalPrice=199.99"
```

### Demo complete (4 events d'un coup)

```bash
curl -X POST http://localhost:8080/api/demo
```

### Voir les resultats (attendre 2-3 secondes apres la demo)

```bash
curl http://localhost:8080/api/results | jq
curl http://localhost:8080/api/results/v1 | jq
curl http://localhost:8080/api/results/v3 | jq
```

## Logs attendus

### V1 consumer recoit un event V2 (FORWARD compatible)

```
[CONSUMER-V1] Event recu (schema V2) : {"schemaVersion":"V2","orderId":2,...,"email":"alice@mail.com"}
[CONSUMER-V1] Traite : orderId=2, customer=Alice, amount=149.99
[CONSUMER-V1] Champs inconnus ignores (FORWARD compatible) : [email]
```

### V3 consumer recoit un event V1 (BACKWARD compatible)

```
[CONSUMER-V3] Event recu (schema V1) : {"schemaVersion":"V1","orderId":1,...}
[CONSUMER-V3] Champ 'email' absent (ancien schema) -> default: N/A
[CONSUMER-V3] Champ 'priority' absent (ancien schema) -> default: MEDIUM
[CONSUMER-V3] Traite : orderId=1, customer=Devalere, amount=99.99, email=null, priority=MEDIUM
```

### V1 consumer recoit un event BREAKING (ECHEC)

```
[CONSUMER-V1] Event recu (schema BREAKING) : {"schemaVersion":"BREAKING","orderId":4,"customerName":"Charlie",...}
[CONSUMER-V1] BREAKING CHANGE ! Champs manquants dans l'event BREAKING.
[CONSUMER-V1]   orderId=4, customer=null, amount=null
[CONSUMER-V1]   Le producer a supprime ou renomme un champ obligatoire !
```

## Concepts demontres

1. **Forward Compatibility** : V1 consumer ignore les champs qu'il ne connait pas (email, priority)
2. **Backward Compatibility** : V3 consumer utilise des valeurs par defaut pour les champs absents
3. **Breaking Change** : Renommer ou supprimer un champ casse tous les consumers existants
4. **Regle d'or** : Toujours AJOUTER des champs optionnels, ne jamais supprimer/renommer

## Stack technique

- Java 17
- Spring Boot 3.2.5
- Spring Kafka
- Jackson (JSON brut, pas d'Avro/Protobuf)
- Kafka KRaft (sans Zookeeper)
- Docker Compose

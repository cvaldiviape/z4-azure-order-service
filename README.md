# Order service

Servicio responsable de crear pedidos y orquestar la Saga de compra.

## Configuración local

| Propiedad | Valor predeterminado |
|---|---|
| Puerto HTTP | `8082` |
| PostgreSQL | `localhost:5434/orders_db` |
| Kafka | `localhost:9092` |
| Consumer group ID | `purchase-saga-consumer-group-id` |

`localhost:5434` corresponde al puerto publicado por `orders-db`. Dentro del contenedor PostgreSQL continúa utilizando `5432`.

## Kafka

Produce eventos en:

```text
orders-events-topic
inventory-events-topic
payments-events-topic
```

`PurchaseSagaEventConsumer` permanece a la escucha de:

```text
inventory-events-topic
payments-events-topic
```

El consumer group ID identifica el progreso de las instancias consumidoras; no es un topic.

## Variables disponibles

```text
ORDERS_DB_URL
ORDERS_DB_USERNAME
ORDERS_DB_PASSWORD
KAFKA_BOOTSTRAP_SERVERS
JWT_SECRET
SPRING_PROFILES_ACTIVE
```

## Ejecutar y depurar

Desde la raíz del proyecto:

```bash
docker compose -f infra/docker-compose.yml up -d kafka orders-db
```

Después ejecuta `OrderServiceApplication` con **Debug** en IntelliJ.

Para compilar sin ejecutar pruebas:

```bash
./mvnw clean package -DskipTests
```

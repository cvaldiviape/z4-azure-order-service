# Order service

Servicio responsable de crear pedidos y orquestar la Saga de compra.

## Configuración local

| Propiedad | Valor predeterminado |
|---|---|
| Puerto HTTP | `8082` |
| PostgreSQL | `localhost:5434/orders_db` |
| Kafka | `localhost:9092` |
| Consumer group ID | `purchase-saga-consumer-group-id` |

`localhost:5434` corresponde al puerto publicado por `orders-postgres`. Dentro del contenedor PostgreSQL continúa utilizando `5432`.

Flyway crea y modifica el esquema mediante `db/migration`. Hibernate utiliza `ddl-auto: validate` únicamente para comprobar que las entidades coincidan con las tablas; no crea ni altera la estructura.

## Kafka

Publica comandos y eventos en:

```text
orders-events-topic
inventory-commands-topic
payments-commands-topic
```

`PurchaseSagaEventConsumer` permanece a la escucha de:

```text
inventory-events-topic
payments-events-topic
```

Los topics `*-commands-topic` transportan solicitudes dirigidas a un servicio. Los topics `*-events-topic` transportan resultados que ya ocurrieron. Esta separación evita que un servicio consuma sus propias respuestas.

El consumer group ID identifica el progreso de las instancias consumidoras; no es un topic.

## Historial de la Saga

`purchase_sagas` conserva el estado actual de una Saga de compra y mantiene una sola fila por pedido. `purchase_saga_histories` funciona como una bitácora append-only: agrega una fila por cada transición y no modifica las transiciones anteriores.

Cada fila registra:

- El identificador de la Saga y del pedido.
- El estado de la orden después de procesar el evento.
- El estado de la Saga después de procesar el evento.
- El tipo y el identificador único del evento.
- El mensaje de error, cuando corresponda.
- La fecha y hora de la transición.

Para consultar cronológicamente el recorrido de una orden desde PostgreSQL o pgAdmin:

```sql
SELECT
    event_type,
    order_status,
    saga_status,
    event_id,
    error_message,
    created_at
FROM purchase_saga_histories
WHERE order_id = 1
ORDER BY created_at, id;
```

Flyway crea esta tabla mediante `V5__create_purchase_saga_histories.sql`.

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
docker compose -f infra/docker-compose.yml up -d kafka orders-postgres
```

Después ejecuta `OrderServiceApplication` con **Debug** en IntelliJ.

Para compilar sin ejecutar pruebas:

```bash
./mvnw clean package -DskipTests
```

## Errores de validación

`GlobalExceptionHandler` transforma los errores generados por Spring Validation al contrato común `ResponseDto`. El campo `data` contiene un mapa con el nombre de cada campo inválido y su mensaje correspondiente.

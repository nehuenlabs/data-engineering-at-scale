# Guía de Ejercicios — Cap.09: Kafka — El Sistema Nervioso Distribuido

> Kafka no es una cola de mensajes.
>
> Una cola de mensajes procesa mensajes y los descarta.
> Kafka los retiene. Un consumidor puede leer un mensaje de hace
> tres días, y otro puede leerlo de nuevo mañana.
>
> Esa diferencia cambia completamente qué problemas puede resolver:
> no es solo un canal de comunicación — es un log distribuido
> que toda la arquitectura puede leer.

---

## El modelo mental: el log como fuente de verdad

```
Cola de mensajes tradicional (RabbitMQ, ActiveMQ):
  Producer → [mensaje] → Queue → Consumer → [mensaje eliminado]
  
  El mensaje existe solo mientras está siendo procesado.
  Si el consumer falla: el mensaje se reencola o se pierde.
  Si quieres reprocesar: imposible (ya no existe).

Kafka (log distribuido):
  Producer → [mensaje] → Partition Log → Consumer A (offset 42)
                                       → Consumer B (offset 38)
                                       → Consumer C (offset 50)
  
  El mensaje existe en el log durante la retención configurada (default: 7 días).
  Cada consumer lleva su propio offset: puede leer a su propio ritmo.
  Si falla un consumer: retoma desde su último offset.
  Si necesitas reprocesar: resetear el offset al inicio.
```

```
El log de Kafka (una partición):

offset: [0][1][2][3][4][5][6][7][8][9]...
         ↑                           ↑
         inicio (más antiguo)        head (más nuevo)
         
Consumer A: ha leído hasta offset 7 → próximo: 8
Consumer B: ha leído hasta offset 4 → próximo: 5 (procesando más lento)
Consumer C: re-leyendo desde offset 0 (reprocesamiento)

Los tres consumers son independientes. Kafka no sabe ni le importa
en qué offset está cada consumer — eso es responsabilidad del consumer.
```

---

## Por qué Kafka es el centro de las arquitecturas modernas

```
Sin Kafka:
  Sistema A → (HTTP directo) → Sistema B
  Sistema A → (HTTP directo) → Sistema C
  Sistema A → (HTTP directo) → Sistema D
  
  Problemas:
  - Si B está caído, A falla (acoplamiento temporal)
  - Si A genera 10k eventos/s y B solo procesa 1k/s: backpressure o pérdida
  - Si se añade Sistema E: hay que modificar A
  - Si B necesita reprocesar: A debe reenviar (¿tiene historial?)

Con Kafka:
  Sistema A → Kafka → Sistema B (consumer independiente)
                    → Sistema C (consumer independiente)
                    → Sistema D (consumer independiente)
  
  Beneficios:
  - B puede estar caído: los mensajes esperan en Kafka
  - B puede procesar a su ritmo (consumer lag gestionado)
  - Añadir Sistema E: sin tocar A (solo suscribirse al topic)
  - Reprocesar: resetear el offset (los mensajes están en el log)
```

---

## Tabla de contenidos

- [Sección 9.1 — Arquitectura: topics, particiones, y offsets](#sección-91--arquitectura-topics-particiones-y-offsets)
- [Sección 9.2 — Producers: escribir datos eficientemente](#sección-92--producers-escribir-datos-eficientemente)
- [Sección 9.3 — Consumers: leer datos con garantías](#sección-93--consumers-leer-datos-con-garantías)
- [Sección 9.4 — Consumer groups: paralelismo en la lectura](#sección-94--consumer-groups-paralelismo-en-la-lectura)
- [Sección 9.5 — Garantías de entrega: at-most-once, at-least-once, exactly-once](#sección-95--garantías-de-entrega-at-most-once-at-least-once-exactly-once)
- [Sección 9.6 — Kafka Streams: procesamiento en el broker](#sección-96--kafka-streams-procesamiento-en-el-broker)
- [Sección 9.7 — Operación: monitoreo y diagnóstico](#sección-97--operación-monitoreo-y-diagnóstico)

---

## Sección 9.1 — Arquitectura: Topics, Particiones, y Offsets

### Ejercicio 9.1.1 — Leer: la anatomía de un topic

**Tipo: Leer/analizar**

Un topic en Kafka es una categoría lógica de mensajes. Internamente
está dividido en particiones — y las particiones son donde ocurre
todo el trabajo real:

```
Topic "transacciones" con 4 particiones:

Partición 0: [msg_0][msg_4][msg_8][msg_12]...
Partición 1: [msg_1][msg_5][msg_9][msg_13]...
Partición 2: [msg_2][msg_6][msg_10][msg_14]...
Partición 3: [msg_3][msg_7][msg_11][msg_15]...

Cada mensaje tiene:
  - Offset: posición dentro de su partición (único por partición, no global)
  - Key: determina en qué partición va (hash(key) % num_particiones)
  - Value: el contenido del mensaje
  - Timestamp: cuándo fue producido
  - Headers: metadatos opcionales
```

```python
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

# Crear un topic con 4 particiones y factor de replicación 3:
admin = AdminClient({"bootstrap.servers": "localhost:9092"})
topic = NewTopic(
    topic="transacciones",
    num_partitions=4,
    replication_factor=3,
    config={
        "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 días
        "cleanup.policy": "delete",                       # borrar mensajes viejos
        "compression.type": "zstd",                       # comprimir en el broker
        "min.insync.replicas": "2",                       # para durabilidad
    }
)
admin.create_topics([topic])
```

**Preguntas:**

1. Si un topic tiene 4 particiones y envías un mensaje con `key="user_123"`,
   ¿en qué partición va? ¿Qué fórmula usa Kafka?

2. ¿Por qué el offset es único dentro de una partición pero no globalmente?
   ¿Qué implica eso para ordenar mensajes de distintas particiones?

3. Si tienes 4 particiones y 6 consumers en el mismo consumer group,
   ¿cuántos consumers están activos? ¿Qué pasa con los 2 extras?

4. ¿Qué es el factor de replicación y por qué `min.insync.replicas=2`
   con 3 réplicas es la configuración estándar de producción?

5. ¿Kafka garantiza ordenamiento global de mensajes? ¿Dentro de una partición?

**Pista:** El hash de la key se calcula con `murmur2(key) % num_partitions`.
Kafka garantiza orden **dentro de una partición** — todos los mensajes con la misma
key van a la misma partición, por lo tanto están ordenados entre sí.
Para `user_123`, todos sus mensajes llegan en orden a la partición asignada.
El orden global entre particiones no está garantizado — los consumers pueden
recibir mensajes de distintas particiones en cualquier orden.

---

### Ejercicio 9.1.2 — Crear y explorar un topic

```python
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from confluent_kafka import TopicPartition

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Crear topic:
admin.create_topics([
    NewTopic("clicks", num_partitions=6, replication_factor=1),  # dev
    NewTopic("compras", num_partitions=3, replication_factor=1),
])

# Inspeccionar el topic:
metadata = admin.list_topics(topic="clicks", timeout=10)
topic_meta = metadata.topics["clicks"]

print(f"Topic: clicks")
print(f"Particiones: {len(topic_meta.partitions)}")
for pid, p in topic_meta.partitions.items():
    print(f"  Partición {pid}:")
    print(f"    Leader: broker {p.leader}")
    print(f"    Replicas: {p.replicas}")
    print(f"    ISR (In-Sync Replicas): {p.isrs}")

# Obtener los offsets actuales (watermarks):
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "inspector",
    "auto.offset.reset": "earliest",
})

for pid in range(6):
    tp = TopicPartition("clicks", pid)
    low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
    print(f"Partición {pid}: offsets [{low}, {high}] — {high - low} mensajes")

consumer.close()
```

**Restricciones:**
1. Crear los dos topics y verificar que existen
2. Producir 1000 mensajes al topic "clicks" y verificar la distribución
   entre particiones (¿son uniformes sin key?)
3. Producir 1000 mensajes con keys de 10 usuarios distintos y observar
   si los mensajes de cada usuario van siempre a la misma partición

---

### Ejercicio 9.1.3 — Particionamiento: diseñar la estrategia correcta

El número de particiones determina el paralelismo máximo de consumers.
Es una decisión difícil de cambiar después — requiere reparticionamiento.

```python
# Estrategias de particionamiento:

# 1. Sin key (round-robin): distribución uniforme, sin orden por clave
producer.produce("clicks", value=mensaje)  # Kafka elige partición

# 2. Con key (hash): orden garantizado por clave, distribución variable
producer.produce("clicks", key="user_123", value=mensaje)

# 3. Partición explícita: control total, raramente necesario
producer.produce("clicks", partition=2, value=mensaje)

# 4. Partitioner personalizado:
class RegionPartitioner:
    """Asigna mensajes a particiones según la región del usuario."""
    REGIONES = {"norte": 0, "sur": 1, "este": 2, "oeste": 3}
    
    def __call__(self, topic, key, value, num_partitions):
        try:
            datos = json.loads(value)
            region = datos.get("region", "norte")
            return self.REGIONES.get(region, 0)
        except Exception:
            return 0  # fallback a partición 0
```

**Preguntas:**

1. Un sistema de detección de fraude necesita ver todos los eventos
   de un usuario en orden. ¿Qué estrategia de particionamiento usas?

2. Un pipeline de métricas agrega por hora (no le importa el orden
   por usuario). ¿Qué estrategia es más eficiente?

3. Si tienes 4 regiones geográficas y usas el `RegionPartitioner`,
   ¿qué problema ocurre si una región genera el 80% del tráfico?

4. ¿Cuántas particiones debería tener un topic para soportar
   20 consumers paralelos con headroom para crecer a 50?

5. Un topic tiene 10 particiones pero solo 3 brokers.
   ¿Cómo distribuye Kafka las particiones entre brokers?

**Pista:** El problema del `RegionPartitioner` con una región dominante es data skew:
la partición de "norte" recibe 80% de los mensajes, los consumers asignados
a esa partición están saturados mientras los de las otras están ociosos.
La solución: añadir un sub-bucket aleatorio a la key: `key = f"norte_{random.randint(0,3)}"`
y tener 4 particiones por región (16 particiones totales). Esto balancea la carga
pero pierde el orden global dentro de una región — hay que evaluar el tradeoff.

---

### Ejercicio 9.1.4 — Retención y compactación: no todos los topics son iguales

```python
from confluent_kafka.admin import AdminClient, ConfigResource

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Topic de eventos (retención por tiempo — delete policy):
# Los mensajes se borran después de 7 días, independientemente de si
# fueron consumidos o no.
config_eventos = {
    "cleanup.policy": "delete",
    "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 días
    "retention.bytes": str(10 * 1024 * 1024 * 1024), # 10 GB por partición
}

# Topic de estado (compactación — compact policy):
# Solo se retiene el último valor para cada key.
# Útil para: catálogos de productos, perfiles de usuario, configuración.
config_estado = {
    "cleanup.policy": "compact",
    "min.cleanable.dirty.ratio": "0.5",
    "segment.ms": str(24 * 60 * 60 * 1000),  # segmentos de 1 día
}

# Topic híbrido (retención + compactación):
config_hibrido = {
    "cleanup.policy": "compact,delete",
    "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 días
}
```

```
Topic con compactación — ejemplo de catálogo de productos:

Mensajes en orden (más antiguo → más nuevo):
  [key=prod_1, value={"precio": 100}]
  [key=prod_2, value={"precio": 200}]
  [key=prod_1, value={"precio": 90}]   ← actualización
  [key=prod_3, value={"precio": 50}]
  [key=prod_1, value={"precio": 85}]   ← otra actualización
  [key=prod_2, value=null]              ← tombstone: eliminar prod_2

Después de compactación:
  [key=prod_1, value={"precio": 85}]   ← solo el último valor
  [key=prod_3, value={"precio": 50}]
  # prod_2 eliminado (tombstone)
```

**Preguntas:**

1. ¿Para cuál de estos casos usarías `cleanup.policy=compact`?
   ¿Y `delete`?
   - Registro de auditoría de acciones de usuarios
   - Estado actual del carrito de compras de cada usuario
   - Stream de clicks en tiempo real
   - Precios actuales de productos
   - Historial de cambios de precio

2. ¿Qué es un "tombstone" y cuándo lo produces?

3. Si un consumer arranca por primera vez en un topic compactado,
   ¿cuál es el estado que recibe?

4. ¿La compactación garantiza que el consumer ve el último valor
   inmediatamente? ¿O puede haber un delay?

**Pista:** El consumer de un topic compactado que arranca desde el inicio
(`auto.offset.reset=earliest`) recibe el estado completo actual del sistema
— exactamente como un snapshot. Esto permite reconstruir el estado local
de la aplicación al arrancar, sin necesitar una base de datos externa.
Sin embargo, la compactación no es instantánea — el broker compacta en
background, por lo que durante un período breve puede haber múltiples valores
para la misma key. El consumer debe manejar esto (la última actualización
de una key reemplaza a las anteriores).

---

### Ejercicio 9.1.5 — Leer: diseñar la topología de topics para el sistema de e-commerce

**Tipo: Diseñar**

Para el sistema de e-commerce del repositorio, diseñar la arquitectura de topics:

```
Eventos del sistema:
  - Clicks en la web (100K/s en horario pico)
  - Búsquedas de productos (50K/s)
  - Adiciones al carrito (5K/s)
  - Compras completadas (1K/s)
  - Devoluciones (100/s)
  - Actualizaciones de catálogo de productos (10/s)
  - Cambios de estado de pedidos (500/s)

Consumers:
  - Pipeline de analytics en Spark (batch, cada hora)
  - Motor de recomendaciones (streaming, latencia < 1s)
  - Sistema de detección de fraude (streaming, latencia < 100ms)
  - Dashboard de métricas en tiempo real (streaming, latencia < 5s)
  - Pipeline de ML para personalización (batch, cada 6 horas)
```

Para cada tipo de evento, especificar:
1. Nombre del topic y número de particiones
2. Política de retención (delete, compact, o híbrido) y duración
3. Estrategia de particionamiento (sin key, por user_id, por producto_id, etc.)
4. Factor de replicación

Justificar cada decisión en términos de los requisitos de los consumers.

---

## Sección 9.2 — Producers: Escribir Datos Eficientemente

### Ejercicio 9.2.1 — El ciclo de vida de un mensaje

```python
from confluent_kafka import Producer
import json
import time

conf = {
    "bootstrap.servers": "localhost:9092",
    # Configuración de batching:
    "batch.size": 65536,          # 64 KB: acumular hasta este tamaño
    "linger.ms": 10,              # esperar hasta 10ms para llenar el batch
    # Configuración de compresión:
    "compression.type": "zstd",   # comprimir el batch
    # Configuración de durabilidad:
    "acks": "all",                # esperar confirmación de todas las réplicas ISR
    "retries": 5,                 # reintentar en caso de error
    "retry.backoff.ms": 100,
    # Configuración de throughput:
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes": 1048576,  # 1 GB de buffer en el producer
}

producer = Producer(conf)

def callback_entrega(error, mensaje):
    """Llamado cuando Kafka confirma o rechaza un mensaje."""
    if error:
        print(f"Error: {error}")
    else:
        print(f"Entregado: topic={mensaje.topic()}, "
              f"partición={mensaje.partition()}, "
              f"offset={mensaje.offset()}")

# Producir mensajes:
for i in range(1000):
    evento = {
        "id": i,
        "user_id": f"user_{i % 100}",
        "tipo": "click",
        "timestamp": int(time.time() * 1000),
        "monto": round(i * 9.99, 2),
    }
    
    producer.produce(
        topic="clicks",
        key=evento["user_id"].encode(),  # key para particionamiento
        value=json.dumps(evento).encode(),
        on_delivery=callback_entrega,
    )
    
    # poll() procesa los callbacks de entrega:
    producer.poll(0)  # non-blocking

# Esperar a que todos los mensajes sean entregados:
producer.flush(timeout=30)  # blocking hasta que el buffer esté vacío
```

**Preguntas:**

1. ¿Por qué `producer.poll(0)` es necesario dentro del loop?
   ¿Qué pasa si no lo llamas?

2. `linger.ms=10` significa que el producer espera 10ms antes de enviar
   un batch. ¿Cómo afecta esto a la latencia vs el throughput?

3. `acks="all"` es más lento que `acks=1` o `acks=0`. ¿Cuándo vale la pena?
   ¿Cuándo `acks=1` es suficiente?

4. ¿Qué pasa si el buffer del producer (`queue.buffering.max.messages`)
   se llena? ¿El `producer.produce()` bloquea o lanza una excepción?

5. Si el broker está caído y `retries=5` con `retry.backoff.ms=100`,
   ¿cuánto tiempo máximo espera el producer antes de reportar error?

**Pista:** `producer.poll(0)` es necesario para que la librería de Kafka
procese los callbacks internamente — sin llamarlo, los callbacks de `on_delivery`
no se ejecutan. El producer de Kafka en Python (confluent-kafka) es
asíncrono internamente: `produce()` añade al buffer y retorna inmediatamente.
`poll()` procesa los eventos pendientes (callbacks, liveness checks con el broker).
`flush()` llama a `poll()` en loop hasta que el buffer está vacío.

---

### Ejercicio 9.2.2 — Throughput vs latencia: configurar el producer para tu caso

```python
# Configuración para MÁXIMO THROUGHPUT:
# Sacrifica latencia por volumen.
config_throughput = {
    "bootstrap.servers": "localhost:9092",
    "batch.size": 1048576,          # 1 MB: batches grandes
    "linger.ms": 100,               # esperar 100ms para llenar el batch
    "compression.type": "lz4",      # compresión rápida
    "acks": "1",                    # solo confirma el leader (no ISR completo)
    "queue.buffering.max.kbytes": 4194304,  # 4 GB buffer
}

# Configuración para MÍNIMA LATENCIA:
# Sacrifica throughput por velocidad.
config_latencia = {
    "bootstrap.servers": "localhost:9092",
    "batch.size": 1,               # enviar inmediatamente (sin batching)
    "linger.ms": 0,                # sin espera
    "compression.type": "none",    # sin CPU de compresión
    "acks": "1",                   # confirmación rápida
}

# Configuración para MÁXIMA DURABILIDAD:
# Sacrifica throughput por garantías de no pérdida.
config_durabilidad = {
    "bootstrap.servers": "localhost:9092",
    "acks": "all",                 # confirmar todas las ISR
    "min.insync.replicas": 2,      # mínimo 2 ISR deben confirmar
    "retries": 2147483647,         # reintentar para siempre
    "enable.idempotence": True,    # exactly-once al escribir
    "max.in.flight.requests.per.connection": 5,
}
```

**Restricciones:**
1. Medir el throughput (mensajes/segundo) y la latencia P99 de cada configuración
2. Usar un dataset de 1M mensajes de 1 KB cada uno
3. ¿En qué caso la compresión `lz4` es más lenta que `none`?
   (pista: CPU bound vs I/O bound)
4. Implementar un producer que adapta su configuración según la carga actual

---

### Ejercicio 9.2.3 — Transacciones en el producer: exactly-once en escritura

```python
from confluent_kafka import Producer

# Producer transaccional: permite escribir a múltiples particiones
# de forma atómica (todo o nada).
producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "transactional.id": "pipeline-transaccional-1",  # ID único por instancia
    "enable.idempotence": True,   # requerido para transacciones
    "acks": "all",                # requerido para transacciones
})

# Inicializar las transacciones (una sola vez al arrancar):
producer.init_transactions()

def producir_evento_transaccional(eventos: list[dict]):
    """Producir múltiples eventos de forma atómica."""
    producer.begin_transaction()
    
    try:
        for evento in eventos:
            producer.produce(
                topic="clicks",
                key=evento["user_id"].encode(),
                value=json.dumps(evento).encode(),
            )
        
        # Confirmar la transacción:
        producer.commit_transaction()
        print(f"Transacción committed: {len(eventos)} eventos")
        
    except Exception as e:
        # Abortar si algo falla:
        producer.abort_transaction()
        print(f"Transacción abortada: {e}")
        raise
```

**Preguntas:**

1. ¿Para qué sirve el `transactional.id`? ¿Qué pasa si dos instancias
   del producer usan el mismo `transactional.id`?

2. ¿Las transacciones de Kafka garantizan exactly-once hacia todos
   los topics, o solo dentro de una transacción específica?

3. Si el producer falla durante `commit_transaction()` (antes de que
   el broker confirme), ¿qué ve el consumer?

4. ¿Las transacciones de Kafka tienen un impacto en la latencia?
   ¿Por qué?

5. ¿Cuándo necesitas transacciones en el producer vs simplemente
   `enable.idempotence=True`?

**Pista:** `enable.idempotence=True` garantiza que cada mensaje se escribe
exactamente una vez incluso con reintentos — elimina duplicados causados
por reintentos de red. Las transacciones añaden atomicidad: múltiples
`produce()` sobre múltiples topics se confirman o abortan juntos.
Son necesarias cuando tienes que mantener consistencia entre topics:
por ejemplo, producir un evento a "pedidos" y actualizar el estado en "inventario"
de forma atómica — si uno falla, el otro también se revierte.

---

### Ejercicio 9.2.4 — Serialización y Schema Registry

```python
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# Schema Registry: versiona y valida los schemas de los mensajes
schema_registry_client = SchemaRegistryClient({
    "url": "http://localhost:8081"
})

# Schema Avro del mensaje:
schema_str = """
{
    "namespace": "com.empresa.eventos",
    "type": "record",
    "name": "Click",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "user_id", "type": "string"},
        {"name": "producto_id", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "monto", "type": ["null", "double"], "default": null}
    ]
}
"""

# Crear el serializer con validación de schema:
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    to_dict=lambda obj, ctx: obj,  # el objeto ya es un dict
)

producer = Producer({"bootstrap.servers": "localhost:9092"})
string_serializer = StringSerializer("utf_8")

# Producir con Avro:
click = {
    "id": 12345,
    "user_id": "user_abc",
    "producto_id": "prod_xyz",
    "timestamp": int(time.time() * 1000),
    "monto": 99.99,
}

producer.produce(
    topic="clicks-avro",
    key=string_serializer("user_abc"),
    value=avro_serializer(
        click,
        SerializationContext("clicks-avro", MessageField.VALUE)
    ),
)
producer.flush()
```

**Preguntas:**

1. ¿Qué ventaja tiene Avro + Schema Registry sobre JSON para mensajes de Kafka?

2. Si el producer produce con schema v1 y el consumer espera schema v2,
   ¿qué pasa? ¿Cómo detectas la incompatibilidad antes de que llegue a producción?

3. ¿El Schema Registry es un componente crítico de la infraestructura?
   ¿Qué pasa si está caído?

4. ¿Cuánto espacio extra ocupa Avro vs JSON para el mismo mensaje?

**Pista:** Avro binario es típicamente 3-5× más compacto que JSON para el mismo
dato porque no incluye los nombres de campos en cada mensaje — solo los datos.
Los nombres de campos están en el schema (referenciado por ID en el encabezado
del mensaje). Para un mensaje de "click" con 6 campos, JSON ocupa ~200 bytes
y Avro ~40 bytes. A 100K mensajes/segundo, esa diferencia es 16 GB/día vs 3.4 GB/día.

---

### Ejercicio 9.2.5 — Leer: el producer que perdía mensajes silenciosamente

**Tipo: Diagnosticar**

Un sistema de notificaciones envía mensajes a Kafka y mide el número
de eventos producidos. Los logs muestran 100,000 eventos producidos por hora,
pero el consumer solo recibe 87,000. No hay errores en los logs del producer.

```python
# El código del producer:
producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "0",               # ← fire and forget
})

for evento in eventos:
    producer.produce("notificaciones", value=json.dumps(evento).encode())
    # Sin producer.poll() ni producer.flush()
    # Sin callback de on_delivery
```

**Preguntas:**

1. ¿Por qué `acks=0` puede causar pérdida de mensajes silenciosa?

2. ¿Por qué la ausencia de `producer.poll()` y `producer.flush()`
   también contribuye a la pérdida?

3. ¿Por qué no hay errores en los logs si se están perdiendo mensajes?

4. ¿Cómo rediseñarías el producer para detectar la pérdida?

5. ¿`acks=0` tiene algún caso de uso válido?

**Pista:** Con `acks=0`, el producer envía el mensaje y no espera confirmación
del broker — ni siquiera sabe si el broker lo recibió. Si el broker rechaza
el mensaje (buffer lleno, por ejemplo), el producer no recibe el error y
el callback `on_delivery` nunca se llama. Sin `flush()` al cerrar el proceso,
los mensajes en el buffer interno del producer se descartan cuando el proceso
termina. Caso de uso válido para `acks=0`: métricas de alta frecuencia donde
perder el 0.1% de los puntos de datos es aceptable y la latencia es crítica
(ej: logs de debugging en producción con volumen extremo).

---

## Sección 9.3 — Consumers: Leer Datos con Garantías

### Ejercicio 9.3.1 — El ciclo de vida de un consumer

```python
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "procesador-clicks",
    "auto.offset.reset": "earliest",  # si es la primera vez, empezar desde el inicio
    "enable.auto.commit": False,       # IMPORTANTE: commit manual para control exacto
    "max.poll.interval.ms": 300000,    # 5 minutos máximo entre polls
    "session.timeout.ms": 30000,       # 30 segundos para detectar consumer caído
    "fetch.min.bytes": 1024,           # esperar hasta 1 KB antes de retornar
    "fetch.max.wait.ms": 500,          # o hasta 500ms
}

consumer = Consumer(conf)
consumer.subscribe(["clicks"])

try:
    while True:
        # poll() retorna hasta max_records mensajes:
        mensaje = consumer.poll(timeout=1.0)  # esperar 1 segundo max
        
        if mensaje is None:
            continue  # timeout — no hay mensajes nuevos
        
        if mensaje.error():
            if mensaje.error().code() == KafkaError._PARTITION_EOF:
                # Llegamos al final de la partición (no es error — solo info)
                print(f"Fin de partición {mensaje.partition()}")
            else:
                raise Exception(f"Error: {mensaje.error()}")
            continue
        
        # Procesar el mensaje:
        try:
            datos = json.loads(mensaje.value().decode())
            procesar(datos)
            
            # Commit después de procesar exitosamente:
            consumer.commit(mensaje)  # commit sincrónico
            # O asíncrono (más rápido, pero el error no se propaga aquí):
            # consumer.commit(mensaje, asynchronous=True)
            
        except Exception as e:
            print(f"Error procesando mensaje: {e}")
            # El mensaje NO se commitea → se reprocesará al reiniciar
            # Implementar dead letter queue si el error es persistente

finally:
    consumer.close()  # commit final de offsets pendientes
```

**Preguntas:**

1. ¿Por qué `enable.auto.commit=False` es la configuración recomendada
   para pipelines que requieren exactly-once processing?

2. ¿Qué pasa con los mensajes no commiteados si el consumer falla?
   ¿Los vuelve a ver el mismo consumer o un consumer diferente del grupo?

3. ¿`consumer.commit(mensaje)` committea solo ese mensaje o todos
   los mensajes hasta ese offset?

4. ¿Qué es el `max.poll.interval.ms` y qué pasa si el procesamiento
   de un mensaje tarda más que ese tiempo?

5. ¿Por qué `consumer.close()` es importante y no simplemente
   terminar el proceso?

**Pista:** `consumer.commit(mensaje)` committea el offset del mensaje + 1
a Kafka — significando "he procesado hasta este offset, el próximo es offset+1".
No committea un mensaje específico sino que avanza el puntero de la partición.
Si el consumer falla sin commitear, el próximo consumer del grupo (o el mismo
al reiniciar) leerá desde el último offset commiteado, potencialmente reprocesando
algunos mensajes — esto es at-least-once. Para exactly-once, necesitas idempotencia
en el sink (el sistema de destino).

---

### Ejercicio 9.3.2 — Gestión manual de offsets: seeking y reset

```python
from confluent_kafka import Consumer, TopicPartition
from datetime import datetime

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "reprocesador",
    "enable.auto.commit": False,
})

# Asignar particiones manualmente (sin subscribe automático):
particiones = [TopicPartition("clicks", 0), TopicPartition("clicks", 1)]
consumer.assign(particiones)

# Mover a un offset específico:
consumer.seek(TopicPartition("clicks", 0, offset=1000))

# Mover al inicio de la partición:
consumer.seek_to_beginning(particiones)

# Mover al final (solo procesar mensajes nuevos):
consumer.seek_to_end(particiones)

# Buscar el offset correspondiente a un timestamp (buscar desde las 09:00 de hoy):
hoy_9am = int(datetime.now().replace(hour=9, minute=0, second=0, microsecond=0).timestamp() * 1000)
offsets_por_tiempo = consumer.offsets_for_times([
    TopicPartition("clicks", 0, hoy_9am),
    TopicPartition("clicks", 1, hoy_9am),
])
for tp in offsets_por_tiempo:
    if tp.offset >= 0:  # offset=-1 si no hay mensajes después del timestamp
        consumer.seek(tp)
        print(f"Partición {tp.partition}: offset={tp.offset} para las 09:00")
```

**Restricciones:**
1. Implementar un script de reprocesamiento: leer las últimas 24 horas
   de un topic y reenviarlos a un topic diferente
2. Medir cuánto tiempo tarda reprocesar 10M mensajes con este enfoque
3. ¿Cómo pausas el consumer temporalmente sin cerrar la conexión?

---

### Ejercicio 9.3.3 — Dead Letter Queue: manejar mensajes que no se pueden procesar

```python
from confluent_kafka import Consumer, Producer
import json
import logging

logger = logging.getLogger(__name__)

class ConsumerConDLQ:
    """
    Consumer que envía mensajes fallidos a una Dead Letter Queue (DLQ).
    
    Patrón estándar en producción: en lugar de fallar o ignorar,
    enviar a un topic separado para revisión manual.
    """
    
    def __init__(self, topic_entrada: str, topic_dlq: str, max_reintentos: int = 3):
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": f"consumer-{topic_entrada}",
            "enable.auto.commit": False,
        })
        self.producer_dlq = Producer({"bootstrap.servers": "localhost:9092"})
        self.topic_dlq = topic_dlq
        self.max_reintentos = max_reintentos
        self.consumer.subscribe([topic_entrada])
    
    def procesar(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            
            exito = False
            for intento in range(self.max_reintentos):
                try:
                    datos = json.loads(msg.value().decode())
                    self._procesar_mensaje(datos)
                    exito = True
                    break
                except Exception as e:
                    logger.warning(f"Intento {intento+1} fallido: {e}")
                    if intento < self.max_reintentos - 1:
                        time.sleep(0.1 * (2 ** intento))  # backoff exponencial
            
            if exito:
                self.consumer.commit(msg)
            else:
                # Enviar a DLQ con metadatos del error:
                self._enviar_a_dlq(msg, "max_reintentos_alcanzados")
                self.consumer.commit(msg)  # también commitear los fallidos
    
    def _enviar_a_dlq(self, msg, razon: str):
        """Enviar mensaje fallido a DLQ con metadatos."""
        dlq_mensaje = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "razon_fallo": razon,
            "timestamp_fallo": int(time.time() * 1000),
            "payload_original": msg.value().decode(),
        }
        self.producer_dlq.produce(
            topic=self.topic_dlq,
            key=msg.key(),
            value=json.dumps(dlq_mensaje).encode(),
        )
        self.producer_dlq.poll(0)
    
    def _procesar_mensaje(self, datos: dict):
        """Implementar la lógica real de procesamiento."""
        raise NotImplementedError
```

**Restricciones:**
1. Implementar `_procesar_mensaje` con lógica de negocio real
2. Añadir métricas: tasa de éxito, tasa de DLQ, latencia de procesamiento
3. Implementar un "DLQ processor" que reintenta los mensajes de la DLQ
   automáticamente después de un tiempo configurable
4. ¿Cuándo es correcto NO usar DLQ (mejor fallar el proceso completo)?

**Pista:** La DLQ es útil cuando el error es probablemente temporal o corregible
(datos malformados que un humano puede arreglar) y no quieres bloquear el procesamiento
de otros mensajes. Mejor fallar el proceso completo (sin DLQ) cuando el error
indica un problema sistémico: bug en el código que afectaría a todos los mensajes,
base de datos de destino caída, o cuando los mensajes son críticos y no puedes
tolerar que ninguno sea silenciado.

---

### Ejercicio 9.3.4 — Batch consumption: procesar mensajes en lotes

```python
from confluent_kafka import Consumer
import json
from typing import Callable

class BatchConsumer:
    """
    Consumer que acumula mensajes en batches antes de procesarlos.
    Más eficiente que procesar uno a uno cuando el procesamiento tiene
    overhead por llamada (ej: inserción en base de datos).
    """
    
    def __init__(
        self,
        topic: str,
        group_id: str,
        batch_size: int = 100,
        batch_timeout_ms: float = 5000,
    ):
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "enable.auto.commit": False,
            # Configurar fetch para soportar batches:
            "fetch.min.bytes": 65536,        # 64 KB mínimo
            "fetch.max.wait.ms": batch_timeout_ms,
        })
        self.consumer.subscribe([topic])
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
    
    def consumir_en_batches(self, procesar_batch: Callable[[list], None]):
        """
        Acumular mensajes y llamar a procesar_batch cuando:
        - El batch llega a batch_size mensajes, o
        - Han pasado batch_timeout_ms milisegundos
        """
        batch = []
        ultimo_mensaje = None
        inicio_batch = time.perf_counter()
        
        while True:
            timeout_restante = (self.batch_timeout_ms / 1000) - (
                time.perf_counter() - inicio_batch
            )
            
            if timeout_restante <= 0 or len(batch) >= self.batch_size:
                # Procesar el batch acumulado:
                if batch:
                    procesar_batch(batch)
                    self.consumer.commit(ultimo_mensaje)
                batch = []
                inicio_batch = time.perf_counter()
                continue
            
            msg = self.consumer.poll(timeout=min(timeout_restante, 1.0))
            if msg is None or msg.error():
                continue
            
            try:
                datos = json.loads(msg.value().decode())
                batch.append(datos)
                ultimo_mensaje = msg
            except Exception as e:
                print(f"Error deserializando: {e}")

# Uso: insertar en PostgreSQL en batches de 500:
consumer = BatchConsumer("clicks", "pg-inserter", batch_size=500, batch_timeout_ms=2000)

def insertar_en_postgres(batch: list[dict]):
    with pg_conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO clicks VALUES (%(id)s, %(user_id)s, %(timestamp)s)",
            batch
        )
    pg_conn.commit()

consumer.consumir_en_batches(insertar_en_postgres)
```

**Restricciones:**
1. Implementar `BatchConsumer` completo con manejo de errores
2. Medir el throughput (mensajes/segundo) de batch vs uno a uno
   para una inserción en PostgreSQL
3. ¿Cuál es el riesgo de hacer `commit(ultimo_mensaje)` para todo el batch?
   ¿Qué pasa si el procesamiento del batch falla a la mitad?

---

### Ejercicio 9.3.5 — Leer: elegir entre auto.commit y commit manual

**Tipo: Analizar**

Para cada caso de uso, recomendar `enable.auto.commit=True` o `False`:

```
Caso 1: Consumer que lee métricas de temperatura de sensores IoT
  y las guarda en un dashboard en tiempo real.
  Si se reprocesa, el dashboard simplemente muestra datos duplicados
  brevemente — no es un problema.

Caso 2: Consumer que lee órdenes de compra y descuenta el inventario
  en la base de datos. Si se reprocesa, se descuenta el inventario dos veces.

Caso 3: Consumer que agrega clicks por usuario y los envía a otro topic.
  Si se reprocesa, habrá duplicados en el topic destino.

Caso 4: Consumer que genera notificaciones por email para usuarios.
  Si se reprocesa, el usuario recibe el mismo email dos veces.

Caso 5: Consumer que actualiza el perfil de usuario con el último evento.
  La operación de update es idempotente (escribir el mismo valor dos veces
  produce el mismo estado).
```

---

## Sección 9.4 — Consumer Groups: Paralelismo en la Lectura

### Ejercicio 9.4.1 — La asignación de particiones a consumers

```python
from confluent_kafka import Consumer

# Topic con 6 particiones:
# P0, P1, P2, P3, P4, P5

# Grupo con 2 consumers:
# Consumer A → P0, P1, P2
# Consumer B → P3, P4, P5

# Grupo con 6 consumers:
# Consumer A → P0
# Consumer B → P1
# Consumer C → P2
# Consumer D → P3
# Consumer E → P4
# Consumer F → P5

# Grupo con 8 consumers (MÁS consumers que particiones):
# Consumer A → P0
# Consumer B → P1
# Consumer C → P2
# Consumer D → P3
# Consumer E → P4
# Consumer F → P5
# Consumer G → (ninguna partición — ocioso)
# Consumer H → (ninguna partición — ocioso)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "mi-grupo",
    "partition.assignment.strategy": "cooperative-sticky",  # Kafka 2.4+
    # cooperative-sticky: minimiza el movimiento de particiones en rebalances
})

def en_asignacion(consumer, particiones):
    print(f"Particiones asignadas: {[p.partition for p in particiones]}")

def en_revocacion(consumer, particiones):
    print(f"Particiones revocadas: {[p.partition for p in particiones]}")

consumer.subscribe(
    ["clicks"],
    on_assign=en_asignacion,
    on_revoke=en_revocacion,
)
```

**Preguntas:**

1. ¿Qué es un "rebalance" y cuándo ocurre?

2. ¿Qué diferencia hay entre `range`, `roundrobin`, y `cooperative-sticky`
   como estrategias de asignación?

3. Durante un rebalance con la estrategia `cooperative-sticky`,
   ¿los consumers que no pierden particiones continúan consumiendo?

4. ¿Cómo implementarías un consumer group con 6 consumers
   para un topic de 6 particiones usando Docker Compose?

5. Si un consumer está consumiendo muy lento (consumer lag creciente),
   ¿el rebalance lo resuelve automáticamente?

**Pista:** El consumer lag creciente no se resuelve con un rebalance — el rebalance
redistribuye particiones pero no cambia la velocidad de procesamiento.
Si un consumer es lento, sus particiones asignadas seguirán con lag incluso
después del rebalance. La solución real: aumentar el número de consumers
(hasta el límite de particiones) o aumentar la velocidad de procesamiento
del consumer individual (batch processing, procesamiento paralelo interno).

---

### Ejercicio 9.4.2 — Consumer lag: la métrica más importante de Kafka

```python
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition

def calcular_consumer_lag(
    group_id: str,
    topic: str,
    bootstrap_servers: str = "localhost:9092",
) -> dict:
    """
    Calcula el consumer lag (mensajes no consumidos) por partición.
    
    Consumer lag = high watermark - committed offset
    
    Si el lag crece: el consumer no puede mantener el ritmo del producer.
    Si el lag es 0: el consumer está al día.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "__lag-checker-tmp",
    })
    
    # Obtener el offset commiteado por el grupo:
    topic_partitions = []
    metadata = admin.list_topics(topic=topic)
    for pid in metadata.topics[topic].partitions:
        topic_partitions.append(TopicPartition(topic, pid))
    
    # Offset commiteado (dónde está el consumer):
    committed = consumer.committed(topic_partitions, timeout=5.0)
    
    # High watermark (dónde está el producer):
    lags = {}
    for tp in committed:
        low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
        committed_offset = tp.offset if tp.offset >= 0 else low
        lag = high - committed_offset
        lags[tp.partition] = {
            "high_watermark": high,
            "committed_offset": committed_offset,
            "lag": lag,
        }
    
    consumer.close()
    return {
        "group_id": group_id,
        "topic": topic,
        "total_lag": sum(p["lag"] for p in lags.values()),
        "por_particion": lags,
    }

# Monitoreo continuo:
import time

while True:
    lag = calcular_consumer_lag("procesador-clicks", "clicks")
    print(f"Total lag: {lag['total_lag']:,} mensajes")
    
    if lag["total_lag"] > 100_000:
        print("⚠ ALERTA: consumer lag alto!")
    
    time.sleep(30)  # verificar cada 30 segundos
```

**Restricciones:**
1. Implementar la función completa y verificar con un consumer real
2. Añadir la estimación del tiempo de recuperación:
   si el lag es 1M mensajes y el consumer procesa 10K/s,
   tardará ~100 segundos en ponerse al día
3. Implementar alertas: si el lag crece en 3 mediciones consecutivas,
   enviar una alerta

---

### Ejercicio 9.4.3 — Múltiples grupos, múltiples casos de uso

```python
# El mismo topic puede tener múltiples consumer groups independientes.
# Cada grupo mantiene sus propios offsets — son completamente independientes.

# Topic "compras":
#   - Consumer Group "analytics": lee para calcular métricas (puede tener lag)
#   - Consumer Group "fraude": lee para detectar fraude (debe tener lag mínimo)
#   - Consumer Group "notificaciones": lee para enviar emails (lag tolerable)
#   - Consumer Group "inventario": lee para actualizar stock (crítico, lag=0)

# Producir un mensaje → todos los grupos lo ven (independientemente)
# No hay "routing" — el topic es broadcast a todos los grupos suscritos

# Ver todos los grupos que leen un topic:
admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Listar todos los consumer groups:
grupos = admin.list_consumer_groups()
for grupo in grupos.valid:
    # Obtener detalles del grupo (incluyendo qué topics lee):
    detalles = admin.describe_consumer_groups([grupo.group_id])
    # ...
```

**Preguntas:**

1. Si tienes 4 consumer groups leyendo el mismo topic, ¿el producer
   necesita producir los mensajes 4 veces?

2. Si el grupo "analytics" tiene lag de 1 millón de mensajes,
   ¿afecta al grupo "fraude"?

3. ¿Qué pasa si el grupo "inventario" (crítico) tiene un bug y
   procesa mensajes incorrectamente? ¿Puedes "volver atrás"?

4. ¿Cómo monitorizarías el lag de los 4 grupos simultáneamente
   con una sola herramienta?

---

### Ejercicio 9.4.4 — Rebalance: el evento que interrumpe el procesamiento

```python
# El rebalance ocurre cuando:
# 1. Un consumer nuevo se une al grupo
# 2. Un consumer muere o pierde la sesión (session.timeout.ms)
# 3. Se añaden/eliminan particiones al topic
# 4. El consumer no llama a poll() dentro de max.poll.interval.ms

# Durante el rebalance (estrategia eager/stop-the-world):
# - Todos los consumers del grupo se detienen
# - Kafka reasigna todas las particiones
# - Los consumers reanudan con sus nuevas asignaciones
# Tiempo de pausa: segundos a minutos dependiendo del número de consumers

# Con cooperative-sticky (Kafka 2.4+):
# - Solo los consumers que pierden o ganan particiones se pausan
# - El resto continúa consumiendo sin interrupción
# Tiempo de pausa: mucho menor (solo para los consumers afectados)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "produccion",
    "partition.assignment.strategy": "cooperative-sticky",
    "session.timeout.ms": 45000,        # 45 segundos para detectar muerte
    "heartbeat.interval.ms": 3000,      # heartbeat cada 3 segundos
    "max.poll.interval.ms": 300000,     # 5 minutos máximo entre polls
})

# Manejar el rebalance gracefully:
def on_assign(consumer, partitions):
    """Llamado al recibir nuevas particiones."""
    # Aquí puedes cargar estado de las nuevas particiones (ej: de Redis)
    for p in partitions:
        print(f"Nueva partición asignada: {p.partition}")

def on_revoke(consumer, partitions):
    """Llamado antes de perder particiones."""
    # IMPORTANTE: commitear offsets de las particiones que se van a perder
    consumer.commit()  # commit sincrónico de todos los offsets pendientes
    for p in partitions:
        print(f"Partición revocada: {p.partition}")
        # Guardar estado en almacenamiento externo si es necesario

consumer.subscribe(["clicks"], on_assign=on_assign, on_revoke=on_revoke)
```

**Restricciones:**
1. Simular un rebalance: lanzar 3 consumers, luego añadir un cuarto
   y observar qué particiones se reasignan
2. Medir el tiempo de pausa del procesamiento durante el rebalance
   con estrategia eager vs cooperative-sticky
3. ¿Qué información debes guardar en `on_revoke` para que el nuevo
   asignatario pueda retomar correctamente?

---

### Ejercicio 9.4.5 — Leer: el consumer group que nunca ponía al día

**Tipo: Diagnosticar**

Un consumer group tiene 4 consumers y un topic de 4 particiones.
El consumer lag crece constantemente a razón de 50,000 mensajes/hora
aunque el processing time por mensaje parece razonable (2ms).

```
Topic: pedidos (4 particiones)
Consumer group: procesador-pedidos (4 consumers)
Producer rate: 100,000 mensajes/hora
Consumer rate medida: ~50,000 mensajes/hora

Métricas de cada consumer:
  Consumer 1 (P0): 20,000 mensajes/hora
  Consumer 2 (P1): 20,000 mensajes/hora
  Consumer 3 (P2): 5,000 mensajes/hora   ← !!
  Consumer 4 (P3): 5,000 mensajes/hora   ← !!
```

**Preguntas:**

1. ¿Por qué Consumers 3 y 4 procesan 4× menos mensajes que 1 y 2?

2. ¿La solución es añadir más consumers? ¿Por qué no?

3. ¿Cómo diagnosticarías qué está causando que Consumers 3 y 4 sean lentos?

4. Si el problema es que P2 y P3 tienen mensajes más grandes (10KB vs 1KB),
   ¿cómo lo resolverías sin cambiar el producer?

5. Si el problema es que algunos mensajes tienen una llamada a una API externa
   que tarda 500ms ocasionalmente, ¿cómo lo resolverías?

---

## Sección 9.5 — Garantías de Entrega: At-Most-Once, At-Least-Once, Exactly-Once

### Ejercicio 9.5.1 — Los tres modelos y sus implicaciones

```
AT-MOST-ONCE (puede perder mensajes, nunca duplica):
  Consumer: leer mensaje → commit offset → procesar
  Si falla entre commit y procesar: el mensaje se pierde (ya fue commiteado)
  Cuándo aceptable: métricas de baja importancia, logs de debug

AT-LEAST-ONCE (nunca pierde, puede duplicar):
  Consumer: leer mensaje → procesar → commit offset
  Si falla entre procesar y commit: el mensaje se reprocesa (duplicado)
  Cuándo aceptable: con sink idempotente (la operación repetida es inocua)

EXACTLY-ONCE (ni pierde ni duplica):
  Consumer + Producer transaccional + sink idempotente
  Más complejo de implementar, tiene overhead de latencia
  Cuándo necesario: pagos, inventario, cualquier operación financiera
```

```python
# AT-MOST-ONCE (malo para casi todo):
msg = consumer.poll(1.0)
consumer.commit(msg)  # ← commitear ANTES de procesar
procesar(msg)         # si falla aquí, el mensaje se perdió

# AT-LEAST-ONCE (correcto para la mayoría):
msg = consumer.poll(1.0)
procesar(msg)         # procesar primero
consumer.commit(msg)  # commitear DESPUÉS — si falla aquí, se reprocesa

# EXACTLY-ONCE (con transacciones):
msg = consumer.poll(1.0)
producer.begin_transaction()
try:
    resultado = procesar(msg)
    producer.produce("destino", value=resultado)
    # Committear el offset como parte de la transacción:
    producer.send_offsets_to_transaction(
        consumer.position(consumer.assignment()),
        consumer.consumer_group_metadata(),
    )
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
```

**Preguntas:**

1. ¿El "exactly-once" de Kafka garantiza que el mensaje se procesa
   exactamente una vez, o que se escribe al topic destino exactamente una vez?

2. ¿Qué significa "sink idempotente"? ¿Una inserción en PostgreSQL
   con `INSERT ... ON CONFLICT DO UPDATE` es idempotente?

3. ¿El exactly-once de Kafka funciona si el sink es una base de datos
   externa (no otro topic de Kafka)?

4. ¿Cuánta latencia adicional añade exactly-once vs at-least-once?

5. ¿Puedes combinar exactly-once en un pipeline de
   Kafka→Kafka→base_de_datos?

**Pista:** El "exactly-once" de Kafka funciona de forma nativa cuando
el sink es otro topic de Kafka (Kafka-to-Kafka). Para sinks externos
(PostgreSQL, S3, etc.), necesitas implementar idempotencia en el sink:
el sink debe poder detectar y manejar duplicados. La combinación ganadora
para sinks externos: at-least-once en Kafka + idempotencia en el sink
= "effectively exactly-once". Los sistemas de pagos, por ejemplo, usan
un `idempotency_key` en cada transacción para detectar duplicados.

---

### Ejercicio 9.5.2 — Implementar idempotencia en el sink

```python
import psycopg2
import json
from confluent_kafka import Consumer

class ConsumerIdempotente:
    """
    Consumer con sink idempotente en PostgreSQL.
    Garantiza exactly-once efectivo para inserciones en BD.
    """
    
    def __init__(self, pg_dsn: str):
        self.pg_conn = psycopg2.connect(pg_dsn)
        self._crear_tabla_procesados()
        
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "insertor-idempotente",
            "enable.auto.commit": False,
        })
        self.consumer.subscribe(["pedidos"])
    
    def _crear_tabla_procesados(self):
        """Tabla para rastrear qué mensajes ya fueron procesados."""
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kafka_offsets_procesados (
                    topic VARCHAR(255),
                    particion INTEGER,
                    offset BIGINT,
                    procesado_en TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (topic, particion, offset)
                )
            """)
        self.pg_conn.commit()
    
    def _ya_procesado(self, topic: str, partition: int, offset: int) -> bool:
        with self.pg_conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM kafka_offsets_procesados WHERE topic=%s AND particion=%s AND offset=%s",
                (topic, partition, offset)
            )
            return cur.fetchone() is not None
    
    def _insertar_con_idempotencia(self, datos: dict, topic: str, partition: int, offset: int):
        """
        Insertar el dato Y marcar el offset como procesado en la misma transacción.
        Si cualquiera de los dos falla, ambos hacen rollback.
        """
        with self.pg_conn.cursor() as cur:
            try:
                # Insertar el dato de negocio:
                cur.execute(
                    "INSERT INTO pedidos (id, user_id, monto, estado) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (id) DO NOTHING",  # idempotente
                    (datos["id"], datos["user_id"], datos["monto"], datos["estado"])
                )
                
                # Marcar el offset como procesado:
                cur.execute(
                    "INSERT INTO kafka_offsets_procesados (topic, particion, offset) VALUES (%s, %s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (topic, partition, offset)
                )
                
                self.pg_conn.commit()  # una sola transacción para ambas operaciones
            except Exception:
                self.pg_conn.rollback()
                raise
    
    def procesar(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            
            # Verificar si ya fue procesado (en caso de reprocesamiento):
            if self._ya_procesado(msg.topic(), msg.partition(), msg.offset()):
                print(f"Offset {msg.offset()} ya procesado — skipping")
                self.consumer.commit(msg)
                continue
            
            datos = json.loads(msg.value().decode())
            self._insertar_con_idempotencia(
                datos, msg.topic(), msg.partition(), msg.offset()
            )
            self.consumer.commit(msg)
```

**Restricciones:**
1. Implementar y verificar que el reprocesamiento no crea duplicados
2. ¿El check `_ya_procesado` es necesario si `INSERT ... ON CONFLICT DO NOTHING`
   ya es idempotente? ¿Cuándo sí es necesario?
3. ¿La tabla `kafka_offsets_procesados` crecerá indefinidamente?
   ¿Cómo la mantienes manejable?

---

### Ejercicio 9.5.3 — Exactly-once end-to-end: Kafka → Kafka

```python
from confluent_kafka import Consumer, Producer

def pipeline_exactamente_una_vez(
    topic_entrada: str,
    topic_salida: str,
    transformar: callable,
):
    """
    Lee de topic_entrada, transforma, y escribe a topic_salida
    con garantía de exactly-once (Kafka-to-Kafka).
    """
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "exactly-once-processor",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",  # solo leer mensajes committed
    })
    
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "transactional.id": "eo-processor-1",
        "enable.idempotence": True,
        "acks": "all",
    })
    
    producer.init_transactions()
    consumer.subscribe([topic_entrada])
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        
        # Procesar dentro de una transacción:
        producer.begin_transaction()
        try:
            datos_entrada = json.loads(msg.value().decode())
            datos_salida = transformar(datos_entrada)
            
            # Producir al topic de salida:
            producer.produce(
                topic=topic_salida,
                key=msg.key(),
                value=json.dumps(datos_salida).encode(),
            )
            
            # Committear el offset del consumer como parte de la transacción:
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata(),
            )
            
            # Confirmar todo:
            producer.commit_transaction()
            
        except Exception as e:
            producer.abort_transaction()
            print(f"Error: {e} — transacción abortada")
```

**Restricciones:**
1. Implementar y verificar con un pipeline de transformación simple
2. Simular un fallo a mitad de la transacción y verificar que no hay duplicados
3. Medir la latencia adicional vs at-least-once para el mismo pipeline
4. ¿Qué significa `isolation.level=read_committed` en el consumer?

---

### Ejercicio 9.5.4 — Leer: cuándo exactly-once es necesario y cuándo no

**Tipo: Analizar**

Para cada caso, determinar el nivel de garantía necesario y justificar:

```
Caso 1: Stream de eventos de navegación web (100M/día)
  Destino: tabla de analytics en BigQuery
  Impacto de un duplicado: contador inflado en ~0.01%

Caso 2: Transferencias bancarias entre cuentas
  Destino: base de datos de cuentas bancarias
  Impacto de un duplicado: el usuario paga dos veces

Caso 3: Notificaciones push a usuarios
  Destino: servicio de push notifications
  Impacto de un duplicado: el usuario recibe la notificación dos veces

Caso 4: Actualización de stock en inventario
  Destino: base de datos de inventario
  Impacto de un duplicado: el stock se reduce dos veces (puede quedar negativo)

Caso 5: Logs de errores de aplicación
  Destino: Elasticsearch para análisis
  Impacto de un duplicado: el error aparece dos veces en el dashboard
```

Para cada caso, recomendar: at-most-once, at-least-once con sink idempotente,
o exactly-once nativo de Kafka. Justificar el costo vs beneficio.

---

### Ejercicio 9.5.5 — Diagnosticar: duplicados en el pipeline de pedidos

**Tipo: Diagnosticar**

El equipo de negocio reporta que algunos pedidos se están procesando dos veces —
los clientes están siendo cobrados dos veces. El código del consumer:

```python
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "procesador-pagos",
    "enable.auto.commit": True,   # auto commit
    "auto.commit.interval.ms": 5000,
})

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    
    datos = json.loads(msg.value().decode())
    cobrar_al_cliente(datos["user_id"], datos["monto"])  # operación que tarda 3s
    # El auto-commit puede ocurrir DURANTE o ANTES de cobrar_al_cliente
```

**Preguntas:**

1. ¿Por qué `enable.auto.commit=True` puede causar duplicados en este caso?

2. ¿Qué condición específica hace que el mismo pedido se cobre dos veces?

3. El auto-commit ocurre cada 5 segundos (`auto.commit.interval.ms=5000`)
   y `cobrar_al_cliente` tarda 3 segundos. ¿Puede el auto-commit commitear
   el offset antes de que `cobrar_al_cliente` termine?

4. Proponer dos soluciones: una de bajo esfuerzo (cambio mínimo de código)
   y una de alto esfuerzo (arquitectura correcta).

5. ¿Habría duplicados si `cobrar_al_cliente` fuera idempotente?

---

## Sección 9.6 — Kafka Streams: Procesamiento en el Broker

### Ejercicio 9.6.1 — Kafka Streams vs consumer manual

```java
// Kafka Streams es una librería Java para procesamiento de streams
// que se ejecuta dentro de la aplicación (no en el broker).
// Es el equivalente "liviano" de Spark Structured Streaming o Flink
// pero sin infraestructura adicional.

// El mismo pipeline de wordcount en Kafka Streams vs consumer manual:

// Consumer manual (Python):
// topics de entrada → leer → procesar → escribir a topic de salida

// Kafka Streams (Java/Scala):
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> textos = builder.stream("textos-entrada");

KTable<String, Long> wordcount = textos
    .flatMapValues(texto -> Arrays.asList(texto.toLowerCase().split("\\s+")))
    .groupBy((key, palabra) -> palabra)
    .count();

wordcount.toStream().to("wordcount-salida");

KafkaStreams streams = new KafkaStreams(builder.build(), config);
streams.start();
// Kafka Streams maneja: particionamiento, estado, rebalances, y exactly-once
```

```python
# Para Python: usar Faust (alternativa a Kafka Streams en Python)
import faust

app = faust.App("wordcount", broker="kafka://localhost:9092")

textos_topic = app.topic("textos-entrada", value_type=str)
wordcount_topic = app.topic("wordcount-salida", value_type=int)

# Tabla de estado (equivale al KTable de Kafka Streams):
conteos = app.Table("wordcount", default=int)

@app.agent(textos_topic)
async def procesar(stream):
    async for texto in stream:
        for palabra in texto.lower().split():
            conteos[palabra] += 1
            await wordcount_topic.send(key=palabra, value=conteos[palabra])
```

**Preguntas:**

1. ¿Kafka Streams se ejecuta en los brokers de Kafka o en los servidores
   de la aplicación?

2. ¿Cuándo preferirías Kafka Streams sobre un consumer manual con Polars o Pandas?

3. ¿Cuándo preferirías Spark Structured Streaming o Flink sobre Kafka Streams?

4. ¿Kafka Streams tiene estado (stateful processing)? ¿Cómo lo gestiona?

5. ¿Faust (Python) es una alternativa completa a Kafka Streams (Java)?
   ¿Qué features no tiene Faust?

---

### Ejercicio 9.6.2 — Estado en Kafka Streams: KTable y GlobalKTable

```java
// KTable: tabla particionada (una partición del state store por partición del topic)
// Útil para: joins entre streams del mismo topic o aggregations

// GlobalKTable: tabla NO particionada (cada instancia tiene la tabla completa)
// Útil para: enriquecer un stream con datos de referencia estáticos

StreamsBuilder builder = new StreamsBuilder();

// Stream de ventas (particionado por venta_id):
KStream<String, Venta> ventas = builder.stream("ventas");

// Tabla de clientes (enriquecimiento — replicada en cada instancia):
GlobalKTable<String, Cliente> clientes =
    builder.globalTable("clientes");

// Join stream con GlobalKTable (no requiere re-particionamiento):
KStream<String, VentaEnriquecida> ventas_enriquecidas = ventas.join(
    clientes,
    (venta_id, venta) -> venta.getClienteId(),  // key del join en la GlobalKTable
    (venta, cliente) -> new VentaEnriquecida(venta, cliente)
);

// Aggregation con KTable:
KTable<String, Double> revenue_por_region = ventas_enriquecidas
    .groupBy((key, venta) -> venta.getRegion())
    .aggregate(
        () -> 0.0,  // inicializar
        (region, venta, acumulado) -> acumulado + venta.getMonto()
    );
```

**Preguntas:**

1. ¿Por qué un join con `GlobalKTable` no requiere re-particionamiento
   mientras un join con `KTable` sí puede requerirlo?

2. ¿Cómo se almacena el estado de Kafka Streams? ¿En memoria, en disco,
   o en Kafka?

3. Si una instancia de Kafka Streams se reinicia, ¿cómo recupera su estado?

4. ¿Cuándo es demasiado grande para un `GlobalKTable`?
   ¿Qué haces si tu tabla de referencia tiene 10 GB?

---

### Ejercicio 9.6.3 — Ventanas de tiempo en Kafka Streams

```java
// Tipos de ventanas:
// Tumbling: ventanas fijas sin solapamiento (0-5min, 5-10min, 10-15min...)
// Hopping: ventanas con solapamiento (0-10min, 5-15min, 10-20min...)
// Session: ventanas basadas en actividad (grupo de eventos cercanos en tiempo)

// Contar clicks en ventanas de 5 minutos:
KStream<String, Click> clicks = builder.stream("clicks");

// Tumbling window de 5 minutos:
KTable<Windowed<String>, Long> clicks_por_ventana = clicks
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .count();

// Emitir resultados al cerrar cada ventana:
clicks_por_ventana
    .toStream()
    .map((windowed_key, count) -> KeyValue.pair(
        windowed_key.key(),
        Map.of(
            "user_id", windowed_key.key(),
            "ventana_inicio", windowed_key.window().startTime().toString(),
            "ventana_fin", windowed_key.window().endTime().toString(),
            "clicks", count
        )
    ))
    .to("clicks-por-ventana");
```

**Preguntas:**

1. ¿Qué es el "grace period" de una ventana y por qué es necesario
   para manejar eventos tardíos?

2. Si un evento llega después de que su ventana cerró (fuera del grace period),
   ¿qué hace Kafka Streams?

3. ¿Las session windows son fáciles de implementar con un consumer manual?
   ¿Por qué Kafka Streams es más conveniente para esto?

4. ¿Cuándo preferirías ventanas tumbling vs hopping?

---

### Ejercicio 9.6.4 — Streams interactivos: consultar el estado

```java
// Kafka Streams permite consultar el estado interno vía "Interactive Queries"
// Sin necesitar escribir el estado a una base de datos externa.

KafkaStreams streams = new KafkaStreams(topology, config);
streams.start();

// Consultar el store de estado:
ReadOnlyKeyValueStore<String, Long> store =
    streams.store(
        StoreQueryParameters.fromNameAndType(
            "mi-store",
            QueryableStoreTypes.keyValueStore()
        )
    );

// Leer el conteo actual de un usuario:
Long conteo_usuario = store.get("user_123");

// Iterar sobre todos los valores:
KeyValueIterator<String, Long> todos = store.all();
while (todos.hasNext()) {
    KeyValue<String, Long> siguiente = todos.next();
    System.out.println(siguiente.key + ": " + siguiente.value);
}

// En una API REST, exponer el estado:
// GET /api/stats/user/{user_id} → lee del store de Kafka Streams directamente
// Sin necesidad de una base de datos externa (Redis, PostgreSQL)
```

**Preguntas:**

1. ¿El estado de Kafka Streams está disponible para todos los nodos
   de la aplicación, o solo para el nodo que tiene la partición?

2. ¿Cómo implementas una API REST que consulta el estado de Kafka Streams
   cuando hay múltiples instancias de la aplicación?

3. ¿Las Interactive Queries afectan al rendimiento del procesamiento?

4. ¿Cuándo es mejor usar un store de Kafka Streams vs Redis para el estado?

---

### Ejercicio 9.6.5 — Leer: Kafka Streams vs Flink para producción

**Tipo: Comparar**

Para una empresa que debe elegir entre Kafka Streams y Flink para
un pipeline de detección de fraude en tiempo real (latencia < 200ms):

```
Requisitos:
  - 50,000 transacciones/segundo en pico
  - Estado por usuario: historial de los últimos 30 minutos
  - Reglas de fraude complejas (joins, ventanas, ML scoring)
  - Disponibilidad: 99.99%
  - Equipo: Java/Python, 5 data engineers

Kafka Streams:
  + Sin infraestructura adicional (corre dentro de la app)
  + Fácil de desplegar y operar
  + Estado gestionado automáticamente
  - Solo Java/Scala (nativo)
  - Menos poderoso para operaciones complejas
  - No tiene checkpointing externo robusto

Flink:
  + Más poderoso (ventanas complejas, ML, grafos)
  + Mejor tolerancia a fallos (checkpointing)
  + Python API disponible
  - Requiere cluster propio (Kubernetes o managed)
  - Más complejo de operar
  - Mayor curva de aprendizaje
```

**Preguntas:**

1. ¿Cuál recomendarías para este caso específico? Justifica.

2. ¿La decisión cambia si el equipo ya tiene experiencia con Kubernetes?

3. ¿Existe una tercera opción que no se ha considerado?

4. Si implementas con Kafka Streams ahora y en 1 año necesitas migrar a Flink,
   ¿cuánto trabajo de migración hay?

---

## Sección 9.7 — Operación: Monitoreo y Diagnóstico

### Ejercicio 9.7.1 — Las métricas críticas de Kafka

```python
# Las métricas más importantes a monitorear en un cluster Kafka:

# 1. Consumer Lag (por consumer group y topic):
# Ya implementado en §9.4.2

# 2. Under-replicated partitions:
# Particiones donde no todas las réplicas están en sincronía
# Si > 0: hay un broker con problemas o una réplica atrasada

# 3. ISR shrink rate:
# Velocidad a la que las réplicas salen del ISR
# Si alta: inestabilidad en el cluster (red, disco, GC en brokers)

# 4. Request rate y latencia del producer:
# produce_request_rate, fetch_request_rate
# request_latency_avg, request_latency_max

# 5. Disk usage:
# Cuánto espacio en disco usa cada broker
# Relevante con políticas de retención largas o alta tasa de producción

# Verificar estado del cluster desde Python:
from confluent_kafka.admin import AdminClient

def estado_cluster(bootstrap_servers: str) -> dict:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    
    metadata = admin.list_topics(timeout=10)
    
    brokers = {bid: {"host": b.host, "port": b.port}
               for bid, b in metadata.brokers.items()}
    
    topics_con_problemas = []
    for topic_name, topic in metadata.topics.items():
        for pid, p in topic.partitions.items():
            if p.isrs != p.replicas:  # hay réplicas fuera de sync
                topics_con_problemas.append({
                    "topic": topic_name,
                    "particion": pid,
                    "replicas": p.replicas,
                    "isr": p.isrs,
                    "under_replicated": True,
                })
    
    return {
        "num_brokers": len(brokers),
        "brokers": brokers,
        "topics": len(metadata.topics),
        "under_replicated_partitions": len(topics_con_problemas),
        "detalles_under_replicated": topics_con_problemas,
    }

estado = estado_cluster("localhost:9092")
if estado["under_replicated_partitions"] > 0:
    print(f"⚠ ALERTA: {estado['under_replicated_partitions']} particiones under-replicated")
```

**Restricciones:**
1. Implementar la función `estado_cluster` completa
2. Añadir verificación del consumer lag para todos los grupos
3. Implementar alertas automáticas (email, Slack) cuando el lag supera umbrales
4. ¿Qué herramientas de monitoreo recomendarías para un cluster Kafka en producción?

> 🔗 Ecosistema: herramientas estándar de monitoreo para Kafka incluyen
> Kafka Exporter + Prometheus + Grafana, Confluent Control Center (si usas
> Confluent Platform), y kminion (open-source, ligero). No se cubren en
> detalle aquí — verificar la documentación oficial de cada herramienta
> para la versión de Kafka que uses.

---

### Ejercicio 9.7.2 — Diagnosticar: el cluster lento

**Tipo: Diagnosticar**

Un cluster Kafka en producción tiene latencia de produce inusualmente alta:
normalmente 5ms, ahora 250ms. Los logs del broker muestran:

```
[2024-01-15 14:32:15] WARN: ISR for partition clicks-0 shrunk from [1,2,3] to [1,2]
[2024-01-15 14:32:18] WARN: ISR for partition clicks-1 shrunk from [1,2,3] to [1,2]
[2024-01-15 14:32:22] INFO: Broker 3 is now unavailable
[2024-01-15 14:32:23] WARN: ISR for partition compras-0 shrunk from [1,2,3] to [1,2]
...
```

Los producers tienen configuración:
```python
{"acks": "all", "min.insync.replicas": "2"}
```

**Preguntas:**

1. ¿Por qué la latencia del producer aumentó de 5ms a 250ms?

2. ¿Con `acks=all` y `min.insync.replicas=2`, y el ISR reducido a [1,2],
   ¿los producers siguen funcionando?

3. ¿Cuál es el riesgo de que el Broker 3 esté caído con esta configuración?

4. ¿Cuándo `min.insync.replicas=2` salvaría los datos que `min.insync.replicas=1` no salvaria?

5. ¿Qué pasos de recuperación tomar inmediatamente?

---

### Ejercicio 9.7.3 — Scaling: cuándo añadir particiones o brokers

```python
# Decisión 1: Añadir particiones a un topic existente

# CUIDADO: añadir particiones cambia el hash de las keys existentes.
# Un mensaje que iba a partición 2 puede ir a partición 5 después del cambio.
# Si el orden por key es importante, esto rompe la garantía de orden.

# Añadir particiones (solo posible aumentar, no disminuir):
admin = AdminClient({"bootstrap.servers": "localhost:9092"})
admin.create_partitions({"mi-topic": NewPartitions(total_count=10)})
# ANTES: 4 particiones → DESPUÉS: 10 particiones
# RIESGO: los consumers existentes necesitan rebalancear

# Decisión 2: Añadir brokers al cluster
# - Requiere reasignar particiones existentes a los nuevos brokers
# - herramienta: kafka-reassign-partitions.sh
# - Proceso gradual: no mover todas las particiones a la vez

# Cuándo añadir:
# Particiones: cuando el throughput de un topic supera la capacidad de un broker
# Brokers: cuando el almacenamiento o CPU total del cluster es insuficiente
```

**Preguntas:**

1. ¿Cuándo es seguro añadir particiones a un topic existente?
   ¿Cuándo es riesgoso?

2. Un topic de "estado de usuario" (compact policy) usa `user_id` como key
   para garantizar que todas las actualizaciones de un usuario van a la misma
   partición. Si añades particiones, ¿qué pasa?

3. ¿Hay alguna forma de añadir particiones sin romper la garantía de orden?

4. Un cluster de 3 brokers tiene un topic con factor de replicación 3
   y 12 particiones. Si añades 3 brokers más, ¿se redistribuyen las particiones
   automáticamente?

---

### Ejercicio 9.7.4 — El runbook de incidentes de Kafka

**Tipo: Construir**

Crear un runbook de diagnóstico para los incidentes más comunes en Kafka:

```markdown
# Runbook: Kafka Production Incidents

## Incidente: Consumer Lag Creciente
**Síntoma**: lag del consumer group aumenta constantemente
**Diagnóstico**:
1. Verificar tasa de producción vs tasa de consumo
   ```bash
   kafka-consumer-groups.sh --describe --group MI_GRUPO
   ```
2. Revisar métricas del consumer: CPU, memoria, latencia de procesamiento
3. Verificar si hay un consumer con partición asignada pero consumiendo lento

**Posibles causas y soluciones**:
- El procesamiento es lento → optimizar el código de procesamiento
- Pocos consumers para la carga → aumentar número de consumers (hasta límite de particiones)
- Partición con mensajes grandes → ???

## Incidente: Under-Replicated Partitions
**Síntoma**: una o más particiones tienen ISR < replication_factor
**Diagnóstico**:
1. ???
2. ???
**Posibles causas y soluciones**:
- ???
```

**Restricciones:**
1. Completar el runbook para al menos 5 tipos de incidentes:
   - Consumer lag creciente
   - Under-replicated partitions
   - Producer con alta latencia
   - OOM en el broker
   - Pérdida de mensajes
2. Para cada incidente: síntoma observable, pasos de diagnóstico,
   posibles causas, y procedimiento de remediación
3. Incluir comandos específicos de Kafka para cada paso de diagnóstico

---

### Ejercicio 9.7.5 — El sistema de e-commerce en Kafka: diseño final

**Tipo: Integrar**

Este es el ejercicio integrador del capítulo. Diseñar el sistema completo
de Kafka para el e-commerce del repositorio:

```
Requisitos finales:
  - 200,000 eventos/segundo en pico (Black Friday)
  - Disponibilidad 99.95% (43 minutos de downtime/año)
  - Retención: 7 días para eventos, 30 días para compras
  - Consumers: analytics (lag tolerable), fraude (lag < 1s), notificaciones

El diseño debe especificar:
1. Topología de topics (nombres, particiones, retención, compactación)
2. Configuración del cluster (número de brokers, factor de replicación)
3. Configuración de producers por tipo de evento
4. Configuración de consumer groups por caso de uso
5. Estrategia de monitoreo y alertas
6. Plan de disaster recovery: ¿qué pasa si un DC falla?
7. Estimación de costo (disco, red, máquinas)
```

---

## Resumen del capítulo

**Los cinco conceptos de Kafka que se aplican todos los días:**

```
1. El log es la fuente de verdad
   Kafka retiene mensajes — no los descarta como una cola.
   Esto permite: reprocesamiento, múltiples consumers, replay.
   La retención tiene un costo (disco) — diseñarla conscientemente.

2. Las particiones determinan el paralelismo
   Un topic de N particiones soporta N consumers paralelos máximo.
   Las particiones también determinan el orden: garantizado dentro de
   una partición, no entre particiones.
   Diseña el número de particiones para el paralelismo futuro, no el actual.

3. El consumer lag es la métrica más importante
   Si el lag crece: el consumer no puede seguir el ritmo.
   Si el lag es 0: el consumer está al día.
   Monitorear el lag es la primera señal de problemas en producción.

4. At-least-once + sink idempotente = exactly-once efectivo
   Kafka-native exactly-once solo funciona Kafka→Kafka.
   Para sinks externos: diseñar la idempotencia en el sink.
   La operación de negocio debe poder ejecutarse dos veces sin efecto doble.

5. El Schema Registry previene incidentes de schema
   Sin schema registry: un cambio de schema rompe todos los consumers.
   Con schema registry: la compatibilidad se verifica antes del deploy.
   Es el componente que separa "Kafka en producción" de "Kafka en dev".
```

**Lo que conecta este capítulo con Cap.10 (Beam) y Cap.11 (Spark Streaming):**

> Kafka es el bus de datos — el sistema que transporta y retiene mensajes.
> Beam y Spark Structured Streaming son motores que procesan esos mensajes.
> La arquitectura estándar: producers → Kafka → Beam/Spark → sinks.
> Cap.10 y Cap.11 cubren el motor de procesamiento que se construye
> sobre la base de Kafka que aprendiste aquí.

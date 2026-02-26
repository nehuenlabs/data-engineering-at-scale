# Guía de Ejercicios — Cap.12: Apache Flink — Streaming Nativo

> Flink no trata el streaming como "batches muy pequeños".
> Para Flink, el streaming es el modelo fundamental —
> y el batch es un caso especial del streaming donde el stream tiene fin.
>
> Esta inversión de perspectiva tiene consecuencias profundas:
> latencia de milisegundos, estado gestionado con precisión quirúrgica,
> y garantías de exactly-once sin los compromisos del micro-batching.
>
> El precio: más complejidad operacional, una curva de aprendizaje más pronunciada,
> y una API que requiere pensar en términos de streams, no de DataFrames.
> Para el 80% de los casos de uso, Spark SS es suficiente.
> Para el 20% que necesita milisegundos o estado complejo, Flink es la respuesta.

---

## Por qué Flink existe: los límites del micro-batching

```
Problema 1: Latencia
  Spark SS (micro-batch de 1s):
    evento ocurre → esperar hasta el siguiente trigger (0-1s)
                  → procesar batch (~500ms)
                  → escribir resultado (~200ms)
    Latencia total: 1.7s en el mejor caso, varios segundos en el peor

  Flink (streaming nativo):
    evento ocurre → procesar inmediatamente (~5ms)
                  → escribir resultado (~2ms)
    Latencia total: ~10ms

Problema 2: Estado con semántica de tiempo precisa
  Spark SS: el tiempo de procesamiento y el event time son aproximaciones
  Flink: event time nativo desde el diseño, watermarks precisos por stream

Problema 3: Joins stream-stream con ventanas largas
  Spark SS: mantener estado por horas en micro-batches genera overhead
  Flink: el estado es un ciudadano de primera clase — RocksDB gestionado
         automáticamente, spill a disco transparente

Casos donde Flink gana claramente:
  → Detección de fraude en tiempo real (< 100ms de latencia)
  → Alertas de monitoreo de infraestructura (sub-segundo)
  → CEP (Complex Event Processing) — patrones en secuencias de eventos
  → Pipelines con joins entre streams con ventanas de horas o días
```

---

## El modelo mental: todo es un stream

```
Flink ve el mundo así:

                    ┌─────────────┐
  stream infinito → │   Flink     │ → stream de resultados
  (Kafka, socket)   │   Job       │
                    └─────────────┘

                    ┌─────────────┐
  archivo finito  → │   Flink     │ → resultados finales
  (batch como      │   Job       │   (el stream "termina")
   stream acotado)  └─────────────┘

La API es la misma. El DataStream API no distingue entre bounded y unbounded.
Los operadores son los mismos. El estado es el mismo.
Solo la fuente de datos es diferente.
```

---

## Tabla de contenidos

- [Sección 12.1 — Arquitectura: JobManager, TaskManager, y el modelo de ejecución](#sección-121--arquitectura-jobmanager-taskmanager-y-el-modelo-de-ejecución)
- [Sección 12.2 — DataStream API: transformaciones sobre streams](#sección-122--datastream-api-transformaciones-sobre-streams)
- [Sección 12.3 — Estado en Flink: el diferenciador principal](#sección-123--estado-en-flink-el-diferenciador-principal)
- [Sección 12.4 — Tiempo y watermarks: event time nativo](#sección-124--tiempo-y-watermarks-event-time-nativo)
- [Sección 12.5 — Ventanas: el modelo completo de Flink](#sección-125--ventanas-el-modelo-completo-de-flink)
- [Sección 12.6 — Checkpointing y tolerancia a fallos](#sección-126--checkpointing-y-tolerancia-a-fallos)
- [Sección 12.7 — Flink SQL y Table API: el nivel de abstracción superior](#sección-127--flink-sql-y-table-api-el-nivel-de-abstracción-superior)

---

## Sección 12.1 — Arquitectura: JobManager, TaskManager, y el Modelo de Ejecución

### Ejercicio 12.1.1 — Leer: los componentes de un cluster Flink

**Tipo: Leer**

```
Cluster Flink:

  ┌─────────────────────────────────────────────────────┐
  │  JobManager (1 por cluster, HA posible con ZooKeeper)│
  │  ┌──────────────────┐  ┌──────────────────────────┐ │
  │  │  JobMaster       │  │  ResourceManager         │ │
  │  │  (1 por job)     │  │  (gestiona TaskManagers) │ │
  │  │  - scheduling    │  └──────────────────────────┘ │
  │  │  - checkpoints   │  ┌──────────────────────────┐ │
  │  │  - recovery      │  │  Dispatcher              │ │
  │  └──────────────────┘  │  (recibe jobs del cliente)│ │
  │                         └──────────────────────────┘ │
  └─────────────────────────────────────────────────────┘

  ┌────────────────────┐  ┌────────────────────┐
  │  TaskManager       │  │  TaskManager       │  ...
  │  ┌──────────────┐  │  │  ┌──────────────┐  │
  │  │  Task Slot 1 │  │  │  │  Task Slot 1 │  │
  │  │  Task Slot 2 │  │  │  │  Task Slot 2 │  │
  │  │  Task Slot 3 │  │  │  │  Task Slot 3 │  │
  │  └──────────────┘  │  └──────────────┘  │
  └────────────────────┘  └────────────────────┘

Cada Task Slot es un thread de procesamiento.
Un Task Slot puede ejecutar una cadena de operadores (operator chaining).
El estado de cada operador está en el Task Slot que lo ejecuta.
```

```
Comparación con Spark:

  Spark Driver     ≈  Flink JobManager
  Spark Executor   ≈  Flink TaskManager
  Spark Task       ≈  Flink Task (sub-task)
  Spark Partition  ≈  Flink Partition del stream

  Diferencia clave:
  Spark: el Driver coordina activamente la ejecución de cada stage
  Flink: el JobMaster lanza el job y luego los operators corren
         continuamente — no hay concepto de "stage" que empieza y termina
```

**Preguntas:**

1. ¿Por qué Flink tiene un modelo de ejecución continuo mientras
   Spark tiene stages que empiezan y terminan?

2. ¿Cuántos Task Slots por TaskManager es la configuración estándar?
   ¿Qué determina el número óptimo?

3. ¿El "operator chaining" de Flink es equivalente al "pipeline fusion"
   de Spark (narrow transformations en el mismo stage)?

4. Si el JobManager falla, ¿qué pasa con los jobs en ejecución?
   ¿Cómo funciona la alta disponibilidad?

5. ¿Cuál es la unidad de paralelismo en Flink y cómo se configura?

**Pista:** El modelo continuo de Flink es posible porque no hay shuffle
entre stages — en Flink, los datos fluyen de operador a operador a través
de canales de red persistentes (network buffers). En Spark, el shuffle
requiere que un stage complete antes de que el siguiente empiece.
En Flink, `map → filter → keyBy → window → aggregate` son operadores
conectados por canales que fluyen continuamente sin barreras entre ellos
(excepto en los puntos de repartición por clave, que sí son similares al shuffle).

---

### Ejercicio 12.1.2 — El flujo de un job: de código Python a ejecución distribuida

```python
import apache_flink
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy

# 1. Crear el entorno de ejecución:
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)  # 4 sub-tasks por operador por defecto

# 2. Definir la fuente:
fuente = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("eventos") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

# 3. Construir el grafo de operadores (no ejecuta aún):
stream = env.from_source(fuente, WatermarkStrategy.no_watermarks(), "kafka-fuente")

resultado = stream \
    .map(lambda msg: msg.upper()) \
    .filter(lambda msg: len(msg) > 10) \
    .key_by(lambda msg: msg[0])  # ← repartición por clave (equivale al shuffle)

# 4. Ejecutar (bloquea hasta que el job termina o falla):
env.execute("mi-primer-job")
```

```
Lo que ocurre al llamar execute():

1. Flink compila el grafo de operadores en un JobGraph
2. El cliente envía el JobGraph al JobManager
3. El JobManager lo distribuye a los TaskManagers disponibles
4. Cada TaskManager inicia sus Task Slots con los operadores asignados
5. Los operadores empiezan a procesar el stream continuamente
6. execute() bloquea en el cliente hasta que el job termina (o falla)
```

**Preguntas:**

1. ¿`env.execute()` es bloqueante o no? ¿Cómo lanzar el job de forma asíncrona?

2. ¿El `set_parallelism(4)` significa que habrá 4 TaskManagers?
   ¿O 4 task slots por operador?

3. ¿`key_by()` genera un shuffle en Flink? ¿Qué datos se mueven por la red?

4. ¿Qué diferencia hay entre `map()` en Flink y `map()` en Spark?
   ¿Cuándo se ejecuta la función lambda?

5. Si el job falla en el paso 5, ¿el cliente recibe una excepción inmediatamente?

---

### Ejercicio 12.1.3 — Operator chaining y el grafo de ejecución

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)

# Este pipeline:
stream = env.from_collection([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
resultado = stream \
    .map(lambda x: x * 2)        # operador 1 — narrow
    .filter(lambda x: x > 5)     # operador 2 — narrow
    .key_by(lambda x: x % 3)     # repartición por clave → barrera de red
    .reduce(lambda a, b: a + b)  # operador 3 — stateful

# Flink "encadena" los operadores 1 y 2 en una sola task (operator chaining):
# map → filter se ejecutan en el mismo thread sin serialización entre ellos.
# key_by() introduce una barrera: los datos se redistribuyen por red.
# reduce() se ejecuta en el thread que recibe los datos ya redistribuidos.

# Ver el plan de ejecución (en la Flink Web UI o imprimiéndolo):
print(env.get_execution_plan())
```

**Preguntas:**

1. ¿Qué condiciones deben cumplirse para que dos operadores puedan
   encadenarse (operator chaining)?

2. ¿El operador `reduce()` después del `key_by()` puede encadenarse
   con el operador anterior? ¿Por qué no?

3. ¿Cómo deshabilitas el operator chaining para un operador específico?
   ¿Cuándo querrías hacerlo?

4. ¿El `key_by()` de Flink es exactamente igual al shuffle de Spark?
   ¿Hay diferencias en la implementación?

5. ¿El grafo de ejecución de Flink puede tener ciclos (feedback loops)?
   ¿Para qué se usarían?

**Pista:** El operator chaining requiere: mismo paralelismo, conexión
forward (no repartición), y que ambos operadores estén en el mismo slot
sharing group. El `key_by()` rompe el chaining porque introduce una
conexión hash-partitioned (datos deben ir a la partición que tiene
la clave correspondiente). En Flink los ciclos son posibles con
`IterativeStream` para algoritmos iterativos (PageRank, k-means)
— uno de los casos donde Flink supera a Spark, que requiere múltiples jobs.

---

### Ejercicio 12.1.4 — Paralelismo: configurar el nivel correcto

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

# Paralelismo global (default para todos los operadores):
env.set_parallelism(8)

# Paralelismo por operador (sobreescribe el global):
stream = env.from_source(fuente, watermark_strategy, "kafka")

resultado = stream \
    .map(parse_json).set_parallelism(8) \        # 8 instancias del mapper
    .filter(es_valido).set_parallelism(8) \      # 8 instancias del filtro
    .key_by(lambda e: e["region"]) \             # repartición por región
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .aggregate(sumar_monto).set_parallelism(4)   # solo 4 regiones → 4 parallelism

# El número de particiones de Kafka limita el paralelismo de la fuente:
# Si Kafka tiene 12 particiones, la fuente tiene como máximo 12 sub-tasks útiles.
# Con más sub-tasks que particiones, algunas quedan inactivas.

# El paralelismo del sink también importa:
resultado.add_sink(kafka_sink).set_parallelism(8)
```

**Preguntas:**

1. Si la fuente de Kafka tiene 6 particiones y el paralelismo es 12,
   ¿qué pasa con los 6 sub-tasks de más?

2. ¿El paralelismo puede cambiarse en un job en ejecución sin reiniciarlo?

3. Para un job que procesa 1M eventos/segundo, ¿cómo calculas el paralelismo
   óptimo dado que cada evento tarda 50 microsegundos en procesarse?

4. ¿Qué pasa con el estado si aumentas el paralelismo del operador stateful?
   ¿El estado se redistribuye automáticamente?

5. ¿Cuándo es contraproducente aumentar el paralelismo?

**Pista:** Cambiar el paralelismo de un operador stateful en un job en ejecución
requiere un "rescaling" — Flink toma un savepoint, redistribuye el estado
entre el nuevo número de sub-tasks, y reinicia. Con `parallelism=12` pero
6 particiones de Kafka, los 6 sub-tasks extras están activos pero sin datos —
consumen recursos sin aportar. La regla: el paralelismo de la fuente ≤
número de particiones del topic de Kafka.

---

### Ejercicio 12.1.5 — Leer: diagnosticar un job lento en la Flink Web UI

**Tipo: Diagnosticar**

La Flink Web UI muestra un job con este comportamiento:

```
Job: metricas-streaming
  Status: RUNNING

  Vertices (operadores):
    Source: kafka-eventos      [12 subtasks]  throughput: 450K/s  backpressure: OK
    map: parse_json            [12 subtasks]  throughput: 390K/s  backpressure: OK
    filter: filtrar_activos    [12 subtasks]  throughput: 310K/s  backpressure: OK
    keyBy → window → aggregate [12 subtasks]  throughput: 45K/s   backpressure: HIGH !!
    Sink: delta-lake           [4 subtasks]   throughput: 45K/s   backpressure: OK

  Checkpoints:
    Last checkpoint: hace 8 minutos (SLA: < 1 minuto) !!
    Checkpoint duration: 7m 23s
    Checkpoint size: 28 GB
```

**Preguntas:**

1. ¿Qué significa "backpressure: HIGH" en el operador de windowing?

2. ¿Por qué el throughput cae de 310K/s a 45K/s en el operador de windowing?

3. ¿Por qué el checkpoint tarda 7 minutos y pesa 28 GB?

4. ¿Qué relación hay entre el backpressure del windowing y el tiempo
   de checkpoint?

5. Propón tres acciones de diagnóstico antes de aplicar cualquier cambio.

**Pista:** El backpressure en Flink funciona como un mecanismo de control de flujo:
si el operador de windowing no puede consumir datos tan rápido como llegan,
acumula datos en los network buffers. Cuando los buffers se llenan, el operador
anterior (filter) se bloquea — se "propaga hacia atrás" el backpressure.
El checkpoint lento es consecuencia del estado grande del operador de windowing:
28 GB de estado es inusual para una ventana de 5 minutos — sugiere cardinalidad
extrema (muchas claves únicas), una ventana muy larga, o estado acumulado sin
limpiar (watermark no avanzando).

---

## Sección 12.2 — DataStream API: Transformaciones sobre Streams

### Ejercicio 12.2.1 — Transformaciones básicas: map, filter, flatMap, keyBy

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import MapFunction, FilterFunction, FlatMapFunction
from pyflink.common.typeinfo import Types
import json

env = StreamExecutionEnvironment.get_execution_environment()

# Datos de prueba:
stream = env.from_collection([
    '{"user_id": "alice", "tipo": "compra", "monto": 150.0}',
    '{"user_id": "bob",   "tipo": "vista",  "monto": 0.0}',
    '{"user_id": "alice", "tipo": "compra", "monto": 300.0}',
    '{"user_id": "carol", "tipo": "compra", "monto": 50.0}',
    '{"user_id": "bob",   "tipo": "compra", "monto": 200.0}',
])

# map: transformar cada elemento
stream_parsed = stream.map(
    lambda msg: json.loads(msg),
    output_type=Types.MAP(Types.STRING(), Types.FLOAT())
)

# filter: eliminar elementos
solo_compras = stream_parsed.filter(
    lambda e: e["tipo"] == "compra"
)

# flatMap: un elemento puede generar 0, 1, o N elementos
# Ejemplo: expandir cada compra en sus ítems individuales
stream_items = stream_parsed.flat_map(
    lambda e, collector: [
        collector.collect({"item": f"item_{i}", "monto": e["monto"] / 3})
        for i in range(3)
    ] if e["tipo"] == "compra" else None
)

# keyBy: repartir por clave (después de esto, todos los registros
# con la misma clave van al mismo sub-task)
por_usuario = solo_compras.key_by(lambda e: e["user_id"])

# Imprimir resultado (para debugging):
por_usuario.print()
env.execute("transformaciones-basicas")
```

**Preguntas:**

1. ¿`key_by()` devuelve un `KeyedStream` o un `DataStream`?
   ¿Qué operaciones solo están disponibles en `KeyedStream`?

2. ¿Las funciones lambda en PyFlink tienen el mismo rendimiento que
   las funciones implementadas como clases (`MapFunction`, `FilterFunction`)?

3. ¿`flatMap` que retorna `None` para algunos elementos actúa como un filtro?

4. Si tienes `key_by("user_id")` y un usuario tiene 1M registros
   pero el resto tienen < 1000, ¿qué problema surge?

5. ¿Puedes hacer `key_by` por múltiples columnas a la vez?

---

### Ejercicio 12.2.2 — ProcessFunction: control total sobre el stream

```python
from pyflink.datastream import ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class DetectorPrimeraCompra(ProcessFunction):
    """
    Detecta la primera compra de cada usuario.
    Emite un evento especial "primera_compra" la primera vez que
    un usuario hace una compra.
    
    ProcessFunction da acceso a:
    - el estado por clave (ya visto un usuario)
    - el timer service (programar callbacks en el futuro)
    - el contexto de tiempo (event time, processing time)
    """
    
    def open(self, runtime_context):
        """Inicializar el estado (llamado una vez por sub-task)."""
        descriptor = ValueStateDescriptor(
            "primera_compra_vista",
            Types.BOOLEAN()
        )
        self.ya_vio_compra = runtime_context.get_state(descriptor)
    
    def process_element(self, evento, ctx: ProcessFunction.Context, out):
        """
        Llamado para cada elemento del stream.
        
        Args:
            evento: el elemento del stream
            ctx: contexto con acceso a tiempo y timers
            out: collector para emitir resultados
        """
        if evento["tipo"] != "compra":
            return
        
        # Acceder al estado de esta clave (user_id):
        ya_compro = self.ya_vio_compra.value()
        
        if not ya_compro:
            # Primera compra: emitir evento especial y actualizar estado
            self.ya_vio_compra.update(True)
            out.collect({
                "tipo": "primera_compra",
                "user_id": evento["user_id"],
                "monto": evento["monto"],
                "timestamp": ctx.timestamp(),
            })
        # Si ya compró antes, no emitir nada

# Usar:
stream_alertas = por_usuario.process(
    DetectorPrimeraCompra(),
    output_type=Types.MAP(Types.STRING(), Types.STRING())
)
```

**Preguntas:**

1. ¿El estado de `DetectorPrimeraCompra` se comparte entre usuarios?
   ¿Cuántas instancias de `ya_vio_compra` existen para 1M usuarios?

2. ¿`ProcessFunction` puede emitir múltiples elementos por cada elemento
   de entrada? ¿Y cero elementos?

3. ¿La función `process_element` se llama en orden de llegada o
   en orden de event time?

4. ¿El `ctx.timestamp()` retorna el event time o el processing time?

5. ¿Cómo implementarías la misma lógica con `filter` y `map` en lugar
   de `ProcessFunction`? ¿Qué información faltaría?

**Pista:** El estado de Flink es **por clave** — hay una instancia del estado
`ya_vio_compra` para cada `user_id` único. Con 1M usuarios, hay 1M entradas
en el estado. Flink gestiona esto eficientemente con RocksDB (cuando el estado
no cabe en memoria) — los valores se serializan y guardan en disco,
con un cache LRU en memoria para las claves recientes.
`filter + map` no puede implementar esta lógica porque necesita "recordar"
si ya vio una compra de ese usuario en batches anteriores — lo que requiere estado.

---

### Ejercicio 12.2.3 — Timers: programar acciones en el futuro

```python
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class AlertaInactividad(KeyedProcessFunction):
    """
    Alerta si un usuario no hace ninguna acción en 30 minutos.
    
    Usa timers para programar una "alarma" cada vez que llega
    un evento, y cancelarla si llega otro evento antes.
    """
    
    TIMEOUT_MS = 30 * 60 * 1000  # 30 minutos en milisegundos
    
    def open(self, runtime_context):
        self.ultimo_timer = runtime_context.get_state(
            ValueStateDescriptor("ultimo_timer", Types.LONG())
        )
    
    def process_element(self, evento, ctx: KeyedProcessFunction.Context, out):
        # Cancelar el timer anterior (si existe):
        timer_anterior = self.ultimo_timer.value()
        if timer_anterior is not None:
            ctx.timer_service().delete_processing_time_timer(timer_anterior)
        
        # Programar un nuevo timer en 30 minutos desde ahora:
        nuevo_timer = ctx.timer_service().current_processing_time() + self.TIMEOUT_MS
        ctx.timer_service().register_processing_time_timer(nuevo_timer)
        self.ultimo_timer.update(nuevo_timer)
    
    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext, out):
        """
        Llamado cuando el timer dispara (30 minutos sin actividad).
        """
        user_id = ctx.get_current_key()
        out.collect({
            "tipo": "inactividad",
            "user_id": user_id,
            "timestamp": timestamp,
            "mensaje": f"Usuario {user_id} inactivo durante 30 minutos",
        })
        self.ultimo_timer.clear()

# Usar:
alertas = por_usuario.process(
    AlertaInactividad(),
    output_type=Types.MAP(Types.STRING(), Types.STRING())
)
```

**Preguntas:**

1. ¿`register_processing_time_timer` vs `register_event_time_timer`:
   cuándo usar cada uno?

2. Si el job falla y se recupera desde un checkpoint, ¿los timers
   pendientes se restauran?

3. ¿Puede un usuario tener múltiples timers activos simultáneamente?

4. ¿Qué pasa con un timer programado para dentro de 30 minutos
   si el job no recibe más datos de ese usuario en ese período?

5. ¿Los timers de Flink son exactos? ¿Pueden dispararse con retraso?

**Pista:** Los timers de event time solo disparan cuando el watermark
supera el timestamp del timer — si los datos se detienen, el watermark
no avanza y los timers no disparan. Los timers de processing time
disparan según el reloj del sistema del TaskManager, independientemente
del event time. Ambos tipos de timers se guardan en el checkpoint
y se restauran después de un fallo.

---

### Ejercicio 12.2.4 — Side outputs: múltiples salidas de un operador

```python
from pyflink.datastream import ProcessFunction, OutputTag
from pyflink.common.typeinfo import Types

# Los side outputs permiten que un operador emita a múltiples streams:
TAG_ERRORES = OutputTag("errores", Types.STRING())
TAG_TARDIOS = OutputTag("tardios", Types.MAP(Types.STRING(), Types.STRING()))

class RouterEventos(ProcessFunction):
    """
    Clasifica los eventos en tres categorías:
    - Salida principal: eventos válidos para procesamiento normal
    - Side output errores: eventos con schema inválido
    - Side output tardíos: eventos con timestamp muy antiguo
    """
    
    THRESHOLD_TARDIO_MS = 5 * 60 * 1000  # 5 minutos
    
    def process_element(self, msg, ctx, out):
        try:
            evento = json.loads(msg)
        except json.JSONDecodeError as e:
            # Emitir al side output de errores:
            ctx.output(TAG_ERRORES, f"JSON inválido: {msg[:100]} — {e}")
            return
        
        # Verificar si es tardío:
        ahora_ms = ctx.timer_service().current_processing_time()
        evento_ms = evento.get("timestamp_ms", ahora_ms)
        
        if ahora_ms - evento_ms > self.THRESHOLD_TARDIO_MS:
            ctx.output(TAG_TARDIOS, evento)
            return
        
        # Emitir al stream principal:
        out.collect(evento)

# Obtener el stream principal y los side outputs:
stream_router = stream.process(
    RouterEventos(),
    output_type=Types.MAP(Types.STRING(), Types.STRING())
)

stream_errores = stream_router.get_side_output(TAG_ERRORES)
stream_tardios = stream_router.get_side_output(TAG_TARDIOS)

# Cada stream puede tener su propio sink:
stream_router.add_sink(sink_principal)
stream_errores.add_sink(sink_errores_dlq)   # dead letter queue
stream_tardios.add_sink(sink_tardios)
```

**Preguntas:**

1. ¿Los side outputs de Flink son equivalentes a los Dead Letter Queues
   del Cap.09 (Kafka)? ¿Qué diferencias hay?

2. ¿Un operador puede tener cuántos side outputs simultáneamente?

3. ¿Los side outputs afectan el rendimiento del stream principal?

4. ¿Cómo implementarías la misma lógica sin side outputs?
   ¿Qué desventajas tendría?

---

### Ejercicio 12.2.5 — Conectar dos streams: union y connect

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

# UNION: combinar streams del mismo tipo
stream_clicks = env.from_collection([
    {"tipo": "click", "user_id": "alice"},
    {"tipo": "click", "user_id": "bob"},
])
stream_compras = env.from_collection([
    {"tipo": "compra", "user_id": "alice", "monto": 100.0},
])

# Union: merge simple, mismo tipo
stream_todos_eventos = stream_clicks.union(stream_compras)
# El stream resultante contiene todos los elementos de ambos streams

# CONNECT: combinar streams de tipos distintos con lógica por tipo
from pyflink.datastream import CoMapFunction

stream_str = env.from_collection(["hola", "mundo"])
stream_int = env.from_collection([1, 2, 3])

class ProcesarDosStreams(CoMapFunction):
    def map1(self, value):
        # Procesar elementos del primer stream (str):
        return f"STRING: {value}"
    
    def map2(self, value):
        # Procesar elementos del segundo stream (int):
        return f"INT: {value * 2}"

stream_conectado = stream_str \
    .connect(stream_int) \
    .map(ProcesarDosStreams())
```

**Preguntas:**

1. ¿`union` preserva el orden de los elementos entre los dos streams?

2. ¿`connect` puede usarse para hacer un join entre dos streams?
   ¿Qué le falta para ser un join completo?

3. ¿Cuál es la diferencia entre `union` y `connect` en términos
   de paralelismo y scheduling?

4. ¿Para qué caso de uso usarías `connect` en lugar de un join
   convencional?

---

## Sección 12.3 — Estado en Flink: el Diferenciador Principal

### Ejercicio 12.3.1 — Los tipos de estado en Flink

```python
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
    MapStateDescriptor,
    ReducingStateDescriptor,
    AggregatingStateDescriptor,
)
from pyflink.common.typeinfo import Types

class OperadorConEstado(KeyedProcessFunction):
    
    def open(self, runtime_context):
        # ValueState: un único valor por clave
        self.ultimo_monto = runtime_context.get_state(
            ValueStateDescriptor("ultimo_monto", Types.FLOAT())
        )
        
        # ListState: lista de valores por clave
        self.historial = runtime_context.get_list_state(
            ListStateDescriptor("historial", Types.FLOAT())
        )
        
        # MapState: mapa clave→valor por clave
        self.conteo_por_tipo = runtime_context.get_map_state(
            MapStateDescriptor("conteo_tipo", Types.STRING(), Types.LONG())
        )
        
        # ReducingState: aplica una función de reducción automáticamente
        self.suma_total = runtime_context.get_reducing_state(
            ReducingStateDescriptor(
                "suma_total",
                lambda a, b: a + b,
                Types.FLOAT()
            )
        )
    
    def process_element(self, evento, ctx, out):
        # Actualizar ValueState:
        self.ultimo_monto.update(evento["monto"])
        
        # Añadir a ListState:
        self.historial.add(evento["monto"])
        
        # Actualizar MapState:
        tipo = evento["tipo"]
        conteo_actual = self.conteo_por_tipo.get(tipo) or 0
        self.conteo_por_tipo.put(tipo, conteo_actual + 1)
        
        # Actualizar ReducingState (aplica la función de reducción automáticamente):
        self.suma_total.add(evento["monto"])
        
        # Leer el estado:
        out.collect({
            "user_id": ctx.get_current_key(),
            "ultimo_monto": self.ultimo_monto.value(),
            "suma_total": self.suma_total.get(),
            "num_compras": self.conteo_por_tipo.get("compra") or 0,
        })
```

**Preguntas:**

1. ¿`ValueState`, `ListState`, `MapState` — cuándo usar cada uno?
   Da un ejemplo de caso de uso para cada tipo.

2. ¿El `ListState` puede crecer indefinidamente? ¿Qué lo limita?

3. ¿El `ReducingState` vs `AggregatingState` — cuál es la diferencia?

4. ¿Cómo se serializa el estado para guardarlo en el checkpoint?
   ¿Afecta al rendimiento?

5. Si cambias el tipo de un estado (ej: de `ValueState<Float>` a
   `ValueState<Double>`) y reinicias con el checkpoint anterior,
   ¿qué ocurre?

**Pista:** `ReducingState` aplica la función en el momento de añadir el valor
(el estado siempre contiene el resultado reducido, no los valores individuales).
`AggregatingState` es más flexible: permite inputs y outputs de tipos diferentes
(ej: añades `Float` y el estado guarda `(suma, count)` para calcular la media).
Cambiar el tipo de un estado es incompatible con el checkpoint existente —
Flink lanzará una excepción al intentar deserializar el estado con el nuevo tipo.
La solución: usar un nuevo nombre para el estado o migrar con un savepoint.

---

### Ejercicio 12.3.2 — RocksDB: estado más grande que la RAM

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend, HashMapStateBackend

# Backend de estado por defecto: HashMapStateBackend (en memoria JVM heap)
# Límite: el estado debe caber en la memoria del TaskManager

# Para estado grande: EmbeddedRocksDBStateBackend
env = StreamExecutionEnvironment.get_execution_environment()
env.set_state_backend(EmbeddedRocksDBStateBackend(incremental=True))

# Con RocksDB:
# - El estado se guarda en disco local del TaskManager
# - Se usa un cache LRU en memoria para las claves más recientes
# - Los checkpoints son incrementales (solo se guarda el delta)
# - El estado puede ser mucho mayor que la RAM disponible

# Configuración recomendada para RocksDB en producción:
env.get_configuration().set_string(
    "state.backend.rocksdb.memory.managed", "true"
)
env.get_configuration().set_string(
    "state.backend.rocksdb.memory.fixed-per-slot", "256mb"
)
```

```
Comparación de backends de estado:

                    HashMapStateBackend    EmbeddedRocksDBStateBackend
Almacenamiento      JVM heap              Disco local + cache en memoria
Tamaño máximo       = RAM del TaskManager >> RAM (limitado por disco)
Serialización       On demand             Siempre (para guardar en disco)
Latencia de acceso  O(1) en memoria       O(1) para cache, O(disk) para miss
Checkpoint          Full snapshot         Incremental (solo el delta)
GC pressure         Alta (JVM GC)         Baja (RocksDB gestiona su memoria)
```

**Preguntas:**

1. ¿Cuándo elegirías `HashMapStateBackend` sobre `EmbeddedRocksDBStateBackend`?

2. ¿Los checkpoints incrementales de RocksDB ahorran tiempo de checkpoint
   independientemente del tamaño del estado?

3. Si el disco local del TaskManager falla, ¿se pierde el estado de RocksDB?
   ¿Cómo lo recupera Flink?

4. ¿El cache LRU de RocksDB puede configurarse? ¿Cómo afecta al rendimiento?

5. ¿Por qué el GC pressure es menor con RocksDB que con HashMapStateBackend?

**Pista:** El disco local del TaskManager es temporal — si falla, Flink
recupera el estado desde el checkpoint en almacenamiento remoto (S3, HDFS).
El estado local de RocksDB es solo la versión de trabajo; el estado durables
está en el checkpoint. Por eso los checkpoints frecuentes son especialmente
importantes con RocksDB: el tiempo de recuperación depende de cuántos eventos
se reprocesen desde el último checkpoint.

---

### Ejercicio 12.3.3 — QueryableState: consultar el estado desde fuera del job

```python
# QueryableState permite que aplicaciones externas consulten el estado
# del job de Flink sin que Flink emita ese estado explícitamente.
# Útil para dashboards en tiempo real o servicios que necesitan el "estado actual".

from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import KeyedProcessFunction
from pyflink.common.typeinfo import Types

class EstadoQueryable(KeyedProcessFunction):
    
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("saldo_actual", Types.FLOAT())
        # Hacer el estado consultable desde fuera del job:
        self.saldo = runtime_context.get_state(descriptor)
        # Nota: en Flink, el QueryableState se configura al crear el descriptor
        # La API varía según la versión — consultar la documentación actualizada

    def process_element(self, transaccion, ctx, out):
        saldo_actual = self.saldo.value() or 0.0
        nuevo_saldo = saldo_actual + transaccion["monto"]
        self.saldo.update(nuevo_saldo)
        out.collect({"user_id": ctx.get_current_key(), "saldo": nuevo_saldo})

# Desde una aplicación externa (cliente de QueryableState):
# client = QueryableStateClient("jobmanager-host", 9069)
# saldo_alice = client.get_kv_state("saldo_actual", "alice", Types.STRING(), Types.FLOAT())
```

> ⚙️ Versión: QueryableState está disponible pero marcado como experimental
> en algunas versiones de Flink. Verificar el estado de soporte en la versión
> que uses en producción. La alternativa más robusta: emitir el estado
> explícitamente a un sistema de lectura rápida (Redis, Cassandra).

**Preguntas:**

1. ¿QueryableState tiene garantías de consistencia? ¿O puede retornar
   un estado parcialmente actualizado?

2. ¿Es más eficiente QueryableState o emitir el estado a Redis?
   ¿Depende del patrón de acceso?

3. ¿QueryableState funciona bien con múltiples sub-tasks del operador?
   ¿Cómo sabe el cliente en qué sub-task está la clave que busca?

---

### Ejercicio 12.3.4 — Gestión del ciclo de vida del estado: TTL

```python
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common.time import Time

# TTL (Time-To-Live): limpiar estado que ya no se necesita
# Sin TTL: el estado crece indefinidamente para claves "muertas"
# Con TTL: las entradas se limpian después de un período de inactividad

ttl_config = StateTtlConfig \
    .new_builder(Time.hours(24)) \
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
    .build()

descriptor = ValueStateDescriptor("ultimo_evento", Types.STRING())
descriptor.enable_time_to_live(ttl_config)

# Con esta configuración:
# - El estado se mantiene 24 horas desde la última escritura
# - Si no hay actividad en 24 horas, la entrada se limpia
# - Las entradas expiradas no son visibles para el código de usuario
# - La limpieza ocurre en background (o en el próximo acceso)
```

**Preguntas:**

1. ¿El TTL de Flink usa event time o processing time?
   ¿Cuáles son las implicaciones de cada opción?

2. ¿La limpieza del estado expirado ocurre inmediatamente o de forma lazy?
   ¿Puede configurarse?

3. ¿El estado expirado pero no limpiado todavía ocupa espacio en el checkpoint?

4. ¿Cómo interacciona el TTL con los checkpoints?
   ¿Un estado expirado se incluye en el checkpoint?

5. ¿Cuándo el TTL no es suficiente y necesitas limpiar el estado
   de forma más explícita?

**Pista:** El TTL usa processing time por defecto (el tiempo del reloj del sistema
del TaskManager). Usar event time para TTL es más complejo porque el event time
solo avanza cuando llegan datos — si no llegan datos de una clave durante
24 horas, pero el job sigue recibiendo datos de otras claves, el TTL de
event time no limpiará esa entrada hasta que lleguen datos que avancen el
watermark más allá del timestamp de expiración. Processing time es más predecible
para TTL, aunque puede no ser lo que el negocio espera si los datos llegan
con retraso.

---

### Ejercicio 12.3.5 — Leer: diseñar el estado para un sistema de sesiones

**Tipo: Diseñar**

Un sistema de e-commerce necesita calcular "sesiones de usuario" en streaming:
- Una sesión empieza con el primer evento de un usuario
- Una sesión termina si no hay actividad en 30 minutos
- Al terminar la sesión, emitir métricas: duración, número de eventos, valor total

Diseñar la estructura de estado necesaria:

```python
# Esquema del estado — a completar:
class SistemaSessiones(KeyedProcessFunction):
    
    def open(self, runtime_context):
        # ¿Qué estado necesitas?
        # ¿ValueState? ¿ListState? ¿MapState? ¿Varios?
        
        # Estado para la sesión actual:
        self.sesion_activa = runtime_context.get_state(
            ValueStateDescriptor("sesion_activa", ...)  # ¿qué tipo?
        )
        
        # Estado para el timer de inactividad:
        self.timer_inactividad = runtime_context.get_state(
            ValueStateDescriptor("timer_inactividad", Types.LONG())
        )
```

**Restricciones:**
1. Completar el diseño del estado con justificación
2. Implementar `process_element` y `on_timer`
3. ¿Qué información necesitas guardar en el estado para calcular
   las métricas de la sesión?
4. ¿Cómo garantizas que la sesión se cierra correctamente si el job
   se reinicia después de un fallo?

---

## Sección 12.4 — Tiempo y Watermarks: Event Time Nativo

### Ejercicio 12.4.1 — Las tres nociones de tiempo en Flink

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Duration

env = StreamExecutionEnvironment.get_execution_environment()

# 1. Processing Time: el reloj del sistema del TaskManager
#    Ventaja: simple, no requiere timestamps en los datos
#    Desventaja: no determinista (mismo job = resultados distintos)
env.set_stream_time_characteristic("ProcessingTime")  # legacy API
# O con WatermarkStrategy:
strategy_processing = WatermarkStrategy.for_monotonous_timestamps()

# 2. Event Time: el timestamp del evento cuando ocurrió
#    Ventaja: determinista, correcto para datos tardíos
#    Desventaja: requiere timestamps en los eventos y watermark logic

# Asignar timestamps desde el campo del evento:
class AsignadorTimestamp(TimestampAssigner):
    def extract_timestamp(self, elemento, timestamp_previo):
        return elemento["timestamp_ms"]  # long en milisegundos

strategy_event_time = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
    .with_timestamp_assigner(AsignadorTimestamp())

stream = env.from_source(fuente, strategy_event_time, "kafka")

# 3. Ingestion Time: el timestamp de cuando llegó a Flink
#    Entre procesamiento y event time — más predecible que processing time
#    pero menos correcto que event time
strategy_ingestion = WatermarkStrategy.for_monotonous_timestamps()
# (el assigner usa el timestamp de ingesta automáticamente)
```

**Preguntas:**

1. ¿Por qué el processing time es "no determinista"?
   Da un ejemplo donde el mismo dataset produce resultados diferentes.

2. ¿Cuándo es apropiado usar processing time en producción?

3. ¿`for_bounded_out_of_orderness(Duration.of_seconds(10))` significa
   que el watermark siempre está 10 segundos por detrás del evento
   más reciente?

4. ¿El watermark puede avanzar más rápido que los datos llegan?
   ¿Qué pasaría si el watermark avanzara más rápido?

5. Si la fuente es Kafka con 12 particiones, ¿cómo se calcula el watermark
   global del stream?

**Pista:** El watermark global de Kafka es el mínimo de los watermarks
de las 12 particiones. Si la partición 7 no recibe datos durante 5 minutos,
su watermark no avanza y el watermark global se congela en ese valor.
Esta es la causa más frecuente de "watermark que no avanza" en producción:
una partición vacía o con tráfico muy bajo bloquea el progreso de todos
los operadores que dependen del event time.
La solución: `WatermarkStrategy.with_idleness(Duration.of_minutes(1))`
marca las particiones sin datos como "idle" y el watermark global ignora
sus contribuciones.

---

### Ejercicio 12.4.2 — Implementar un WatermarkGenerator personalizado

```python
from pyflink.common.watermark_strategy import WatermarkGenerator
from pyflink.common.watermark import Watermark

class WatermarkPorPercentil(WatermarkGenerator):
    """
    Watermark basado en percentil: en lugar de usar el evento más reciente
    menos un delay fijo, usa el percentil 5 de los últimos N timestamps.
    
    Más robusto contra outliers que 'for_bounded_out_of_orderness'.
    """
    
    def __init__(self, ventana_eventos: int = 1000, percentil: float = 0.05):
        self.ventana = ventana_eventos
        self.percentil = percentil
        self.buffer = []
    
    def on_event(self, evento, evento_timestamp: int, output: "WatermarkOutput"):
        """Llamado para cada evento."""
        self.buffer.append(evento_timestamp)
        if len(self.buffer) > self.ventana:
            self.buffer.pop(0)
        # No emitir watermark en cada evento — demasiado costoso.
        # El watermark se emite en on_periodic_emit.
    
    def on_periodic_emit(self, output: "WatermarkOutput"):
        """Llamado periódicamente (configurable, default: 200ms)."""
        if len(self.buffer) >= 10:
            timestamps_sorted = sorted(self.buffer)
            idx = int(len(timestamps_sorted) * self.percentil)
            watermark_ts = timestamps_sorted[idx]
            output.emit_watermark(Watermark(watermark_ts))
```

**Restricciones:**
1. Implementar el WatermarkGenerator completo
2. ¿Cuándo `WatermarkPorPercentil` es mejor que `for_bounded_out_of_orderness`?
3. ¿Qué pasa si el buffer tiene timestamps de eventos de distintas particiones
   de Kafka mezclados?
4. Medir la latencia extra que introduce esperar al percentil vs usar el máximo

---

### Ejercicio 12.4.3 — Leer: el watermark que no avanzaba

**Tipo: Diagnosticar**

Un job de Flink tiene el siguiente comportamiento:

```
Evento time del stream: avanza normalmente (eventos de hoy)
Watermark: congelado en "2024-01-15 09:23:41" desde hace 2 horas
Ventanas de 5 minutos: no se están cerrando
Estado: creciendo continuamente

Configuración:
  - Fuente: Kafka con 16 particiones
  - Estrategia: for_bounded_out_of_orderness(Duration.of_minutes(5))
  - Paralelismo: 8

Métricas de Kafka por partición:
  Partition 0-6:   activas, 100-200 msgs/segundo
  Partition 7:     0 msgs/segundo — congelada en offset 45,231
  Partition 8-15:  activas, 100-200 msgs/segundo
```

**Preguntas:**

1. ¿Por qué la partición 7 congela el watermark global?

2. ¿El watermark congelado en `09:23:41` corresponde al último evento
   de la partición 7? ¿Cuándo ocurrió ese evento?

3. ¿Cómo confirmarías este diagnóstico en la Flink Web UI?

4. ¿Cuáles son las posibles causas de que la partición 7 esté vacía?

5. ¿Cómo resuelves el problema sin detener el job?

**Pista:** La partición 7 produjo el último mensaje a las 09:23:41 y luego
se detuvo. El watermark global es el mínimo de los watermarks de las 16
particiones — como la partición 7 no avanza, el watermark global tampoco.
La solución inmediata: añadir `with_idleness(Duration.of_minutes(2))` —
si una partición no produce datos en 2 minutos, se marca como idle y el
watermark global la ignora. Esto permite que las ventanas avancen aunque
una partición esté vacía. La causa raíz (partición vacía) puede ser un
productor que falló, un rebalance de Kafka mal gestionado, o un topic
con datos estacionales donde algunas particiones genuinamente se quedan sin datos.

---

### Ejercicio 12.4.4 — Event time vs Processing time en un pipeline real

```python
# Mismo pipeline, dos versiones: event time vs processing time
# Demostrar la diferencia con datos tardíos

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import WatermarkStrategy, Time
from pyflink.common.time import Duration

env = StreamExecutionEnvironment.get_execution_environment()

# Simular un stream con datos tardíos:
# Eventos: [ts=10:00:01, ts=10:00:02, ts=10:00:03, ts=09:59:55 (tardío!), ts=10:00:04]

eventos = [
    {"ts_ms": 1700000001000, "valor": 10},  # 10:00:01
    {"ts_ms": 1700000002000, "valor": 20},  # 10:00:02
    {"ts_ms": 1700000003000, "valor": 30},  # 10:00:03
    {"ts_ms": 1699999995000, "valor": 99},  # 09:59:55 — tardío 6 segundos
    {"ts_ms": 1700000004000, "valor": 40},  # 10:00:04
]

stream = env.from_collection(eventos)

# Versión Event Time: el tardío puede incluirse en la ventana 09:59:00-10:00:00
strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
    .with_timestamp_assigner(lambda e, _: e["ts_ms"])

stream_et = stream \
    .assign_timestamps_and_watermarks(strategy) \
    .key_by(lambda _: "global") \
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .sum("valor")

# Versión Processing Time: el tardío entra en la ventana "actual" (no donde pertenece)
stream_pt = stream \
    .key_by(lambda _: "global") \
    .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) \
    .sum("valor")
```

**Restricciones:**
1. Ejecutar ambas versiones y comparar los resultados
2. ¿El evento tardío (09:59:55) aparece en la ventana correcta
   con event time? ¿Y con processing time?
3. ¿Con event time, el evento tardío se descarta si el watermark
   ya superó las 09:59:55 + 10 segundos?

---

### Ejercicio 12.4.5 — El pipeline completo con event time y manejo de tardíos

```python
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time

# Pipeline production-ready con manejo completo de datos tardíos:
stream_con_watermark = stream \
    .assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(2))
        .with_idleness(Duration.of_minutes(5))  # ignorar particiones idle
        .with_timestamp_assigner(AsignadorTimestamp())
    )

resultado = stream_con_watermark \
    .key_by(lambda e: e["region"]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .allowed_lateness(Time.minutes(2)) \
    .side_output_late_data(TAG_TARDIOS)  \
    .aggregate(AgregadorRevenue())

# Con allowed_lateness:
# - La ventana se cierra normalmente cuando el watermark la supera
# - Si llega un dato tardío (pero dentro de allowed_lateness), la ventana
#   re-emite el resultado actualizado
# - Los datos con más retraso que allowed_lateness van al side output

stream_tardios = resultado.get_side_output(TAG_TARDIOS)
stream_tardios.add_sink(sink_tardios_para_reproceso)
```

**Preguntas:**

1. ¿Cuántas veces puede re-emitirse el resultado de una ventana
   con `allowed_lateness`?

2. ¿El sink downstream puede manejar re-emisiones? ¿Qué problemas causa?

3. ¿`allowed_lateness` y `withWatermark` en Spark SS son equivalentes?
   ¿Cuál es la diferencia semántica?

4. ¿Cuánto estado extra ocupa `allowed_lateness` comparado con una
   ventana sin lateness?

---

## Sección 12.5 — Ventanas: el Modelo Completo de Flink

### Ejercicio 12.5.1 — Los cuatro tipos de ventanas

```python
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows,
    EventTimeSessionWindows,
    GlobalWindows,
)
from pyflink.common import Time

# 1. Tumbling: intervalos fijos sin solapamiento
stream.key_by(...) \
      .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
      .sum("monto")
# Ventanas: [0-5min], [5-10min], [10-15min], ...

# 2. Sliding: ventanas con solapamiento
stream.key_by(...) \
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5))) \
      .sum("monto")
# Ventanas: [0-10min], [5-15min], [10-20min], ... (cada evento en 2 ventanas)

# 3. Session: ventana que se cierra cuando no hay actividad
stream.key_by(...) \
      .window(EventTimeSessionWindows.with_gap(Time.minutes(30))) \
      .sum("monto")
# Ventanas: [primer_evento...inactividad_30min], [siguiente_evento...]

# 4. Global: una sola ventana para todas las claves
# (requiere trigger personalizado para emitir resultados)
stream.key_by(...) \
      .window(GlobalWindows.create()) \
      .trigger(CountTrigger.of(1000))  # emitir cada 1000 eventos
      .sum("monto")
```

**Preguntas:**

1. Para calcular la media móvil de 7 días actualizada cada hora,
   ¿qué tipo de ventana usas? ¿Cuántas ventanas activas simultáneamente
   hay para un usuario con actividad diaria?

2. ¿Las ventanas de sesión pueden solaparse entre usuarios?
   ¿Son independientes por clave?

3. ¿Las ventanas de sesión en Flink funcionan con processing time
   igual que con event time?

4. ¿Cuánto estado ocupa una ventana sliding de 7 días con slide de 1 hora
   para 1M usuarios activos?

5. ¿La ventana global es útil en producción? Da un caso de uso real.

---

### Ejercicio 12.5.2 — Window Functions: apply, aggregate, process

```python
from pyflink.datastream.window import TumblingEventTimeWindows, WindowFunction
from pyflink.datastream import ProcessWindowFunction
from pyflink.common import Time

# Opción A: apply (WindowFunction) — acceso a todos los elementos de la ventana
class SumarConContexto(WindowFunction):
    def apply(self, key, window, inputs, out):
        total = sum(e["monto"] for e in inputs)
        out.collect({
            "region": key,
            "inicio_ventana": window.start,
            "fin_ventana": window.end,
            "revenue": total,
        })

# Opción B: aggregate — incrementalmente, más eficiente
class AcumuladorRevenue:
    def create_accumulator(self):
        return {"suma": 0.0, "count": 0}
    
    def add(self, value, accumulator):
        accumulator["suma"] += value["monto"]
        accumulator["count"] += 1
        return accumulator
    
    def get_result(self, accumulator):
        return accumulator
    
    def merge(self, acc1, acc2):
        return {"suma": acc1["suma"] + acc2["suma"],
                "count": acc1["count"] + acc2["count"]}

# Opción C: ProcessWindowFunction — como apply pero con acceso al estado global
class AnalisisVentana(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        elementos = list(elements)
        montos = [e["monto"] for e in elementos]
        out.collect({
            "region": key,
            "revenue": sum(montos),
            "max_transaccion": max(montos),
            "num_transacciones": len(montos),
            "watermark_actual": context.current_watermark(),
        })
```

**Preguntas:**

1. ¿Cuál de las tres opciones usa menos memoria? ¿Por qué?

2. ¿`aggregate` puede calcular la media? ¿El percentil 99?
   ¿Cuál requiere almacenar todos los elementos en memoria?

3. ¿Puedes combinar `aggregate` y `ProcessWindowFunction`
   para tener lo mejor de ambos?

4. ¿`WindowFunction.apply()` se llama una vez por ventana o una vez
   por elemento?

**Pista:** `aggregate` es la opción más eficiente porque procesa los elementos
incrementalmente — no necesita guardar todos los elementos de la ventana en memoria,
solo el acumulador. `apply` y `ProcessWindowFunction` guardan todos los elementos
hasta que la ventana cierra. Para calcular el percentil exacto, necesitas todos
los valores en memoria (no hay acumulador incremental) — usar `apply`.
Para la media: `aggregate` con acumulador `(suma, count)` es más eficiente.
Sí puedes combinar: `.aggregate(acumulador, procesadora)` aplica el acumulador
incrementalmente y luego pasa el resultado al `ProcessWindowFunction` para
enriquecerlo con contexto.

---

### Ejercicio 12.5.3 — Triggers personalizados: controlar cuándo se emite

```python
from pyflink.datastream.window import Trigger, TriggerResult

class TriggerPorCountOTiempo(Trigger):
    """
    Emitir cuando se acumulan N eventos O cuando pasa X tiempo.
    El que llegue primero gana.
    """
    
    def __init__(self, max_count: int, max_time_ms: int):
        self.max_count = max_count
        self.max_time_ms = max_time_ms
    
    def on_element(self, element, timestamp, window, ctx):
        count_state = ctx.get_partitioned_state(
            ValueStateDescriptor("count", Types.LONG())
        )
        count = (count_state.value() or 0) + 1
        count_state.update(count)
        
        if count >= self.max_count:
            count_state.clear()
            return TriggerResult.FIRE_AND_PURGE
        
        # Registrar timer para el timeout:
        ctx.register_processing_time_timer(
            ctx.get_current_processing_time() + self.max_time_ms
        )
        return TriggerResult.CONTINUE
    
    def on_processing_time(self, time, window, ctx):
        return TriggerResult.FIRE_AND_PURGE
    
    def on_event_time(self, time, window, ctx):
        return TriggerResult.CONTINUE
    
    def on_merge(self, window, ctx):
        pass
    
    def clear(self, window, ctx):
        ctx.delete_processing_time_timer(
            ctx.get_current_processing_time() + self.max_time_ms
        )
```

**Preguntas:**

1. ¿`FIRE` vs `FIRE_AND_PURGE` — cuál es la diferencia?

2. ¿Este trigger puede causar que la misma ventana emita resultados
   múltiples veces? ¿Con qué semántica de tiempo?

3. ¿Cuándo un trigger personalizado es necesario vs usar
   los triggers estándar de Flink?

---

### Ejercicio 12.5.4 — CEP (Complex Event Processing): detectar patrones

```python
from pyflink.cep import CEP, Pattern
from pyflink.cep.pattern import Quantifier
from pyflink.common import Time

# CEP permite detectar secuencias de eventos con condiciones temporales.
# Ejemplo: detectar fraude — tres intentos fallidos seguidos de una compra

patron_fraude = Pattern \
    .begin("intento_fallido") \
    .where(lambda e, _: e["tipo"] == "login_fallido") \
    .times(3) \
    .consecutive() \
    .within(Time.minutes(5)) \
    .followedBy("compra_exitosa") \
    .where(lambda e, _: e["tipo"] == "compra")

# Aplicar el patrón al stream:
alertas_fraude = CEP.pattern(stream_por_usuario, patron_fraude) \
    .select(
        lambda pattern: {
            "tipo_alerta": "fraude_potencial",
            "user_id": pattern["intento_fallido"][0]["user_id"],
            "num_intentos": len(pattern["intento_fallido"]),
            "monto_compra": pattern["compra_exitosa"][0]["monto"],
        }
    )

alertas_fraude.add_sink(sink_alertas)
```

**Preguntas:**

1. ¿El CEP de Flink usa estado internamente? ¿Cuánto estado
   puede acumular un patrón de 5 pasos?

2. ¿`consecutive()` en el patrón significa que los eventos deben
   ser contiguos sin ningún evento entre medias?

3. ¿CEP funciona con event time? ¿Qué papel juega el watermark
   para determinar cuándo un patrón "expiró" sin completarse?

4. ¿Puedes detectar la ausencia de un evento? (ej: "no hubo
   confirmación en 10 minutos después del pago")

5. ¿Qué alternativas a Flink CEP existen para detectar patrones?

---

### Ejercicio 12.5.5 — Leer: ventanas en Flink vs ventanas en Spark SS

**Tipo: Comparar**

Para el mismo caso de uso (media móvil de 7 días, emitida cada hora),
comparar la implementación en Flink y Spark SS:

```python
# Flink:
resultado_flink = stream \
    .assign_timestamps_and_watermarks(strategy) \
    .key_by(lambda e: e["producto_id"]) \
    .window(SlidingEventTimeWindows.of(Time.days(7), Time.hours(1))) \
    .aggregate(AcumuladorMedia())

# Spark SS:
resultado_spark = df_stream \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        F.col("producto_id"),
        F.window("timestamp", "7 days", "1 hour")
    ) \
    .agg(F.avg("monto"))
```

**Preguntas:**

1. ¿Cuánto estado necesita cada implementación para 1M productos?

2. ¿La latencia del resultado es diferente? ¿Por qué?

3. ¿Qué pasa con datos tardíos en cada implementación?

4. ¿Cuál es más fácil de operar en producción? ¿Y más fácil de entender?

5. ¿Hay casos donde el resultado de Flink y Spark SS sea diferente
   para los mismos datos de entrada?

---

## Sección 12.6 — Checkpointing y Tolerancia a Fallos

### Ejercicio 12.6.1 — El algoritmo de Chandy-Lamport en Flink

```
El checkpoint de Flink usa una variante del algoritmo de Chandy-Lamport:

1. El JobManager envía una "barrera de checkpoint" a todas las fuentes.

2. Las fuentes inyectan la barrera en el stream (como si fuera un evento especial).

3. Cuando un operador recibe la barrera en TODOS sus canales de entrada:
   a) Toma un snapshot de su estado
   b) Envía la barrera a sus canales de salida
   c) Continúa procesando elementos después de la barrera

4. Cuando el JobManager recibe confirmación de todos los operadores:
   el checkpoint está completo.

5. En paralelo con el procesamiento normal:
   el snapshot del estado se escribe a almacenamiento remoto (S3/HDFS).

Esto garantiza un snapshot consistente sin pausar el job:
  t=0: barrera enviada a fuente A
  t=1ms: fuente A inyecta barrera, continúa procesando
  t=5ms: operador B recibe barrera, toma snapshot, continúa procesando
  t=10ms: sink C confirma al JobManager
  t=15ms: JobManager declara checkpoint completo
  (mientras tanto, el job siguió procesando elementos post-barrera)
```

**Preguntas:**

1. ¿Por qué el checkpoint de Flink es más eficiente que el de Spark SS
   (que pausa brevemente el procesamiento)?

2. Si un operador tiene dos canales de entrada (ej: después de un join),
   ¿qué pasa cuando la barrera llega solo por uno de los dos canales?

3. ¿Cuánto tiempo dura un checkpoint típico? ¿Qué lo hace lento?

4. ¿Qué diferencia hay entre un checkpoint y un savepoint en Flink?

5. ¿Si un checkpoint falla (no puede escribir a S3), el job continúa?

**Pista:** Cuando un operador espera la barrera en el segundo canal de entrada,
debe "almacenar en buffer" los elementos que llegan del canal ya recibido
(para no procesarlos antes de que llegue la barrera del otro canal — eso
rompería la consistencia del snapshot). Este buffering tiene un costo en
memoria proporcional al volumen de datos en vuelo entre los dos canales.
Para joins entre streams con diferente velocidad, el canal lento puede causar
un buffer grande en el canal rápido — aumentando el tiempo de checkpoint.

---

### Ejercicio 12.6.2 — Savepoints: migración y actualizaciones

```bash
# Los savepoints son checkpoints iniciados manualmente por el operador.
# A diferencia de los checkpoints automáticos, los savepoints se
# mantienen indefinidamente hasta que el operador los borra.

# Tomar un savepoint de un job en ejecución:
flink savepoint <job-id> s3://savepoints/job-metricas/

# Detener el job y tomar un savepoint simultáneamente:
flink stop --savepointPath s3://savepoints/job-metricas/ <job-id>

# Reiniciar desde un savepoint (con la misma versión del código):
flink run --fromSavepoint s3://savepoints/job-metricas/sp-123/ jar/mi-job.jar

# Reiniciar desde un savepoint (con código actualizado):
# Flink intenta mapear el estado del savepoint a los operadores del nuevo código
# usando el nombre del operador (uid).
```

```python
# En el código: asignar UIDs explícitos a los operadores
# (CRÍTICO para migración con savepoints)
stream \
    .map(parse_eventos).uid("parse-eventos") \          # UID explícito
    .filter(filtrar_validos).uid("filtrar-validos") \   # UID explícito
    .key_by("region") \
    .window(...) \
    .aggregate(...).uid("agregar-revenue")              # UID explícito
# Sin UIDs, Flink asigna UIDs autogenerados que cambian si el grafo cambia.
# Con UIDs explícitos, puedes añadir/quitar operadores y el savepoint
# mapea el estado correcto al operador correcto.
```

**Preguntas:**

1. ¿Cuándo usarías un savepoint en lugar de esperar al próximo checkpoint?

2. ¿Puedes aplicar un savepoint a una versión del job con más operadores
   que la versión que generó el savepoint?

3. ¿Los savepoints son compatibles entre versiones de Flink?
   ¿Hay limitaciones?

4. ¿Qué pasa si un operador tiene un UID en el savepoint pero no
   existe en el nuevo código?

5. ¿Los savepoints son más grandes que los checkpoints? ¿Por qué?

---

### Ejercicio 12.6.3 — Configurar checkpointing para producción

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import CheckpointingMode, ExternalizedCheckpointCleanup

env = StreamExecutionEnvironment.get_execution_environment()

# Configuración de checkpointing para producción:
env.enable_checkpointing(60_000)  # checkpoint cada 60 segundos

checkpoint_config = env.get_checkpoint_config()

# Semántica exactly-once (más costoso pero más correcto):
checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# Tiempo mínimo entre checkpoints (para no saturar el sistema):
checkpoint_config.set_min_pause_between_checkpoints(30_000)  # 30s mínimo

# Timeout del checkpoint (si tarda más, se cancela):
checkpoint_config.set_checkpoint_timeout(300_000)  # 5 minutos máximo

# Tolerancia a fallos de checkpoint:
checkpoint_config.set_tolerable_checkpoint_failure_number(3)  # 3 fallos consecutivos antes de fallar el job

# Retener el último checkpoint si el job falla (para recuperación manual):
checkpoint_config.enable_externalized_checkpoints(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
)

# Backend de checkpoint (S3):
env.set_state_backend(EmbeddedRocksDBStateBackend(incremental=True))
env.get_checkpoint_config().set_checkpoint_storage("s3://checkpoints/flink/")
```

**Restricciones:**
1. ¿Por qué `min_pause_between_checkpoints` es importante?
   ¿Qué pasa si los checkpoints se solapan?
2. ¿El modo `AT_LEAST_ONCE` es más rápido que `EXACTLY_ONCE`? ¿En qué casos?
3. ¿Cómo calculas el intervalo de checkpoint óptimo para un job dado?

---

### Ejercicio 12.6.4 — Recuperación de fallos: qué se reprocesa

```
Escenario: job con checkpoint cada 60 segundos.

t=0:    checkpoint 100 completado
t=10s:  llegan 5,000 eventos, procesados y estado actualizado
t=20s:  llegan 5,000 eventos más
t=30s:  llegan 5,000 eventos más
t=35s:  !!!FALLO!!! — TaskManager 3 se cae
t=36s:  Flink detecta el fallo
t=37s:  Flink inicia recuperación desde checkpoint 100

Recuperación:
  1. Flink restaura el estado de todos los operadores al estado del checkpoint 100
  2. Flink resetea los offsets de Kafka al valor del checkpoint 100
  3. Flink reinicia todos los Task Slots
  4. Los 15,000 eventos entre t=0 y t=35s se releen de Kafka y reprocesa

Resultado:
  - Los 15,000 eventos se procesan dos veces
  - Si el sink es idempotente: exactly-once (segunda escritura sobreescribe la primera)
  - Si el sink no es idempotente: at-least-once (duplicados en el sink)
```

**Preguntas:**

1. ¿Por qué Flink puede "rebobinar" Kafka a los offsets del checkpoint?
   ¿Qué propiedad de Kafka hace esto posible?

2. Si el checkpoint tarda 45 segundos en completarse, ¿cuántos eventos
   se reprocesarán en el peor caso?

3. ¿El reprocesamiento de 15,000 eventos puede causar problemas de rendimiento?
   ¿Flink tiene mecanismos para acelerar la recuperación?

4. ¿Si el sink es una base de datos sin upsert (solo INSERT), cómo garantizas
   que los duplicados del reprocesamiento no afecten la corrección?

5. ¿Exactly-once de Flink garantiza que el usuario final nunca ve
   el efecto de un evento dos veces?

---

### Ejercicio 12.6.5 — Leer: el job que nunca completaba un checkpoint

**Tipo: Diagnosticar**

Un job de Flink lleva 6 horas sin completar un checkpoint:

```
Checkpoint history:
  #1200: FAILED  (duration: 5m 12s, reason: timeout after 5 minutes)
  #1201: FAILED  (duration: 5m 01s, reason: timeout after 5 minutes)
  #1202: FAILED  (duration: 5m 00s, reason: timeout after 5 minutes)
  ...
  #1250: FAILED  (duration: 5m 00s, reason: timeout after 5 minutes)

Métricas del job:
  Throughput: 85K eventos/segundo (normal)
  Backpressure: NONE
  Estado actual: 45 GB (en RocksDB)
  Checkpoint size (si completara): estimado 45 GB

Estado de S3 durante los checkpoints:
  Velocidad de escritura: ~150 MB/s
  Tiempo estimado para 45 GB: 45,000 MB / 150 MB/s = 300s = 5 minutos
```

**Preguntas:**

1. ¿Por qué el checkpoint tarda exactamente 5 minutos (el timeout)?

2. ¿Cuál es la causa raíz del problema?

3. ¿Qué tres soluciones son posibles?

4. ¿Los checkpoints incrementales de RocksDB resolverían el problema?
   ¿Por qué?

5. ¿Qué riesgos tiene el job después de 6 horas sin checkpoint exitoso?

**Pista:** 45 GB / 150 MB/s = exactamente 300 segundos = 5 minutos = el timeout.
El checkpoint siempre llega justo al límite y falla. Las soluciones:
(1) Aumentar el timeout de checkpoint (ej: a 15 minutos),
(2) Aumentar el ancho de banda de escritura a S3 (usar instancias con más red),
(3) Activar checkpoints incrementales de RocksDB — solo escribe el delta
desde el último checkpoint exitoso, que puede ser mucho menos de 45 GB.
Con incrementales, el primer checkpoint sigue siendo lento (45 GB) pero
los sucesivos solo escriben los cambios (típicamente 1-5% del estado total).
El riesgo sin checkpoint: si el job falla, debe reprocesar 6 horas de eventos.

---

## Sección 12.7 — Flink SQL y Table API: el Nivel de Abstracción Superior

### Ejercicio 12.7.1 — Flink SQL: el mismo SQL, semántica de streaming

```python
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import lit

# Crear el entorno de SQL streaming:
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)

# Registrar una tabla de Kafka como fuente:
table_env.execute_sql("""
    CREATE TABLE eventos (
        evento_id STRING,
        user_id   STRING,
        tipo      STRING,
        monto     DOUBLE,
        ts        TIMESTAMP(3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'eventos',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
""")

# Registrar el sink:
table_env.execute_sql("""
    CREATE TABLE metricas_por_ventana (
        ventana_inicio TIMESTAMP(3),
        ventana_fin    TIMESTAMP(3),
        region         STRING,
        revenue        DOUBLE,
        PRIMARY KEY (ventana_inicio, ventana_fin, region) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/metricas',
        'table-name' = 'metricas_por_ventana'
    )
""")

# La query SQL (automáticamente en streaming):
table_env.execute_sql("""
    INSERT INTO metricas_por_ventana
    SELECT
        TUMBLE_START(ts, INTERVAL '5' MINUTE) AS ventana_inicio,
        TUMBLE_END(ts,   INTERVAL '5' MINUTE) AS ventana_fin,
        user_id,
        SUM(monto) AS revenue
    FROM eventos
    WHERE tipo = 'compra'
    GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE), user_id
""")
```

**Preguntas:**

1. ¿La semántica de `TUMBLE` en Flink SQL es event time o processing time?
   ¿Cómo se controla?

2. ¿El `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` en el DDL es equivalente
   a `for_bounded_out_of_orderness(Duration.of_seconds(5))`?

3. ¿Flink SQL soporta joins entre dos tablas de streaming?
   ¿Con qué restricciones?

4. ¿Una query SQL de Flink puede mezclarse con la DataStream API
   en el mismo job?

5. ¿Flink SQL es más rápido o más lento que la DataStream API equivalente?

---

### Ejercicio 12.7.2 — Flink SQL: joins temporales y pattern matching

```sql
-- Join temporal: enriquecer el stream con la versión del perfil de cliente
-- en el momento en que ocurrió la transacción (no el perfil actual)
SELECT
    t.transaccion_id,
    t.monto,
    p.segmento,  -- segmento del cliente EN EL MOMENTO de la transacción
    p.region
FROM transacciones AS t
LEFT JOIN perfiles FOR SYSTEM_TIME AS OF t.ts AS p  -- join temporal
ON t.user_id = p.user_id;

-- MATCH_RECOGNIZE: detección de patrones en SQL (similar a Flink CEP)
SELECT *
FROM eventos
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY ts
    MEASURES
        FIRST(A.ts)  AS inicio_patron,
        LAST(C.ts)   AS fin_patron,
        C.monto      AS monto_compra
    PATTERN (A B+ C)  -- A seguido de 1+ B seguido de C
    DEFINE
        A AS tipo = 'login_fallido',
        B AS tipo = 'login_fallido',
        C AS tipo = 'compra'
)
```

**Preguntas:**

1. ¿El "join temporal" (`FOR SYSTEM_TIME AS OF t.ts`) es posible en Spark SS?
   ¿Cómo lo implementarías en Spark?

2. ¿`MATCH_RECOGNIZE` en Flink SQL es equivalente al CEP de la DataStream API?
   ¿Tiene las mismas capacidades?

3. ¿Qué tipo de tabla puede usarse en el lado derecho del join temporal?
   ¿Puede ser otra tabla de streaming?

---

### Ejercicio 12.7.3 — Table API vs DataStream API vs SQL: cuándo usar cada una

**Tipo: Analizar**

Para cada caso de uso, elegir la API más apropiada y justificar:

```
Caso 1: Un analista de datos (no ingeniero) necesita calcular métricas
        sobre un stream de eventos. Solo sabe SQL.

Caso 2: Implementar un algoritmo de detección de fraude con lógica
        compleja de estado: sesiones de usuario, patrones de comportamiento,
        modelos de ML aplicados por clave.

Caso 3: Un pipeline que ya existe en Spark SQL y necesita migrarse
        a Flink para reducir la latencia.

Caso 4: Calcular medias móviles de 7 días para 1M series temporales,
        cada una con estado independiente.

Caso 5: Integrar Flink con una librería de Python (scikit-learn, numpy)
        para aplicar transformaciones sobre el stream.
```

---

### Ejercicio 12.7.4 — Flink en el ecosistema: CDC, ML, y más allá

```python
# Flink como motor de CDC (Change Data Capture):
# Leer cambios de una base de datos como stream de eventos

table_env.execute_sql("""
    CREATE TABLE clientes_cdc (
        id       INT,
        nombre   STRING,
        email    STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname' = 'localhost',
        'port' = '3306',
        'username' = 'flink',
        'password' = 'xxx',
        'database-name' = 'ecommerce',
        'table-name' = 'clientes'
    )
""")
# Cada INSERT/UPDATE/DELETE en MySQL aparece como evento en el stream

# Flink + ML: aplicar un modelo sklearn sobre el stream
from pyflink.datastream import MapFunction
import joblib
import numpy as np

class AplicarModelo(MapFunction):
    def __init__(self, ruta_modelo: str):
        self.ruta_modelo = ruta_modelo
        self.modelo = None
    
    def open(self, runtime_context):
        # Cargar el modelo una vez por sub-task (no por evento):
        self.modelo = joblib.load(self.ruta_modelo)
    
    def map(self, evento):
        features = np.array([[evento["f1"], evento["f2"], evento["f3"]]])
        prediccion = self.modelo.predict(features)[0]
        return {**evento, "prediccion": float(prediccion)}

stream_con_predicciones = stream.map(
    AplicarModelo("modelos/fraud_detector_v3.pkl"),
    output_type=Types.MAP(Types.STRING(), Types.STRING())
)
```

**Preguntas:**

1. ¿El CDC con Flink tiene las mismas garantías que Debezium + Kafka?
   ¿Cuándo usar cada opción?

2. ¿El modelo de sklearn se carga una vez por sub-task o una vez por evento?
   ¿Por qué importa esta diferencia?

3. Si el modelo de ML se actualiza, ¿cómo actualizas el job de Flink
   sin detenerlo?

4. ¿Flink tiene integración con MLflow u otros registros de modelos?

---

### Ejercicio 12.7.5 — El sistema de e-commerce completo en Flink

**Tipo: Diseñar + Implementar**

Implementar el pipeline de métricas en tiempo real del sistema de e-commerce
usando Flink, con todos los conceptos del capítulo:

```
Fuentes:
  - Kafka: topic "clicks" (100K msgs/seg)
  - Kafka: topic "compras" (10K msgs/seg)
  - MySQL CDC: tabla "productos" (actualizaciones esporádicas)

Procesamiento requerido:
  1. Revenue en tiempo real por región (ventana tumbling de 1 minuto)
  2. Tasa de conversión click→compra (stream-stream join, ventana de 30 min)
  3. Alerta si revenue cae > 30% respecto al mismo minuto del día anterior
  4. Detección de fraude: 3 compras de > $500 en 5 minutos para el mismo usuario

Sinks:
  - Delta Lake: todas las métricas (para análisis histórico)
  - Kafka: alertas de fraude (para notificación en tiempo real)
  - Redis: dashboard de métricas actuales (últimos 5 minutos)

SLA:
  - Revenue: resultado disponible en < 2 minutos
  - Fraude: alerta en < 10 segundos
  - Conversión: resultado disponible en < 5 minutos
```

**Restricciones:**
1. Diseñar la arquitectura completa antes de escribir código
2. Justificar el tipo de ventana, watermark, y output mode para cada métrica
3. Implementar al menos dos de las cuatro métricas
4. ¿Qué parte del pipeline usa DataStream API y cuál Flink SQL?

---

## Resumen del capítulo

**Cuándo Flink y cuándo Spark SS:**

```
Usar Flink cuando:
  ✓ Latencia < 1 segundo es un requisito de negocio
  ✓ Estado complejo (sesiones, patrones, joins con ventanas largas)
  ✓ CEP (Complex Event Processing) — detección de patrones en secuencias
  ✓ Algoritmos iterativos (PageRank, k-means sobre streams)
  ✓ El equipo tiene experiencia con Flink o quiere un control fino del estado
  ✓ CDC (Change Data Capture) como fuente de datos principal

Usar Spark SS cuando:
  ✓ El equipo ya conoce Spark y el ecosistema Spark
  ✓ Latencia de segundos es aceptable
  ✓ Necesitas integración con MLlib, Delta Lake nativo, o el catálogo de Spark
  ✓ El mismo código debe funcionar para batch y streaming (lambda architecture)
  ✓ La complejidad operacional de Flink no está justificada para el caso
```

**Los cinco conceptos que hacen a Flink diferente:**

```
1. Streaming nativo
   El evento se procesa cuando llega, no cuando se acumula un batch.
   Latencia de milisegundos vs segundos.

2. Estado como ciudadano de primera clase
   RocksDB, TTL, QueryableState, savepoints para migración.
   El estado puede ser terabytes sin OOM.

3. Event time nativo
   El watermark es parte fundamental del modelo, no una extensión.
   Datos tardíos, allow_lateness, y side outputs para el residual.

4. Checkpointing sin pausa
   Algoritmo de Chandy-Lamport: snapshot consistente sin detener el procesamiento.
   Recuperación de fallos sin replay manual.

5. CEP y patrones
   Detectar secuencias de eventos con condiciones temporales.
   Casos de uso imposibles o muy complejos en Spark SS.
```

**La arquitectura completa de la Parte 3:**

```
Productores → Kafka (Cap.09) → Motor de procesamiento → Sinks

Motor de procesamiento:
  Beam (Cap.10):   portabilidad entre runners, batch+streaming unificado
  Spark SS (Cap.11): integración con Spark, micro-batching, segundos
  Flink (Cap.12):  streaming nativo, milisegundos, estado complejo

Los tres convergen en los mismos conceptos:
  watermarks, ventanas, estado, exactly-once, checkpoints.
  La diferencia: el punto del espectro latencia/complejidad/integración
  donde cada uno hace sus compromisos.
```

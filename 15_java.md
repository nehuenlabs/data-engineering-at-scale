# Guía de Ejercicios — Cap.15: Java — Beam, Flink, y Kafka en su Lenguaje Nativo

> Java es el lenguaje en que están escritos los frameworks de data engineering más importantes.
> Hadoop, Kafka, Flink, Beam, Spark — todos son proyectos Java/JVM.
> No es un accidente: la JVM ofrece gestión de memoria automática,
> portabilidad entre plataformas, y un ecosistema de librerías
> que ninguna otra plataforma ha replicado para infraestructura de datos.
>
> Cuando escribes un pipeline de Beam en Python, el SDK traduce tu código
> a un plan que ejecuta la JVM. Cuando escribes un conector de Kafka Connect,
> la interfaz es Java. Cuando necesitas extender Flink con un operador custom,
> la API completa está en Java.
>
> La verbosidad de Java no es un defecto — es explícita.
> Cada tipo está declarado. Cada excepción está chequeada.
> En un pipeline de producción que procesa 10 TB diarios,
> esa explicitud es lo que hace que el código sea mantenible
> cuando lo lee otro ingeniero seis meses después.

---

## El modelo mental: Java como la plataforma nativa del ecosistema de datos

```
Lo que ocurre cuando ejecutas un pipeline de Beam en Python:

  tu código Python (Beam SDK para Python)
        ↓
  Beam traduce el pipeline a un plan de ejecución (proto)
        ↓
  El Runner (Flink/Dataflow/Spark) recibe el plan
        ↓
  Si el Runner es Flink: ejecuta en JVM
  Si el Runner es Dataflow: ejecuta en JVM
  Si usas DoFn en Python: JVM → gRPC → Python subprocess → gRPC → JVM
        ↑ ← este boundary existe

  La mayoría del overhead de procesamiento está en la JVM.
  Python construye el plan y ejecuta las funciones que tú escribiste en Python.

Lo que ocurre cuando escribes el mismo pipeline en Java:

  tu código Java (Beam SDK para Java)
        ↓
  Beam construye el plan de ejecución (nativo, sin traducción)
        ↓
  El Runner ejecuta el plan directamente en la JVM
        ↓
  No hay boundary. No hay gRPC. No hay subprocess.
  Tu DoFn ejecuta en el mismo proceso que el framework.
```

```
El ecosistema de datos y su relación con Java:

  Framework         Lenguaje nativo    Python SDK     Boundary
  ─────────────     ───────────────    ──────────     ────────
  Hadoop MapReduce  Java               mrjob (wrap)   process
  Apache Kafka      Java/Scala         confluent-kafka C wrapper
  Kafka Streams     Java               —              no existe
  Kafka Connect     Java               —              no existe
  Apache Flink      Java/Scala         PyFlink        gRPC
  Apache Beam       Java               Beam Python    gRPC
  Apache Spark      Scala/Java         PySpark        Py4J
  Apache Hive       Java               PyHive (SQL)   JDBC

  Java no es una opción — es el lenguaje en que se escribió el ecosistema.
```

---

## Tabla de contenidos

- [Sección 15.1 — Java moderno para ingenieros de datos](#sección-151--java-moderno-para-ingenieros-de-datos)
- [Sección 15.2 — La JVM como plataforma de data engineering](#sección-152--la-jvm-como-plataforma-de-data-engineering)
- [Sección 15.3 — Apache Beam: el modelo unificado en su lenguaje nativo](#sección-153--apache-beam-el-modelo-unificado-en-su-lenguaje-nativo)
- [Sección 15.4 — Kafka Streams y Kafka Connect: solo Java](#sección-154--kafka-streams-y-kafka-connect-solo-java)
- [Sección 15.5 — Apache Flink: la API Java](#sección-155--apache-flink-la-api-java)
- [Sección 15.6 — Spark Java API y el ecosistema Hadoop](#sección-156--spark-java-api-y-el-ecosistema-hadoop)
- [Sección 15.7 — Java en el stack de datos: cuándo la verbosidad es ventaja](#sección-157--java-en-el-stack-de-datos-cuándo-la-verbosidad-es-ventaja)

---

## Sección 15.1 — Java Moderno para Ingenieros de Datos

### Ejercicio 15.1.1 — Leer: Java 17+ en 30 minutos para un Python developer

**Tipo: Leer**

```java
// Java moderno: lo que un Python developer necesita saber
// para leer y escribir pipelines de datos.

// 1. Variables — siempre con tipo (inferencia con var desde Java 10)
String nombre = "Alice";           // tipo explícito
var contador = 0;                  // tipo inferido: int
final double PI = 3.14159;        // inmutable (equivalente a val de Scala)

// 2. Records (Java 16+) — equivalentes a dataclass de Python o case class de Scala
public record Transaccion(
    String id,
    String userId,
    double monto,
    String region,
    long timestamp
) {}
// Records tienen: equals, hashCode, toString, y getters automáticos.
// Son inmutables por defecto.
var t = new Transaccion("t1", "alice", 150.0, "norte", 1700000000L);
var monto = t.monto();       // getter — no t.monto sino t.monto()
// t.monto = 200.0;          // Error: los records son inmutables

// 3. Sealed classes (Java 17) — jerarquías cerradas
public sealed interface Evento permits Compra, Devolucion, Click {
    String userId();
    long timestamp();
}
public record Compra(String userId, long timestamp, double monto) implements Evento {}
public record Devolucion(String userId, long timestamp, String motivo) implements Evento {}
public record Click(String userId, long timestamp, String pagina) implements Evento {}

// 4. Pattern matching (Java 21) — switch mejorado
String describir(Evento e) {
    return switch (e) {
        case Compra c     -> "Compra de $" + c.monto();
        case Devolucion d -> "Devolución: " + d.motivo();
        case Click cl     -> "Click en " + cl.pagina();
    };
}
// El compilador verifica exhaustividad — si agregas un nuevo tipo, el switch falla.

// 5. Optional — alternativa a null (similar al Option de Scala)
Optional<String> quizas = Optional.of("valor");
Optional<String> nada = Optional.empty();
String resultado = quizas.orElse("default");        // "valor"
String resultado2 = nada.orElse("default");          // "default"
quizas.map(String::toUpperCase);                     // Optional.of("VALOR")

// 6. Streams API — operaciones funcionales sobre colecciones
List<Transaccion> transacciones = List.of(
    new Transaccion("t1", "alice", 150.0, "norte", 0L),
    new Transaccion("t2", "bob", 50.0, "sur", 0L),
    new Transaccion("t3", "alice", 300.0, "norte", 0L)
);

Map<String, Double> revenuePorRegion = transacciones.stream()
    .filter(t -> t.monto() > 100)
    .collect(Collectors.groupingBy(
        Transaccion::region,
        Collectors.summingDouble(Transaccion::monto)
    ));
// {"norte" -> 450.0}

// 7. Text blocks (Java 15) — strings multilínea
String sql = """
    SELECT region, SUM(monto) AS revenue
    FROM transacciones
    WHERE monto > 100
    GROUP BY region
    """;
```

**Preguntas:**

1. ¿Los `record` de Java son equivalentes a los `case class` de Scala?
   ¿Qué tienen de menos?

2. ¿`sealed interface` en Java y `sealed trait` en Scala cumplen el mismo rol?
   ¿El pattern matching de Java 21 es tan potente como el de Scala?

3. ¿La Streams API de Java es lazy como `LazyList` de Scala o eager como `List`?

4. ¿Por qué Java sigue necesitando el tipo `void` cuando Scala tiene `Unit`?

5. ¿`var` en Java convierte a Java en un lenguaje de tipado dinámico?

**Pista:** Los `record` de Java no soportan herencia (no puedes extender otro record),
y no tienen `copy()` como las `case class` de Scala. Para crear una copia con un campo
cambiado, necesitas construir un nuevo record manualmente:
`new Transaccion(t.id(), t.userId(), 200.0, t.region(), t.timestamp())`.
La Streams API es lazy: las operaciones intermedias (`filter`, `map`) no se ejecutan
hasta una operación terminal (`collect`, `reduce`, `count`). Pero a diferencia
de las colecciones lazy de Scala, un Stream de Java solo puede consumirse una vez.

---

### Ejercicio 15.1.2 — El primer job de datos en Java

```java
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class AnalisisVentas {

    public record Venta(String id, String userId, double monto, String region) {}

    public static void main(String[] args) throws IOException {

        // Leer un CSV (simplificado, sin librería externa):
        List<Venta> ventas = Files.readAllLines(Path.of("ventas.csv")).stream()
            .skip(1)  // encabezado
            .map(linea -> {
                var campos = linea.split(",");
                return new Venta(campos[0], campos[1],
                    Double.parseDouble(campos[2]), campos[3]);
            })
            .toList();

        // Agrupar por región y sumar revenue:
        Map<String, DoubleSummaryStatistics> stats = ventas.stream()
            .collect(Collectors.groupingBy(
                Venta::region,
                Collectors.summarizingDouble(Venta::monto)
            ));

        stats.forEach((region, s) ->
            System.out.printf("Region: %s | Revenue: %.2f | Count: %d | Avg: %.2f%n",
                region, s.getSum(), s.getCount(), s.getAverage()));

        // Top 5 clientes por gasto:
        ventas.stream()
            .collect(Collectors.groupingBy(
                Venta::userId,
                Collectors.summingDouble(Venta::monto)))
            .entrySet().stream()
            .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
            .limit(5)
            .forEach(e -> System.out.printf("  %s: %.2f%n", e.getKey(), e.getValue()));
    }
}
```

**Preguntas:**

1. ¿La Streams API de Java puede paralizar automáticamente?
   ¿Qué pasa si cambias `.stream()` por `.parallelStream()`?

2. ¿`DoubleSummaryStatistics` es equivalente a algo en Polars o PySpark?

3. ¿Este código funciona para un archivo de 50 GB? ¿Por qué no?

4. ¿Qué librería usarías en Java para leer CSV de forma robusta
   (con comillas, escapes, etc.)?

5. ¿Por qué Java requiere `.toList()` al final del stream
   cuando Python retorna una lista directamente de list comprehensions?

---

### Ejercicio 15.1.3 — Maven y Gradle: los sistemas de build del ecosistema

```xml
<!-- pom.xml — Maven: el equivalente de pyproject.toml para proyectos Java -->
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.ecommerce</groupId>
    <artifactId>pipeline-datos</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <java.version>17</java.version>
        <beam.version>2.54.0</beam.version>
        <flink.version>1.18.1</flink.version>
        <kafka.version>3.7.0</kafka.version>
    </properties>

    <dependencies>
        <!-- Beam SDK — core -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-core</artifactId>
            <version>${beam.version}</version>
        </dependency>

        <!-- Beam Runner para Flink -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-runners-flink-1.18</artifactId>
            <version>${beam.version}</version>
        </dependency>

        <!-- Kafka Streams -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <!-- Plugin para crear un fat JAR (shade) -->
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.2</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <transformers>
                                <transformer implementation=
                                    "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

**Preguntas:**

1. ¿Por qué `maven-shade-plugin` es necesario para jobs de Beam/Flink?
   ¿Qué problema resuelve el fat JAR?

2. ¿El `scope` de las dependencias Maven (`compile`, `provided`, `test`)
   es equivalente a los extras de pip en Python?

3. ¿Por qué los conflictos de dependencias son más frecuentes en Java
   que en Python? ¿Qué es el "dependency hell" de JVM?

4. ¿Maven o Gradle? ¿Cuándo elegir cada uno?

5. ¿El `ServicesResourceTransformer` es necesario? ¿Qué rompe si no lo incluyes?

**Pista:** El `ServicesResourceTransformer` fusiona los archivos `META-INF/services/*`
cuando creas un fat JAR. Beam y Flink usan el patrón ServiceLoader de Java
para descubrir implementaciones en tiempo de ejecución. Sin este transformer,
el fat JAR solo incluye la versión de un JAR y pierde las de otros → clases no encontradas.

---

### Ejercicio 15.1.4 — Generics en Java: el sistema de tipos que usan Beam y Flink

```java
// Los frameworks de datos en Java usan generics extensivamente.
// Entenderlos es necesario para leer las APIs de Beam, Flink, y Kafka Streams.

// 1. Colecciones tipadas:
List<String> nombres = List.of("alice", "bob");
Map<String, List<Double>> montosPorRegion = new HashMap<>();

// 2. Métodos genéricos:
public static <T> List<T> filtrar(List<T> items, Predicate<T> predicado) {
    return items.stream().filter(predicado).toList();
}
var altos = filtrar(List.of(1.5, 1.8, 1.6, 1.9), x -> x > 1.7);

// 3. Bounded generics (frecuente en Beam):
public static <T extends Comparable<T>> T maximo(List<T> items) {
    return items.stream().max(Comparator.naturalOrder()).orElseThrow();
}

// 4. Wildcards — el concepto más confuso de Java generics:
// ? extends T  → productor (lees pero no escribes)
// ? super T    → consumidor (escribes pero no lees con tipo seguro)
public static double sumarMontos(List<? extends Transaccion> transacciones) {
    return transacciones.stream().mapToDouble(Transaccion::monto).sum();
}

// 5. Type erasure — por qué Java generics son distintos a los de Scala:
// En runtime, List<String> y List<Integer> son el mismo tipo: List.
// No puedes hacer: if (x instanceof List<String>) — el tipo se borra.
// Beam resuelve esto con TypeDescriptor:
import org.apache.beam.sdk.values.TypeDescriptor;
TypeDescriptor<String> tipo = TypeDescriptor.of(String.class);
// Beam lo necesita porque la serialización requiere saber el tipo en runtime.

// 6. Functional interfaces (lo que Beam/Flink usan para lambdas):
@FunctionalInterface
interface Transformador<T, R> {
    R aplicar(T input);
}
Transformador<String, Integer> longitud = s -> s.length();
```

**Preguntas:**

1. ¿Por qué Beam necesita `TypeDescriptor` si Java tiene generics?
   ¿Qué problema causa el type erasure?

2. ¿Las wildcards `? extends` y `? super` tienen equivalente en Scala?
   ¿Y en Python?

3. ¿`@FunctionalInterface` es necesario para que una interfaz acepte lambdas?

4. ¿Los generics de Java son más restrictivos que los de Scala?
   ¿En qué aspectos?

5. ¿El type erasure afecta al rendimiento en runtime?

---

### Ejercicio 15.1.5 — Leer: excepciones chequeadas y por qué importan en pipelines

**Tipo: Analizar**

```java
// Java tiene excepciones "checked" (chequeadas) — el compilador te obliga a manejarlas.
// Esto es una diferencia fundamental con Python, Scala (no las chequea), y Rust (usa Result).

// En Python:
//   open("archivo.txt")  # puede fallar — Python no te obliga a manejar el error

// En Java:
//   Files.readString(Path.of("archivo.txt"));
//   // Error de compilación: Unhandled exception: IOException
//   // DEBES envolver en try-catch O declarar "throws IOException"

// Opción 1: manejar explícitamente
public List<String> leerArchivo(String ruta) {
    try {
        return Files.readAllLines(Path.of(ruta));
    } catch (IOException e) {
        throw new RuntimeException("Error leyendo " + ruta, e);
    }
}

// Opción 2: propagar (el caller debe manejarlo)
public List<String> leerArchivo2(String ruta) throws IOException {
    return Files.readAllLines(Path.of(ruta));
}

// En pipelines de datos, esto importa:
// Beam, Flink, y Kafka Streams requieren que las funciones de usuario
// manejen las excepciones explícitamente:

// Beam DoFn — no puedes lanzar checked exceptions:
public class ProcesarEvento extends DoFn<String, Transaccion> {
    @ProcessElement
    public void process(@Element String json, OutputReceiver<Transaccion> out) {
        try {
            Transaccion t = objectMapper.readValue(json, Transaccion.class);
            out.output(t);
        } catch (JsonProcessingException e) {
            // Debes decidir: ¿saltar el registro? ¿enviarlo a dead letter queue?
            // Java te OBLIGA a tomar esta decisión en tiempo de compilación.
        }
    }
}
```

**Preguntas:**

1. ¿Las excepciones chequeadas son una ventaja o un obstáculo para pipelines de datos?

2. ¿Scala eliminó las excepciones chequeadas? ¿Python nunca las tuvo?
   ¿Cuál de los tres enfoques produce pipelines más robustos?

3. ¿Cómo manejas errores de parsing en un pipeline de Beam en Python
   si Python no te obliga a atrapar excepciones?

4. ¿El patrón de "dead letter queue" es más natural en Java que en Python?
   ¿Por qué?

5. ¿`try-with-resources` de Java es equivalente al `with` de Python?

**Pista:** Las excepciones chequeadas son controversiales. Kotlin (otro lenguaje JVM)
las eliminó. Scala las ignora. Pero en data engineering, el manejo explícito de errores
es critical: un registro malformado en un stream de 1M eventos/segundo no puede
crashear el pipeline. Java te obliga a pensar en esto en compilación.
El pattern de dead letter queue (DLQ) es natural porque cada `catch`
tiene un lugar lógico donde enviar el registro fallido.

---

## Sección 15.2 — La JVM como Plataforma de Data Engineering

### Ejercicio 15.2.1 — Leer: el modelo de memoria de la JVM

**Tipo: Leer**

```
JVM Memory Model (relevante para data engineering):

  ┌──────────────────────────────────────────────────────┐
  │                    JVM Process                        │
  │                                                       │
  │  ┌──────────────────────────────────────────┐        │
  │  │              Heap Memory                  │        │
  │  │  ┌─────────────────┐  ┌───────────────┐  │        │
  │  │  │   Young Gen      │  │   Old Gen      │  │        │
  │  │  │  Eden + Survivor │  │  (objetos      │  │        │
  │  │  │  (objetos nuevos)│  │   longevos)    │  │        │
  │  │  └─────────────────┘  └───────────────┘  │        │
  │  └──────────────────────────────────────────┘        │
  │                                                       │
  │  ┌──────────────────────────────────────────┐        │
  │  │          Off-Heap (Direct Memory)         │        │
  │  │  Flink Managed Memory: buffers de red,    │        │
  │  │  state backends, sort buffers              │        │
  │  │  Netty: buffers de I/O de red              │        │
  │  │  Arrow: buffers de datos columnares        │        │
  │  └──────────────────────────────────────────┘        │
  │                                                       │
  │  ┌──────────────────────────────────────────┐        │
  │  │           Metaspace                       │        │
  │  │  Metadata de clases cargadas              │        │
  │  └──────────────────────────────────────────┘        │
  └──────────────────────────────────────────────────────┘

¿Por qué importa en data engineering?
  - Flink usa off-heap para evitar pausas del GC con datos grandes
  - Spark usa off-heap para Tungsten (su motor de ejecución)
  - Kafka usa off-heap para zero-copy transfer al kernel
  - Arrow en Java usa off-heap para compatibilidad con C/Rust
```

```java
// Configuración JVM típica para un job de datos:
// java -Xms4g -Xmx8g -XX:MaxDirectMemorySize=4g -jar pipeline.jar
//
// -Xms4g: heap mínimo 4 GB (evitar resizing)
// -Xmx8g: heap máximo 8 GB
// -XX:MaxDirectMemorySize=4g: off-heap máximo 4 GB (para Flink/Netty/Arrow)

// Verificar desde Java:
Runtime runtime = Runtime.getRuntime();
long heapMax = runtime.maxMemory();           // -Xmx
long heapUsado = runtime.totalMemory() - runtime.freeMemory();
System.out.printf("Heap: %d MB usado de %d MB%n",
    heapUsado / (1024*1024), heapMax / (1024*1024));

// Off-heap (Direct ByteBuffer):
import java.nio.ByteBuffer;
ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1 MB off-heap
// Este buffer NO está en el heap — no lo gestiona el Garbage Collector
```

**Preguntas:**

1. ¿Por qué Flink prefiere off-heap para su state backend?
   ¿Qué problema del Garbage Collector evita?

2. ¿`ByteBuffer.allocateDirect()` es equivalente a `malloc()` en C?
   ¿Quién libera esa memoria?

3. ¿Un `OutOfMemoryError: Direct buffer memory` y un `OutOfMemoryError: Java heap space`
   son el mismo problema?

4. ¿Por qué `-Xms` y `-Xmx` deberían ser iguales en producción?

5. ¿Cuánto heap necesita un TaskManager de Flink que procesa 10 GB/hora?

---

### Ejercicio 15.2.2 — Garbage Collection: el costo oculto en pipelines de datos

```java
// El GC es el precio que pagas por no gestionar memoria manualmente.
// En data engineering, las pausas del GC pueden causar:
// - Timeouts en Kafka consumers (si la pausa > session.timeout.ms)
// - Backpressure en Flink (si el GC pausa un operator)
// - Slowdowns en Spark (si el GC ejecuta mientras una task procesa)

// Los GC disponibles en Java 17+:
// 1. G1GC (default): pausas cortas (~200ms), heap < 32GB
// 2. ZGC: pausas ultra-cortas (~1ms), para heaps grandes (>32GB)
// 3. Shenandoah: similar a ZGC, disponible en OpenJDK
// 4. Parallel GC: throughput máximo pero pausas largas

// Configuración para un pipeline de streaming (bajo latencia):
// java -XX:+UseZGC -Xmx16g -jar streaming-pipeline.jar

// Configuración para un job batch (máximo throughput):
// java -XX:+UseParallelGC -Xmx32g -jar batch-pipeline.jar

// Monitorear el GC:
// java -Xlog:gc*:file=gc.log:time -jar pipeline.jar

// Ejemplo de salida del GC log:
// [2024-03-15T10:23:45.123] GC(42) Pause Young (Normal) 12M->8M(256M) 1.234ms
// [2024-03-15T10:23:47.456] GC(43) Pause Full (Ergonomics) 200M->150M(512M) 456.789ms
//                                    ↑ una Full GC de 456ms puede causar timeout en Kafka
```

**Preguntas:**

1. ¿Por qué ZGC es preferible para pipelines de streaming sobre G1GC?

2. ¿Un Kafka consumer con `session.timeout.ms=30000` puede ser afectado
   por una pausa de GC de 456ms?

3. ¿Flink evita el problema del GC usando off-heap?
   ¿Completamente o parcialmente?

4. ¿Cómo diagnosticas si un pipeline es lento por GC?
   ¿Qué métricas buscas en el log?

5. ¿Python tiene problemas de GC equivalentes?
   ¿Por qué el reference counting de CPython evita las pausas largas?

---

### Ejercicio 15.2.3 — Serialización en Java: el cuello de botella silencioso

```java
// En data engineering, los datos cruzan boundaries constantemente:
// proceso → disco, proceso → red, executor → executor.
// La serialización convierte objetos Java en bytes y viceversa.

import java.io.*;

// 1. Java Serializable (lento, pero simple):
public class EventoV1 implements Serializable {
    private String userId;
    private double monto;
    // Spark usa Serializable para enviar objetos entre executors
    // cuando no hay un Encoder específico.
}

// 2. Avro (schema-based, eficiente, evolución de schema):
// Beam y Kafka lo usan extensivamente
// El schema viaja con los datos → deserialización sin conocer la clase Java
import org.apache.avro.specific.SpecificRecordBase;
public class EventoAvro extends SpecificRecordBase {
    // generado automáticamente desde un .avsc (schema Avro)
}

// 3. Protobuf (compacto, rápido, schema estricto):
// Beam usa Protobuf internamente para el plan de ejecución
// Google Dataflow usa Protobuf para la comunicación entre workers

// 4. Kryo (rápido, sin schema, usado por Spark como alternativa):
// spark.serializer=org.apache.spark.serializer.KryoSerializer

// Benchmark simplificado:
public static byte[] serializarJava(Object obj) throws IOException {
    var baos = new ByteArrayOutputStream();
    try (var oos = new ObjectOutputStream(baos)) {
        oos.writeObject(obj);
    }
    return baos.toByteArray();
}

// Comparación de tamaños para el mismo objeto:
// Java Serializable: ~450 bytes
// Avro:              ~120 bytes
// Protobuf:          ~85 bytes
// Kryo:              ~95 bytes
```

**Preguntas:**

1. ¿Por qué Kafka recomienda Avro sobre Java Serializable para los mensajes?

2. ¿Protobuf y Avro son equivalentes? ¿Cuándo elegir uno sobre el otro?

3. ¿El costo de serialización/deserialización en un pipeline distribuido
   puede ser mayor que el costo de la transformación misma?

4. ¿Beam tiene un formato de serialización propio? ¿Cómo maneja los Coders?

5. ¿Jackson (JSON) es una alternativa viable para serialización entre servicios
   en un pipeline de datos?

---

### Ejercicio 15.2.4 — Profiling de un job JVM: encontrar el cuello de botella

```java
// Herramientas de profiling para jobs de datos en la JVM:

// 1. async-profiler: sampling profiler de bajo overhead
//    Uso: java -agentpath:/path/to/libasyncProfiler.so=start,event=cpu,file=profile.html \
//         -jar pipeline.jar

// 2. JFR (Java Flight Recorder): integrado en el JDK
//    java -XX:StartFlightRecording=duration=60s,filename=recording.jfr \
//         -jar pipeline.jar

// 3. jcmd (herramienta de diagnóstico en caliente):
//    jcmd <pid> Thread.print           # stack traces
//    jcmd <pid> GC.heap_info           # estado del heap
//    jcmd <pid> VM.native_memory       # memoria nativa

// 4. JMX (Java Management Extensions):
//    Expone métricas via JMX que herramientas como Grafana pueden consumir
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

MemoryMXBean memoria = ManagementFactory.getMemoryMXBean();
var heap = memoria.getHeapMemoryUsage();
System.out.printf("Heap: %d MB usado, %d MB max%n",
    heap.getUsed() / (1024*1024),
    heap.getMax() / (1024*1024));

var nonHeap = memoria.getNonHeapMemoryUsage();
System.out.printf("Non-heap: %d MB usado%n",
    nonHeap.getUsed() / (1024*1024));
```

**Preguntas:**

1. ¿`async-profiler` puede usarse en un cluster de Flink en producción
   sin afectar el rendimiento?

2. ¿JFR muestra dónde el código Java pasa más tiempo: en CPU, en GC,
   o esperando I/O?

3. ¿Cómo perfilarías un job de Spark que corre en un cluster de EMR?

4. ¿Las herramientas de profiling de Java son más completas que las de Python?

5. ¿El overhead de JMX en un pipeline de producción es aceptable?

---

### Ejercicio 15.2.5 — Leer: Java vs JVM — por qué Kotlin y Scala corren en la misma plataforma

**Tipo: Analizar**

```
El ecosistema JVM:

  Lenguaje     Compila a     Runtime        Datos Engineering
  ──────────   ──────────    ──────────     ──────────────────
  Java         bytecode      JVM            Beam, Flink, Kafka
  Scala        bytecode      JVM            Spark
  Kotlin       bytecode      JVM            Android, cada vez más en backend
  Groovy       bytecode      JVM            Jenkins pipelines, Gradle
  Clojure      bytecode      JVM            Nicho

  Todos compilan a bytecode JVM.
  Todos pueden usar las mismas librerías Java.
  Un JAR de Flink en Kotlin puede usar la API Java de Flink sin cambios.

  Implicación para data engineering:
  - El código Scala de Spark y el código Java de Flink corren en la misma JVM
  - Puedes mezclar Java y Scala en el mismo proyecto (con límites)
  - Las librerías de Java (Jackson, Avro, Protobuf) funcionan en cualquier lenguaje JVM
  - El tuning de JVM (GC, heap, off-heap) aplica a todos por igual
```

**Preguntas:**

1. ¿Puedes escribir un pipeline de Beam en Kotlin y usar el runner de Flink?

2. ¿Un JAR compilado con Scala 2.13 puede usarse desde Java sin problemas?

3. ¿Por qué Kafka no eligió Scala (como Spark) sino Java?

4. ¿Kotlin está ganando terreno en data engineering?
   ¿Qué ventajas ofrece sobre Java?

5. ¿El bytecode generado por Java y Scala tiene el mismo rendimiento?

---

## Sección 15.3 — Apache Beam: el Modelo Unificado en su Lenguaje Nativo

### Ejercicio 15.3.1 — El pipeline de Beam en Java: estructura básica

```java
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class RevenuePorRegion {

    public static void main(String[] args) {
        // Crear el pipeline con opciones desde CLI:
        var options = PipelineOptionsFactory.fromArgs(args).create();
        var pipeline = Pipeline.create(options);

        // Leer → Parsear → Filtrar → Agrupar → Escribir
        PCollection<String> lineas = pipeline.apply("LeerCSV",
            TextIO.read().from("gs://datos/ventas.csv"));

        PCollection<KV<String, Double>> revenuePorRegion = lineas
            .apply("Parsear", MapElements.via(
                new SimpleFunction<String, KV<String, Double>>() {
                    @Override
                    public KV<String, Double> apply(String linea) {
                        String[] campos = linea.split(",");
                        return KV.of(campos[3], Double.parseDouble(campos[2]));
                    }
                }))
            .apply("Sumar", Sum.doublesPerKey());

        revenuePorRegion.apply("Escribir", MapElements.via(
            new SimpleFunction<KV<String, Double>, String>() {
                @Override
                public String apply(KV<String, Double> kv) {
                    return kv.getKey() + "," + kv.getValue();
                }
            }))
            .apply(TextIO.write().to("gs://resultados/revenue"));

        // Ejecutar el pipeline:
        pipeline.run().waitUntilFinish();
    }
}
```

**Preguntas:**

1. ¿El mismo código Java de Beam puede ejecutar en Flink, Dataflow, y Spark
   sin cambios? ¿Qué cambia entre runners?

2. ¿`SimpleFunction` es la única forma de definir transformaciones en Beam Java?
   ¿Por qué es tan verboso?

3. ¿`PCollection` es equivalente a un RDD de Spark o a un DataStream de Flink?

4. ¿`pipeline.run().waitUntilFinish()` es bloqueante?
   ¿Cómo lanzas un pipeline asíncrono?

5. ¿El plan de ejecución se construye cuando llamas `.apply()` o cuando
   llamas `.run()`?

**Pista:** En Beam, `.apply()` construye el grafo del pipeline (plan lógico)
pero no ejecuta nada. `.run()` envía el plan al runner (Flink, Dataflow, etc.)
que lo traduce a un plan físico y ejecuta. Esto es exactamente el mismo patrón
que PySpark (lazy evaluation del plan) y Polars (lazy frames).

---

### Ejercicio 15.3.2 — DoFn: la unidad de procesamiento de Beam

```java
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

// DoFn es el equivalente de una UDF, pero con ciclo de vida:
public class ParsearEvento extends DoFn<String, Transaccion> {

    // Tags para outputs múltiples (main output + dead letter queue):
    static final TupleTag<Transaccion> EVENTOS_OK = new TupleTag<>() {};
    static final TupleTag<String> EVENTOS_ERROR = new TupleTag<>() {};

    private ObjectMapper mapper;

    @Setup
    public void setup() {
        // Se llama UNA VEZ por instancia (equivalente a open() en Flink).
        // Inicializar recursos costosos aquí: conexiones, modelos ML, etc.
        mapper = new ObjectMapper();
    }

    @ProcessElement
    public void process(
            @Element String json,
            MultiOutputReceiver out) {
        try {
            Transaccion t = mapper.readValue(json, Transaccion.class);
            if (t.monto() > 0) {
                out.get(EVENTOS_OK).output(t);
            }
        } catch (Exception e) {
            // Dead letter queue: el registro fallido se envía a un output lateral
            out.get(EVENTOS_ERROR).output(json + "|ERROR:" + e.getMessage());
        }
    }

    @Teardown
    public void teardown() {
        // Limpiar recursos. Se llama cuando la instancia se destruye.
    }
}

// Uso en el pipeline:
var resultados = lineas.apply("Parsear", ParDo.of(new ParsearEvento())
    .withOutputTags(ParsearEvento.EVENTOS_OK,
        TupleTagList.of(ParsearEvento.EVENTOS_ERROR)));

PCollection<Transaccion> eventosOk = resultados.get(ParsearEvento.EVENTOS_OK);
PCollection<String> eventosError = resultados.get(ParsearEvento.EVENTOS_ERROR);

// Los errores van a una tabla de dead letter:
eventosError.apply("EscribirDLQ", TextIO.write().to("gs://dlq/errores"));
```

**Preguntas:**

1. ¿El ciclo de vida `@Setup → @ProcessElement → @Teardown` de Beam
   es equivalente al `open → processElement → close` de Flink?

2. ¿El pattern de dead letter queue con `TupleTag` es más limpio en Java
   que en Python? ¿Por qué?

3. ¿`@Setup` se llama una vez por worker o una vez por bundle?

4. ¿Puedes hacer I/O de red dentro de `@ProcessElement`?
   ¿Es una buena práctica?

5. ¿Un DoFn en Java es más eficiente que un DoFn en Python
   cuando el runner es Flink?

---

### Ejercicio 15.3.3 — Windowing y triggers en Beam Java

```java
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

// Beam tiene el modelo de windowing más completo del ecosistema.
// En Java, la API es más explícita que en Python.

// 1. Fixed windows (tumbling):
PCollection<Transaccion> ventanasFijas = eventos
    .apply("VentanaFija", Window.<Transaccion>into(
        FixedWindows.of(Duration.standardMinutes(5)))
        .triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(1)))
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
        .withAllowedLateness(Duration.standardHours(1))
        .accumulatingFiredPanes());

// 2. Sliding windows:
PCollection<Transaccion> ventanasDeslizantes = eventos
    .apply("VentanaDeslizante", Window.<Transaccion>into(
        SlidingWindows.of(Duration.standardMinutes(10))
            .every(Duration.standardMinutes(1))));

// 3. Session windows:
PCollection<Transaccion> sesiones = eventos
    .apply("Sesiones", Window.<Transaccion>into(
        Sessions.withGapDuration(Duration.standardMinutes(30))));

// 4. Global window con trigger custom:
PCollection<Transaccion> global = eventos
    .apply("Global", Window.<Transaccion>into(new GlobalWindows())
        .triggering(Repeatedly.forever(
            AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(30))))
        .discardingFiredPanes());
```

**Preguntas:**

1. ¿`accumulatingFiredPanes` vs `discardingFiredPanes` — cuándo cada uno?

2. ¿`withAllowedLateness` permite eventos que llegan 1 hora tarde?
   ¿Qué pasa con eventos que llegan 2 horas tarde?

3. ¿El modelo de windowing de Beam es más potente que el de Flink?
   ¿O son equivalentes con distinta API?

4. ¿Session windows pueden causar problemas de memoria con usuarios
   que tienen sesiones de 24 horas?

5. ¿El trigger `AfterWatermark.pastEndOfWindow()` garantiza exactitud
   o solo best-effort?

---

### Ejercicio 15.3.4 — Beam Schemas y Row: el sistema de tipos estructurado

```java
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.values.Row;

// Beam Schemas permiten operaciones tipo SQL sobre PCollections:

// Opción 1: definir el schema manualmente
Schema schema = Schema.builder()
    .addStringField("userId")
    .addDoubleField("monto")
    .addStringField("region")
    .addInt64Field("timestamp")
    .build();

// Opción 2: inferir el schema desde un Java Bean o Record
@DefaultSchema(JavaBeanSchema.class)
public class Transaccion {
    private String userId;
    private double monto;
    private String region;
    // getters y setters...
}

// Con schemas, puedes hacer SQL directamente:
import org.apache.beam.sdk.extensions.sql.SqlTransform;

PCollection<Row> resultado = transacciones
    .apply("SQL", SqlTransform.query("""
        SELECT region, SUM(monto) AS revenue, COUNT(*) AS num_transacciones
        FROM PCOLLECTION
        WHERE monto > 100
        GROUP BY region
        """));

// El schema de Beam es equivalente al StructType de Spark
// pero integrado con el modelo de PCollections.
```

**Preguntas:**

1. ¿Beam SQL ejecuta en el runner (Flink/Dataflow) o en el SDK?

2. ¿El schema de Beam soporta tipos complejos (arrays, maps, structs anidados)?

3. ¿`Row` de Beam es equivalente a `Row` de Spark?
   ¿Cuál tiene mejor rendimiento?

4. ¿Puedes mezclar DoFns con SQL transforms en el mismo pipeline?

5. ¿El schema de Beam permite evolución de schema (añadir campos)?

---

### Ejercicio 15.3.5 — Implementar: pipeline Beam Java end-to-end

**Tipo: Implementar**

```java
// Pipeline completo: leer de Kafka → procesar → escribir a BigQuery

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import com.google.api.services.bigquery.model.TableRow;

public class StreamingPipeline {

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args)
            .as(DataflowPipelineOptions.class);
        var pipeline = Pipeline.create(options);

        // TODO: Implementar el pipeline:
        // 1. Leer de Kafka (topic "eventos", Avro deserializer)
        // 2. Parsear eventos con DoFn + dead letter queue
        // 3. Windowing: ventanas fijas de 5 minutos con early triggers cada 1 minuto
        // 4. Agregar: revenue por región por ventana
        // 5. Escribir a BigQuery (tabla particionada por fecha)
        // 6. Escribir errores a una tabla de DLQ en BigQuery

        pipeline.run();
    }
}
```

**Restricciones:**
1. Implementar el pipeline completo con manejo de errores
2. Usar Beam schemas para la salida a BigQuery
3. Incluir métricas custom (Counter para eventos procesados y errores)

---

## Sección 15.4 — Kafka Streams y Kafka Connect: Solo Java

### Ejercicio 15.4.1 — Kafka Streams: procesamiento de streams sin cluster

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;
import java.time.Duration;

// Kafka Streams es ÚNICO en el ecosistema:
// - No necesita cluster (corre como aplicación Java normal)
// - Usa los brokers de Kafka como cluster manager
// - Escala horizontalmente lanzando más instancias de la misma app
// - No tiene equivalente en Python (no existe Kafka Streams para Python)

public class RevenuePorRegionStream {

    public record Venta(String userId, double monto, String region) {}

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "revenue-por-region");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // Leer del topic "ventas" como stream:
        KStream<String, Venta> ventas = builder.stream("ventas",
            Consumed.with(Serdes.String(), ventaSerde()));

        // Agrupar por región y calcular revenue en ventanas de 5 minutos:
        KTable<Windowed<String>, Double> revenue = ventas
            .groupBy((key, venta) -> KeyValue.pair(venta.region(), venta),
                Grouped.with(Serdes.String(), ventaSerde()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                () -> 0.0,                                     // inicializador
                (region, venta, acumulado) -> acumulado + venta.monto(),  // agregador
                Materialized.with(Serdes.String(), Serdes.Double())
            );

        // Escribir resultado al topic "revenue-por-region":
        revenue.toStream()
            .map((windowedKey, value) -> KeyValue.pair(
                windowedKey.key(), value))
            .to("revenue-por-region",
                Produced.with(Serdes.String(), Serdes.Double()));

        // Iniciar la aplicación:
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Shutdown hook:
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

**Preguntas:**

1. ¿Kafka Streams sin cluster dedicado — cómo escala?
   ¿Es más simple o más limitado que Flink?

2. ¿`KStream` vs `KTable` — cuál es la diferencia semántica?
   ¿Es equivalente a stream vs table en Flink SQL?

3. ¿Por qué no existe Kafka Streams para Python?
   ¿Faust (Python) es una alternativa real?

4. ¿El state store de Kafka Streams es RocksDB?
   ¿Es el mismo concepto que el state backend de Flink?

5. ¿Kafka Streams puede hacer joins entre un KStream y un KTable?
   ¿Cuáles son las limitaciones?

**Pista:** Kafka Streams escala usando las particiones del topic de entrada.
Si el topic tiene 12 particiones, puedes lanzar hasta 12 instancias
de la aplicación (cada una procesa un subconjunto de particiones).
Es más simple que Flink (no hay cluster que gestionar) pero más limitado
(no puedes hacer operaciones que requieran shuffle arbitrario).

---

### Ejercicio 15.4.2 — Kafka Connect: conectores en Java

```java
import org.apache.kafka.connect.source.*;
import org.apache.kafka.connect.data.*;
import java.util.*;

// Kafka Connect es un framework para mover datos entre Kafka y sistemas externos.
// Los conectores se escriben en Java. No hay alternativa en Python.

// Source Connector: lee de un sistema externo y produce a Kafka
public class MiSourceConnector extends SourceConnector {
    @Override
    public void start(Map<String, String> props) {
        // Configuración del conector
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MiSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Distribuir el trabajo entre maxTasks tasks
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("partition", String.valueOf(i));
            configs.add(config);
        }
        return configs;
    }

    @Override public void stop() {}
    @Override public ConfigDef config() { return new ConfigDef(); }
    @Override public String version() { return "1.0.0"; }
}

public class MiSourceTask extends SourceTask {
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Leer registros de la fuente externa
        // Retornar como SourceRecords para que Connect los publique a Kafka
        Schema schema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("monto", Schema.FLOAT64_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .build();

        Struct value = new Struct(schema)
            .put("id", "tx-001")
            .put("monto", 150.0)
            .put("timestamp", System.currentTimeMillis());

        return List.of(new SourceRecord(
            Collections.singletonMap("source", "mi-fuente"),
            Collections.singletonMap("offset", "123"),
            "topic-destino", schema, value));
    }

    @Override public void start(Map<String, String> props) {}
    @Override public void stop() {}
    @Override public String version() { return "1.0.0"; }
}
```

**Preguntas:**

1. ¿Por qué crear un conector de Kafka Connect en lugar de
   escribir un productor de Kafka directamente?

2. ¿`Schema` de Kafka Connect es equivalente al `Schema` de Beam
   o al `StructType` de Spark?

3. ¿Los conectores de Connect garantizan exactly-once delivery?

4. ¿Existen alternativas a Kafka Connect que no requieran Java?
   ¿Debezium es un conector de Connect?

5. ¿El schema de Connect soporta evolución (añadir campos sin romper consumidores)?

---

### Ejercicio 15.4.3 — Exactly-once en Kafka Streams: transacciones

```java
// Kafka Streams soporta exactly-once processing:
Properties config = new Properties();
config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
    StreamsConfig.EXACTLY_ONCE_V2);
// EXACTLY_ONCE_V2 usa transacciones de Kafka para garantizar
// que cada mensaje se procesa exactamente una vez,
// incluso si un nodo falla y otro toma su lugar.

// El costo de exactly-once:
// - Mayor latencia (~5-10% overhead por las transacciones)
// - Requiere configuración correcta del cluster Kafka
//   (min.insync.replicas, acks=all)

// Diagrama del flujo transaccional:
//   1. Leer batch de mensajes del topic de entrada
//   2. Iniciar transacción
//   3. Procesar mensajes
//   4. Escribir resultados al topic de salida (dentro de la transacción)
//   5. Commit offsets del topic de entrada (dentro de la misma transacción)
//   6. Commit transacción
//   → Si falla en cualquier punto, la transacción se aborta
//   → Otro nodo re-lee y re-procesa desde el último offset commiteado
```

**Preguntas:**

1. ¿`EXACTLY_ONCE_V2` es truly exactly-once o effectively-once?
   ¿Cuál es la diferencia?

2. ¿Flink tiene un mecanismo equivalent a las transacciones de Kafka
   para exactly-once? ¿Es más o menos complejo?

3. ¿El overhead de 5-10% por transacciones justifica el costo
   para un pipeline financiero?

4. ¿Qué pasa si el procesamiento tiene side effects externos
   (ej: llamar a una API REST)? ¿El exactly-once aplica?

5. ¿At-least-once con deduplicación idempotente es una alternativa viable?

---

### Ejercicio 15.4.4 — Interactive Queries: estado como servicio

```java
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

// Kafka Streams puede exponer su state store como un servicio:
// Los microservicios pueden consultar el estado en tiempo real
// sin necesidad de una base de datos adicional.

KafkaStreams streams = new KafkaStreams(builder.build(), config);
streams.start();

// Esperar a que el estado esté listo:
while (streams.state() != KafkaStreams.State.RUNNING) {
    Thread.sleep(100);
}

// Consultar el state store:
ReadOnlyKeyValueStore<String, Double> store = streams.store(
    StoreQueryParameters.fromNameAndType(
        "revenue-store",
        QueryableStoreTypes.<String, Double>keyValueStore()));

// Exponer via HTTP (con Javalin, por ejemplo):
import io.javalin.Javalin;

Javalin app = Javalin.create().start(8080);
app.get("/revenue/{region}", ctx -> {
    String region = ctx.pathParam("region");
    Double revenue = store.get(region);
    ctx.json(Map.of("region", region, "revenue", revenue != null ? revenue : 0.0));
});
// GET /revenue/norte → {"region": "norte", "revenue": 12345.67}
```

**Preguntas:**

1. ¿Interactive Queries elimina la necesidad de una base de datos
   de lectura separada (Redis, DynamoDB)?

2. ¿Qué pasa si la instancia que tiene el estado de la región "norte"
   se cae? ¿Otra instancia puede responder?

3. ¿El state store usa RocksDB por defecto?
   ¿Qué implicaciones tiene para el rendimiento?

4. ¿Interactive Queries funciona en Flink? ¿O es exclusivo de Kafka Streams?

5. ¿Este patrón (stream processing + API HTTP) reemplaza a CQRS?

---

### Ejercicio 15.4.5 — Implementar: pipeline Kafka Streams con join y DLQ

**Tipo: Implementar**

```java
// Implementar un pipeline que:
// 1. Lee ventas de un topic "ventas" (KStream)
// 2. Lee clientes de un topic "clientes" (KTable, compactado)
// 3. Hace join stream-table para enriquecer ventas con datos del cliente
// 4. Calcula métricas por segmento en ventanas de 10 minutos
// 5. Envía errores de parsing a un topic "dlq-ventas"
// 6. Expone el estado via Interactive Queries

public class PipelineVentas {

    public record Venta(String userId, double monto, String region) {}
    public record Cliente(String userId, String segmento, String pais) {}
    public record VentaEnriquecida(Venta venta, Cliente cliente) {}

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // TODO: implementar
        // Hint: usar ValueJoiner para el join stream-table
        // Hint: usar branching para separar errores de parsing

        return builder.build();
    }
}
```

**Restricciones:**
1. Implementar el join KStream-KTable con manejo de nulls
2. Implementar dead letter queue para registros que no se pueden parsear
3. Añadir Interactive Queries para consultar métricas por segmento
4. Escribir tests unitarios con `TopologyTestDriver`

---

## Sección 15.5 — Apache Flink: la API Java

### Ejercicio 15.5.1 — DataStream API en Java vs Python

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FlinkRevenue {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> lineas = env.readTextFile("ventas.csv");

        DataStream<Tuple2<String, Double>> revenue = lineas
            .map(new MapFunction<String, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(String linea) {
                    String[] campos = linea.split(",");
                    return Tuple2.of(campos[3], Double.parseDouble(campos[2]));
                }
            })
            .filter(new FilterFunction<Tuple2<String, Double>>() {
                @Override
                public boolean filter(Tuple2<String, Double> t) {
                    return t.f1 > 100;
                }
            })
            .keyBy(t -> t.f0)
            .timeWindow(Time.minutes(5))
            .sum(1);

        revenue.print();
        env.execute("Revenue por Region");
    }
}
```

```python
# Equivalente en PyFlink:
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

env = StreamExecutionEnvironment.get_execution_environment()

# En PyFlink, las transformaciones cruzan el boundary JVM-Python via gRPC.
# En Java, todo ejecuta en la JVM directamente.
```

**Preguntas:**

1. ¿`Tuple2<String, Double>` es la forma idiomática de representar
   datos en Flink Java? ¿Hay alternativas más legibles?

2. ¿Flink Java con lambdas es más conciso?
   (ej: `.map(linea -> { ... })`)

3. ¿El plan de ejecución generado por Java y PyFlink
   para el mismo pipeline es idéntico?

4. ¿PyFlink tiene latencia adicional por el boundary gRPC?
   ¿Cuánto para operaciones simples vs complejas?

5. ¿`env.execute()` es equivalente a `pipeline.run()` de Beam?

---

### Ejercicio 15.5.2 — POJO y TypeInformation: el sistema de tipos de Flink

```java
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

// Flink necesita TypeInformation para serializar datos entre operadores.
// En Scala, Flink infiere los tipos automáticamente.
// En Java, a veces necesitas ayuda del compilador.

// POJO que Flink puede serializar automáticamente:
// (debe tener constructor vacío, campos públicos o getters/setters)
public class Transaccion {
    public String id;
    public String userId;
    public double monto;
    public String region;
    public long timestamp;

    public Transaccion() {} // requerido por Flink
    public Transaccion(String id, String userId, double monto,
                       String region, long timestamp) {
        this.id = id; this.userId = userId; this.monto = monto;
        this.region = region; this.timestamp = timestamp;
    }
}

// Flink detecta que Transaccion es un POJO y genera un serializer eficiente.
// Si no cumple las reglas de POJO, Flink usa Kryo (más lento).

// TypeHint para tipos genéricos (el type erasure de Java):
DataStream<Tuple2<String, List<Double>>> stream = input
    .map(x -> Tuple2.of(x.region, List.of(x.monto)))
    .returns(new TypeHint<Tuple2<String, List<Double>>>() {});
// Sin .returns(), Flink no puede inferir el tipo por type erasure → error.
```

**Preguntas:**

1. ¿Por qué Flink necesita un constructor vacío para los POJOs?

2. ¿Records de Java 16+ funcionan como POJOs en Flink?
   ¿O necesitan tratamiento especial?

3. ¿`.returns(new TypeHint<>() {})` es equivalente al `TypeDescriptor`
   de Beam? ¿Ambos resuelven el type erasure?

4. ¿Qué pasa si Flink usa Kryo en lugar del serializer de POJO?
   ¿Cuánto impacta el rendimiento?

5. ¿Scala no tiene este problema porque no tiene type erasure?
   (Pista: sí lo tiene, pero los implicits lo ocultan)

---

### Ejercicio 15.5.3 — ProcessFunction: el operador más potente de Flink

```java
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// ProcessFunction da acceso a:
// - Timestamps y watermarks
// - State (estado por clave)
// - Timers (acciones programadas en el futuro)

public class DetectorFraude extends KeyedProcessFunction<String, Transaccion, String> {

    // Estado por usuario: última transacción procesada
    private ValueState<Transaccion> ultimaTransaccion;
    // Estado por usuario: flag de alerta activa
    private ValueState<Boolean> alertaActiva;

    @Override
    public void open(Configuration params) {
        ultimaTransaccion = getRuntimeContext().getState(
            new ValueStateDescriptor<>("ultima", Transaccion.class));
        alertaActiva = getRuntimeContext().getState(
            new ValueStateDescriptor<>("alerta", Boolean.class));
    }

    @Override
    public void processElement(
            Transaccion tx,
            Context ctx,
            Collector<String> out) throws Exception {

        Transaccion anterior = ultimaTransaccion.value();

        if (anterior != null) {
            // Regla: dos transacciones > $5000 en menos de 1 minuto = fraude
            long diffMs = tx.timestamp - anterior.timestamp;
            if (diffMs < 60_000 && tx.monto > 5000 && anterior.monto > 5000) {
                out.collect("FRAUDE: " + tx.userId +
                    " — $" + anterior.monto + " y $" + tx.monto +
                    " en " + diffMs + "ms");
                alertaActiva.update(true);

                // Timer: limpiar la alerta después de 1 hora
                ctx.timerService().registerEventTimeTimer(
                    tx.timestamp + 3_600_000);
            }
        }
        ultimaTransaccion.update(tx);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<String> out) throws Exception {
        alertaActiva.clear();
        ultimaTransaccion.clear();
    }
}
```

**Preguntas:**

1. ¿El `ValueState` de Flink persiste entre reinicios del job?
   ¿Cómo funciona con checkpoints?

2. ¿Los timers se ejecutan en event time o en processing time?
   ¿Puedes usar ambos en la misma ProcessFunction?

3. ¿Este patrón de detección de fraude es más natural en Flink (Java)
   que en Beam (Java)? ¿Por qué?

4. ¿El state de Flink está en heap o en RocksDB?
   ¿Cómo afecta la elección al rendimiento?

5. ¿Puedes implementar este detector de fraude en Kafka Streams?
   ¿Con qué limitaciones?

---

### Ejercicio 15.5.4 — Flink Table API y SQL en Java

```java
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// Flink Table API: SQL sobre streams en Java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Registrar una fuente Kafka como tabla:
tableEnv.executeSql("""
    CREATE TABLE ventas (
        userId STRING,
        monto DOUBLE,
        region STRING,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'ventas',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
    """);

// Query SQL sobre el stream:
Table resultado = tableEnv.sqlQuery("""
    SELECT
        region,
        TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS ventana_inicio,
        SUM(monto) AS revenue,
        COUNT(*) AS num_ventas
    FROM ventas
    WHERE monto > 0
    GROUP BY
        region,
        TUMBLE(event_time, INTERVAL '5' MINUTE)
    """);

// Escribir resultado a otra tabla Kafka:
tableEnv.executeSql("""
    CREATE TABLE revenue_por_region (
        region STRING,
        ventana_inicio TIMESTAMP(3),
        revenue DOUBLE,
        num_ventas BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'revenue-por-region',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json'
    )
    """);

resultado.executeInsert("revenue_por_region");
```

**Preguntas:**

1. ¿Flink SQL y Beam SQL generan el mismo plan de ejecución
   para la misma query? ¿Cuál es más optimizado?

2. ¿`TUMBLE` en Flink SQL es equivalente a `FixedWindows` en Beam?

3. ¿Puedes mezclar DataStream API y Table API en el mismo job?

4. ¿El `WATERMARK` de Flink SQL tiene el mismo significado
   que en el modelo de Beam?

5. ¿Flink SQL soporta joins temporales (temporal joins)?
   ¿Cuándo los usarías?

---

### Ejercicio 15.5.5 — Implementar: detector de anomalías con Flink Java

**Tipo: Implementar**

```java
// Implementar un detector de anomalías que:
// 1. Lee métricas de servidores desde Kafka (CPU, memoria, latencia)
// 2. Calcula estadísticas por servidor en ventanas deslizantes de 10 min
// 3. Detecta anomalías usando z-score > 3 con ProcessFunction
// 4. Emite alertas al topic "alertas" con contexto (servidor, métrica, valor, z-score)
// 5. Mantiene estado de los últimos 100 valores por servidor para el cálculo

public class DetectorAnomalias {

    public record Metrica(String servidor, String nombre, double valor, long timestamp) {}
    public record Alerta(String servidor, String metrica, double valor,
                         double zScore, long timestamp) {}

    // TODO: implementar la ProcessFunction y el pipeline completo
}
```

**Restricciones:**
1. Usar `ListState` para mantener los últimos 100 valores por servidor
2. Implementar el cálculo de z-score sin librerías externas
3. Usar timers para limpiar estado de servidores inactivos (>30 min sin datos)
4. Escribir tests con `MiniClusterExtension`

---

## Sección 15.6 — Spark Java API y el Ecosistema Hadoop

### Ejercicio 15.6.1 — Spark en Java: la API original

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class SparkJavaJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("spark-java-job")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate();

        Dataset<Row> df = spark.read().parquet("datos/ventas.parquet");

        Dataset<Row> resultado = df
            .filter(col("monto").gt(100))
            .groupBy("region")
            .agg(
                sum("monto").alias("revenue"),
                count("*").alias("transacciones"),
                avg("monto").alias("ticket_promedio"))
            .orderBy(desc("revenue"));

        resultado.write()
            .format("delta")
            .mode("overwrite")
            .save("resultados/revenue_por_region");

        spark.stop();
    }
}
```

**Preguntas:**

1. ¿El API de Spark Java y el de PySpark generan el mismo plan de ejecución?

2. ¿`col("monto").gt(100)` en Java es equivalente a `F.col("monto") > 100`
   en Python? ¿Por qué Java no puede usar operadores?

3. ¿Spark Java tiene Dataset[T] tipado como Scala?
   ¿O solo tiene DataFrame (Dataset<Row>)?

4. ¿Un job de Spark en Java es más rápido que en PySpark
   para operaciones puramente de DataFrame API?

5. ¿Cuándo elegirias Java sobre Scala para un job de Spark?

**Pista:** El plan de ejecución es idéntico para Java, Scala, y Python
cuando solo usas DataFrame API (sin UDFs). El optimizador Catalyst
trabaja sobre el plan lógico, no sobre el lenguaje. Java es más verboso
que Scala para Spark, pero tiene la ventaja de que muchos equipos
ya conocen Java — la curva de aprendizaje es menor que Scala.

---

### Ejercicio 15.6.2 — Hadoop MapReduce: el ancestro (y por qué entenderlo)

```java
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// MapReduce es el framework original de procesamiento distribuido (2004).
// Ya no se usa directamente, pero su modelo influenció todo lo que vino después.

// Mapper: una función que transforma (key, value) → (key, value)*
public class VentasMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        String[] campos = value.toString().split(",");
        String region = campos[3];
        double monto = Double.parseDouble(campos[2]);
        context.write(new Text(region), new DoubleWritable(monto));
    }
}

// Reducer: agrega todos los valores para la misma clave
public class VentasReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws java.io.IOException, InterruptedException {
        double total = 0;
        for (DoubleWritable val : values) {
            total += val.get();
        }
        context.write(key, new DoubleWritable(total));
    }
}

// Driver: configura y lanza el job
public class VentasJob {
    public static void main(String[] args) throws Exception {
        var job = Job.getInstance();
        job.setJarByClass(VentasJob.class);
        job.setMapperClass(VentasMapper.class);
        job.setReducerClass(VentasReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[0]));
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

**Preguntas:**

1. ¿Por qué MapReduce requiere `Writable` types (`Text`, `DoubleWritable`)
   en lugar de `String` y `double`? ¿Qué problema resuelven?

2. ¿El shuffle entre Map y Reduce es equivalente a qué operación en Spark?

3. ¿Por qué MapReduce es 10-100× más lento que Spark para el mismo job?

4. ¿Hay sistemas en producción que aún usan MapReduce en 2024?
   ¿En qué contextos?

5. ¿El concepto de Combiner en MapReduce es equivalente al `reduceByKey`
   local de Spark?

---

### Ejercicio 15.6.3 — HDFS, YARN, y el ecosistema Hadoop desde Java

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

// HDFS es el sistema de archivos distribuido de Hadoop.
// Aunque S3/GCS lo han reemplazado en muchos contextos,
// HDFS sigue siendo fundamental en clusters on-premise.

// Acceder a HDFS desde Java:
Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://namenode:8020");

FileSystem fs = FileSystem.get(conf);

// Listar archivos:
RemoteIterator<LocatedFileStatus> files = fs.listFiles(
    new Path("/datos/ventas/"), true);
while (files.hasNext()) {
    LocatedFileStatus file = files.next();
    System.out.printf("  %s  (%d bytes, %d replicas)%n",
        file.getPath(), file.getLen(), file.getReplication());
}

// Leer un archivo:
try (FSDataInputStream in = fs.open(new Path("/datos/ventas/part-00000.parquet"))) {
    byte[] buffer = new byte[1024];
    int bytesRead = in.read(buffer);
}

// Escribir un archivo:
try (FSDataOutputStream out = fs.create(new Path("/resultados/output.txt"))) {
    out.writeBytes("region,revenue\n");
    out.writeBytes("norte,12345.67\n");
}

// El mismo API funciona con S3 (cambiando fs.defaultFS):
// conf.set("fs.defaultFS", "s3a://mi-bucket/");
// FileSystem s3 = FileSystem.get(conf);
```

**Preguntas:**

1. ¿El API de `FileSystem` de Hadoop funciona con S3, GCS, y Azure Blob?

2. ¿Por qué HDFS usa replicación (3 copias por defecto)
   mientras que S3 usa erasure coding?

3. ¿Los frameworks (Spark, Flink, Beam) usan esta misma API
   para leer datos de HDFS/S3?

4. ¿YARN sigue siendo relevante con Kubernetes para orquestar jobs?

5. ¿Qué ventaja tiene HDFS sobre S3 para jobs que leen los mismos datos
   repetidamente?

---

### Ejercicio 15.6.4 — Parquet en Java: lectura y escritura directa

```java
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;

// Parquet es el formato estándar del ecosistema.
// En Java, se accede via la librería parquet-mr (Parquet para MapReduce).

// Schema Avro para escribir Parquet:
String schemaJson = """
    {
      "type": "record",
      "name": "Transaccion",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "userId", "type": "string"},
        {"name": "monto", "type": "double"},
        {"name": "region", "type": "string"}
      ]
    }
    """;
Schema schema = new Schema.Parser().parse(schemaJson);

// Escribir Parquet:
try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new Path("datos.parquet"))
        .withSchema(schema)
        .build()) {

    GenericRecord record = new GenericRecordBuilder(schema)
        .set("id", "t1")
        .set("userId", "alice")
        .set("monto", 150.0)
        .set("region", "norte")
        .build();
    writer.write(record);
}

// Leer Parquet:
try (ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(new Path("datos.parquet"))
        .build()) {

    GenericRecord record;
    while ((record = reader.read()) != null) {
        System.out.println(record.get("userId") + ": " + record.get("monto"));
    }
}
```

**Preguntas:**

1. ¿Por qué Parquet en Java usa Avro para el schema?
   ¿Es la única opción?

2. ¿Leer Parquet con Java es más rápido que con PyArrow?
   ¿O depende del contexto?

3. ¿Puedes leer solo columnas específicas de un archivo Parquet
   con la API de Java?

4. ¿Parquet en Java soporta predicate pushdown (leer solo filas
   que cumplen un filtro)?

5. ¿Arrow en Java (`arrow-java`) es una alternativa a `parquet-mr`
   para leer Parquet?

---

### Ejercicio 15.6.5 — Implementar: ETL clásico Java con Spark y Parquet

**Tipo: Implementar**

```java
// Implementar un ETL que:
// 1. Lee archivos CSV desde un directorio de HDFS/S3
// 2. Valida el schema (columnas esperadas, tipos correctos)
// 3. Limpia datos (nulls, duplicados, valores fuera de rango)
// 4. Transforma (calcular métricas derivadas)
// 5. Escribe a Parquet particionado por fecha
// 6. Registra estadísticas del job (filas leídas, escritas, errores)

public class EtlVentas {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("etl-ventas")
            .getOrCreate();

        // TODO: implementar
        // Usar try-with-resources para SparkSession
        // Validar schema antes de procesar
        // Separar registros válidos de inválidos
        // Escribir ambos a destinos distintos
    }
}
```

**Restricciones:**
1. Implementar validación de schema con tipos esperados
2. Separar registros válidos de inválidos sin usar UDFs
3. Escribir métricas del job a un archivo JSON de resumen
4. El job debe ser idempotente (re-ejecutar no duplica datos)

---

## Sección 15.7 — Java en el Stack de Datos: Cuándo la Verbosidad es Ventaja

### Ejercicio 15.7.1 — Java vs Python vs Scala: el benchmark de rendimiento

**Tipo: Analizar**

```
Benchmark: el mismo pipeline en tres lenguajes

Pipeline:
  Leer 10 GB de Parquet → filtrar → groupBy → agregar → escribir

               Tiempo total    Startup    Procesamiento
  Java/Spark   45s             5s         40s
  Scala/Spark  44s             5s         39s
  Python/Spark 47s             7s         40s

  ¿Por qué la diferencia es mínima?
  Porque el 95% del tiempo está en la JVM (Spark engine).
  El lenguaje del driver solo importa en:
  - Startup (Python: iniciar intérprete + Py4J)
  - UDFs (Python: boundary JVM-Python)
  - Operaciones que no son DataFrame API

Pipeline con UDFs complejas:
  Leer 10 GB → aplicar UDF custom a cada fila → escribir

               Tiempo total    UDF overhead
  Java/Spark   52s             7s (JVM nativo)
  Scala/Spark  51s             6s (JVM nativo)
  Python/Spark 180s            135s (boundary JVM-Python)

  Aquí la diferencia SÍ importa: 3.5× más lento por el boundary.
```

**Preguntas:**

1. ¿Para pipelines sin UDFs, hay razón técnica para elegir Java sobre Python?

2. ¿El overhead de startup de Python (2s extra) importa para jobs batch?
   ¿Y para jobs de streaming que corren 24/7?

3. ¿Pandas UDFs reducen la brecha de 3.5× a cuánto?

4. ¿El benchmark incluye el tiempo de desarrollo?
   ¿Java es 3× más lento de escribir que Python?

5. ¿Para un nuevo proyecto en 2024, ¿cuál es el criterio de decisión
   entre Java y Python para Spark?

---

### Ejercicio 15.7.2 — Java en producción: las ventajas de la verbosidad

```java
// La verbosidad de Java es una ventaja en producción:

// 1. Tipos explícitos → errores detectados en compilación
public class Pipeline {
    // El compilador garantiza que el schema es correcto:
    public Dataset<Row> procesarVentas(Dataset<Row> ventas, Dataset<Row> clientes) {
        // Si cambias el nombre de una columna, el IDE te avisa inmediatamente
        // (no en runtime como en Python)
        return ventas
            .join(clientes, ventas.col("userId").equalTo(clientes.col("id")))
            .groupBy("region")
            .agg(sum("monto").alias("revenue"));
    }
}

// 2. Excepciones chequeadas → manejo de errores obligatorio
// En Python: nadie te obliga a atrapar JsonDecodeError
// En Java: IOException DEBE manejarse explícitamente
// → menos sorpresas en producción a las 3am

// 3. Herramientas de refactoring maduras
// IntelliJ puede renombrar una clase usada en 500 archivos
// con garantía de corrección. Python IDEs no pueden garantizar lo mismo.

// 4. Ecosistema de testing maduro
// JUnit + Mockito + TestContainers + assertj
// → tests más completos y mantenibles que pytest para proyectos grandes

// 5. Backward compatibility
// Java es extremadamente conservador con breaking changes.
// Código Java de 2005 compila y ejecuta en Java 21 (2023).
// Python 2 → Python 3 rompió ecosistemas enteros.
```

**Preguntas:**

1. ¿Los type hints de Python (mypy) cierran la brecha con los tipos de Java?

2. ¿El manejo de errores explícito de Java produce pipelines más robustos
   que el approach de Python de atrapar excepciones donde quieras?

3. ¿La backward compatibility de Java importa para pipelines de datos
   que se reescriben cada 2 años?

4. ¿El ecosistema de testing de Java es objetivamente mejor
   que el de Python para data engineering?

5. ¿IntelliJ para Java vs VS Code para Python — ¿la experiencia
   del IDE influye en la calidad del código?

---

### Ejercicio 15.7.3 — Árbol de decisión: Java, Scala, Python, o Rust

**Tipo: Analizar**

```
¿Cuándo elegir Java para data engineering en 2024?

  ┌─ ¿Kafka Streams o Kafka Connect?
  │   └─ SÍ → Java (no hay alternativa)
  │
  ├─ ¿Beam con runner Dataflow en producción?
  │   └─ SÍ → Java (SDK más maduro, features primero en Java)
  │
  ├─ ¿Flink con ProcessFunction y estado complejo?
  │   └─ SÍ → Java o Scala (sin boundary gRPC)
  │
  ├─ ¿Spark sin UDFs complejas?
  │   └─ NO a Java → PySpark (productividad > rendimiento)
  │
  ├─ ¿Equipo que ya conoce Java y no Scala?
  │   └─ SÍ → Java (no aprender Scala si no hace falta)
  │
  ├─ ¿Microservicio que también procesa datos?
  │   └─ SÍ → Java (Spring Boot + Kafka + Flink en el mismo stack)
  │
  └─ ¿Procesamiento de datos single-node sin framework?
      └─ NO a Java → Python/Polars o Rust

  Resumen:
  Python: el 80% de los proyectos de data engineering
  Java:   Kafka ecosistema, Beam, Flink producción, equipos Java
  Scala:  Spark avanzado, APIs internas, equipos Scala
  Rust:   librerías de infraestructura (Polars, DataFusion, delta-rs)
```

**Preguntas:**

1. ¿Java está ganando o perdiendo relevancia en data engineering?

2. ¿Kotlin reemplazará a Java para Kafka Streams y Beam?

3. ¿El auge de Flink sobre Spark beneficia a Java sobre Python?

4. ¿Un data engineer debería aprender Java en 2024?
   ¿Cuánto tiempo invertir vs Python?

5. ¿Go es un competidor de Java para data engineering?
   ¿En qué nicho?

---

### Ejercicio 15.7.4 — Integrar Java y Python en el mismo stack

```java
// Patrón común: microservicios Java + pipelines Python

// Java (Kafka Streams): procesamiento de streams en tiempo real
// Python (Airflow + PySpark): pipelines batch diarios
// Java (Flink): streaming analytics
// Python (notebooks): exploración y ML

// Comunicación entre ambos mundos:
// 1. Kafka: Java produce, Python consume (y viceversa)
// 2. Parquet en S3/GCS: Java escribe, Python lee
// 3. Avro schemas compartidos: el schema registry centraliza

// Ejemplo: Java produce eventos enriquecidos, Python los consume para ML
// Java (Kafka Streams):
ventas.join(clientes, enricher)
    .to("ventas-enriquecidas");

// Python (PySpark):
df = spark.read.format("kafka") \
    .option("subscribe", "ventas-enriquecidas") \
    .load()
```

**Preguntas:**

1. ¿Kafka como "bus" entre Java y Python es el patrón estándar?
   ¿Hay alternativas más eficientes?

2. ¿Avro schema registry resuelve el problema de compatibilidad
   entre Java y Python?

3. ¿Parquet como formato de intercambio es más eficiente
   que JSON via Kafka para batch?

4. ¿Es mantenible un stack que mezcla Java y Python?
   ¿Cuándo se justifica?

5. ¿gRPC entre servicios Java y Python es una alternativa a Kafka?

---

### Ejercicio 15.7.5 — El sistema de e-commerce en Java: el pipeline completo

**Tipo: Implementar**

Tomar el pipeline del sistema de e-commerce (Cap.13 en Python, Cap.14 en Scala)
y reimplementar la parte de streaming en Java usando Beam:

```java
package com.ecommerce.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.io.kafka.KafkaIO;

public class StreamingEcommerce {

    public record Evento(String userId, String tipo, double monto,
                         String region, long timestamp) {}
    public record MetricaRegion(String region, double revenue,
                                long numTransacciones, double ticketPromedio) {}

    // TODO: implementar
    // 1. Leer eventos de Kafka
    // 2. Parsear con DoFn + dead letter queue
    // 3. Windowing de 5 minutos con early triggers
    // 4. Calcular MetricaRegion por ventana
    // 5. Escribir a BigQuery (tabla particionada)
    // 6. Escribir DLQ a Cloud Storage
    // 7. Exponer métricas con Beam Metrics

    public static Pipeline buildPipeline(String[] args) {
        // TODO: implementar
        return null;
    }

    public static void main(String[] args) {
        Pipeline p = buildPipeline(args);
        p.run().waitUntilFinish();
    }
}
```

**Restricciones:**
1. Implementar el pipeline completo con manejo de errores
2. Usar Records de Java 16+ para los tipos de datos
3. Implementar tests con `TestPipeline` y `PAssert`
4. Comparar la cantidad de líneas de código con la versión Python del Cap.13

---

## Resumen del capítulo

**Java en data engineering: cuándo es la respuesta correcta**

```
✓ Kafka Streams y Kafka Connect
  → No hay alternativa. El SDK es Java. Punto.

✓ Apache Beam en producción (especialmente con Dataflow)
  → El SDK Java de Beam tiene features primero, mejor documentación,
    y el runner de Dataflow está optimizado para Java.

✓ Apache Flink con estado complejo
  → ProcessFunction, timers, y state access sin boundary gRPC.
    Java y Scala son las opciones nativas.

✓ Equipos que ya conocen Java
  → No aprender Scala o Python si el equipo es productivo en Java.
    La diferencia de rendimiento para DataFrame API es negligible.

✓ Pipelines que necesitan backward compatibility extrema
  → Java no rompe APIs. Un JAR compilado hoy ejecutará en 10 años.

✗ No vale la pena cuando:
  → El pipeline es puramente batch con Spark DataFrame API
    (PySpark es igual de rápido y más productivo)
  → El equipo es de data scientists que conocen Python
  → El procesamiento es single-node (Polars en Python es más simple)
```

**La frase que resume el capítulo:**

```
Java no es el lenguaje más productivo para data engineering.
Pero es el lenguaje en que se escribió la infraestructura de datos.
Cuando necesitas ir más allá de la API de alto nivel —
  escribir un conector, extender un operador, o debuggear el framework —
  el código que lees y escribes es Java.

La verbosidad es el precio. La explícita, los tipos, y la estabilidad
son lo que obtienes a cambio.
```

**Conexión con el Cap.16 (Rust):**

> Java domina la infraestructura de datos existente.
> Rust está construyendo la nueva: Polars, DataFusion, Delta-rs, Arrow2.
> La tendencia: Python como interfaz, Rust como motor.
> La estabilidad: el que más perdura es el más útil en producción —
>   y en producción, Java y Python siguen siendo los más productivos.
>   Rust y Scala son para casos específicos donde cada uno brilla.
>
> La Parte 5 (Cap.17-20) conecta todo: orquestación, observabilidad,
> testing, y el sistema completo de extremo a extremo.

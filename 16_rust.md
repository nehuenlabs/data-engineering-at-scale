# Guía de Ejercicios — Cap.16: Rust — DataFusion, Polars, y Delta-rs

> Rust no llegó al data engineering por accidente.
> Llegó porque un lenguaje de sistemas con gestión de memoria sin GC,
> con SIMD automático, y con seguridad de tipos en tiempo de compilación
> resulta ser exactamente lo que necesitas cuando quieres procesar
> un terabyte de datos en el menor tiempo posible en una sola máquina.
>
> Polars no es "un Pandas en Rust". Es una reimaginación de lo que
> un DataFrame library puede ser cuando no tienes que luchar con el GIL,
> cuando puedes usar SIMD para procesar 8 floats en una instrucción,
> y cuando el compilador garantiza que no tienes data races.
>
> DataFusion no es "un Spark pequeño en Rust". Es un query engine embebido
> que corre en tu proceso, sin JVM, sin serialización entre procesos,
> con el mismo plan de ejecución columnar que Spark pero sin el overhead.
>
> El ecosistema Rust de datos está ganando la batalla del rendimiento.
> La pregunta ya no es "¿puede Rust competir con Java/Python en datos?"
> sino "¿cuándo el overhead de aprender Rust vale la pena?"

---

## El modelo mental: Rust en el stack de datos

```
¿Cómo encaja Rust en el ecosistema de datos?

  Capa de aplicación (Python, Java):
    Tu código de negocio, los pipelines, la orquestación.
    Python/Java siguen siendo el lenguaje de trabajo.

  Capa de motor (Rust):
    Polars, DataFusion, Arrow2, Delta-rs, object_store.
    El procesamiento real ocurre aquí — sin GC, con SIMD.

  La frontera Python-Rust:
    PyO3 / maturin: Python llama a Rust sin overhead de proceso.
    Los datos pasan via Apache Arrow (zero-copy donde es posible).

  Lo que Rust aporta que Java/Python no pueden igualar:
    - Sin GC: no hay "pause to collect garbage" en medio del procesamiento
    - SIMD: el compilador genera instrucciones vectoriales automáticamente
    - Ownership: garantía en compilación de que no hay data races
    - Zero-cost abstractions: el código idiomático es tan rápido como el código bajo nivel
```

---

## Tabla de contenidos

- [Sección 16.1 — Rust para data engineers: el ownership en contexto](#sección-161--rust-para-data-engineers-el-ownership-en-contexto)
- [Sección 16.2 — Apache Arrow en Rust: la base del ecosistema](#sección-162--apache-arrow-en-rust-la-base-del-ecosistema)
- [Sección 16.3 — Polars en Rust: cuando necesitas más que la API Python](#sección-163--polars-en-rust-cuando-necesitas-más-que-la-api-python)
- [Sección 16.4 — DataFusion: SQL embebido en tu aplicación Rust](#sección-164--datafusion-sql-embebido-en-tu-aplicación-rust)
- [Sección 16.5 — Delta-rs: el lakehouse sin JVM](#sección-165--delta-rs-el-lakehouse-sin-jvm)
- [Sección 16.6 — Extender Python con Rust: PyO3 y maturin](#sección-166--extender-python-con-rust-pyo3-y-maturin)
- [Sección 16.7 — Cuándo Rust en data engineering](#sección-167--cuándo-rust-en-data-engineering)

---

## Sección 16.1 — Rust para Data Engineers: el Ownership en Contexto

### Ejercicio 16.1.1 — Leer: el ownership explicado con datos

**Tipo: Leer**

```rust
// El ownership de Rust es el concepto más importante y más extraño para
// un developer de Python/Java. En lugar de GC (Java) o reference counting (Python),
// Rust usa reglas en tiempo de compilación para saber cuándo liberar memoria.

// Regla 1: cada valor tiene exactamente un "dueño" (owner)
let datos: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
// "datos" es el owner del Vec<f64>

// Regla 2: cuando el owner sale de scope, el valor se libera (drop)
{
    let temporal = vec![6.0, 7.0, 8.0];
    // temporal existe aquí
} // temporal se libera aquí — sin GC, sin finalizer, determinístico

// Regla 3: mover el valor transfiere el ownership
let datos2 = datos;  // "datos" ya no es el owner — "datos2" lo es
// println!("{:?}", datos);  // Error de compilación: datos ya no es válido

// Regla 4: borrowing — prestar el valor sin transferir ownership
fn calcular_suma(v: &Vec<f64>) -> f64 {  // & = referencia inmutable (borrow)
    v.iter().sum()
}
let suma = calcular_suma(&datos2);  // prestamos datos2, pero datos2 sigue siendo el owner

// Mutable borrow: solo uno a la vez
fn escalar(v: &mut Vec<f64>, factor: f64) {
    for x in v.iter_mut() {
        *x *= factor;
    }
}
let mut mis_datos = vec![1.0, 2.0, 3.0];
escalar(&mut mis_datos, 2.0);
// mis_datos ahora es [2.0, 4.0, 6.0]
// No puede haber otro &mut simultáneo — el compilador lo garantiza

// ¿Por qué importa en data engineering?
// Un DataFrame de Polars puede contener GBs de datos.
// Con ownership, el compilador garantiza:
//   - No hay dos threads modificando el mismo DataFrame
//   - La memoria se libera exactamente cuando ya no se necesita
//   - No hay "use after free" ni "double free"
```

**Preguntas:**

1. ¿Por qué Rust puede liberar memoria determinísticamente sin GC?
   ¿Cómo sabe el compilador exactamente cuándo liberar?

2. ¿El "move" de Rust es equivalente a pasar un argumento a una función
   en Python? ¿O es diferente?

3. ¿La regla "solo un mutable borrow a la vez" es equivalente a un mutex?
   ¿Con qué diferencias?

4. ¿Cómo maneja Rust los ciclos de referencias (como Python con reference cycles)?

5. ¿El ownership añade overhead de CPU? ¿O es solo una restricción en compilación?

**Pista:** El ownership es puramente un análisis en tiempo de compilación — en runtime
no hay ningún overhead. El compilador rastrea los lifetimes de las variables y genera
código que libera la memoria exactamente en el punto de "drop" (cuando el scope cierra).
En Python, el GC necesita rastrear el conteo de referencias en runtime; en Java,
el GC corre en un thread separado con pausas intermitentes. En Rust, todo esto
ocurre en compilación y el código generado es tan eficiente como C/C++.
Los ciclos de referencias se rompen con `Weak<T>` en lugar de `Arc<T>` —
Rust no tiene ciclos de `Arc` que se auto-limpien, pero son imposibles con el
ownership normal (sin `Rc`/`Arc`).

---

### Ejercicio 16.1.2 — Tipos y traits: el sistema de tipos de Rust en datos

```rust
use std::fmt::Display;

// Traits: interfaces de Rust — equivalentes a los traits de Scala o los protocols de Python
trait Agregable {
    fn sumar(&self, otro: &Self) -> Self;
    fn identidad() -> Self;  // método asociado (como classmethod de Python)
}

impl Agregable for f64 {
    fn sumar(&self, otro: &f64) -> f64 { self + otro }
    fn identidad() -> f64 { 0.0 }
}

impl Agregable for i64 {
    fn sumar(&self, otro: &i64) -> i64 { self + otro }
    fn identidad() -> i64 { 0 }
}

// Función genérica que funciona para cualquier tipo Agregable:
fn reducir<T: Agregable + Copy>(datos: &[T]) -> T {
    datos.iter().fold(T::identidad(), |acc, x| acc.sumar(x))
}

// Usar:
let floats = vec![1.0_f64, 2.0, 3.0, 4.0];
let enteros = vec![10_i64, 20, 30];
println!("{}", reducir(&floats));   // 10.0
println!("{}", reducir(&enteros));  // 60

// Enums: la forma idiomática de manejar opciones y errores en Rust
enum TipoEvento {
    Compra { monto: f64, region: String },
    Click { pagina: String },
    Error { mensaje: String },
}

fn procesar(evento: TipoEvento) -> Option<f64> {
    match evento {
        TipoEvento::Compra { monto, .. } if monto > 0.0 => Some(monto),
        TipoEvento::Compra { .. } => None,
        TipoEvento::Click { .. } => None,
        TipoEvento::Error { mensaje } => {
            eprintln!("Error: {}", mensaje);
            None
        }
    }
}

// Result: manejo de errores sin excepciones
fn parsear_monto(s: &str) -> Result<f64, String> {
    s.parse::<f64>().map_err(|e| format!("No es un número: {}", e))
}

let resultado = parsear_monto("150.5");  // Ok(150.5)
let error = parsear_monto("abc");        // Err("No es un número: ...")
```

**Preguntas:**

1. ¿El `match` de Rust es equivalente al pattern matching de Scala?
   ¿El compilador garantiza exhaustividad?

2. ¿Los traits de Rust son equivalentes a los traits de Scala o a las ABC de Python?
   ¿Cuál es la diferencia principal?

3. ¿`Option<T>` de Rust y `Option[T]` de Scala son equivalentes?
   ¿Cuál es más seguro en la práctica?

4. ¿`Result<T, E>` es preferible a las excepciones de Java/Python en data engineering?
   ¿Para qué casos cada enfoque es mejor?

5. ¿Las funciones genéricas `<T: Trait>` en Rust tienen overhead en runtime?
   ¿O generan código especializado en compilación?

**Pista:** En Rust, los genéricos son "monomorfizados" en compilación:
el compilador genera una versión de `reducir` para `f64` y otra para `i64`,
con código especializado para cada tipo. Esto es equivalente a los templates
de C++ y más eficiente que los genéricos de Java (que usan type erasure
y boxing). Las funciones genéricas de Rust son tan rápidas como funciones
específicas de tipo — zero-cost abstractions. El `match` de Rust es exhaustivo:
si olvidas un caso, el compilador lo detecta. Scala también, pero Python no
(los `match`/`case` de Python 3.10 no son exhaustivos).

---

### Ejercicio 16.1.3 — Iteradores: el corazón del procesamiento de datos en Rust

```rust
// Los iteradores de Rust son lazy, como los LazyList de Scala o los generadores de Python.
// La diferencia: el compilador optimiza las cadenas de iteradores en código eficiente.

let datos: Vec<f64> = (1..=1_000_000).map(|x| x as f64).collect();

// Cadena de iteradores — todo lazy hasta el .collect() o .sum():
let resultado: f64 = datos
    .iter()                          // crear iterador (lazy)
    .filter(|&&x| x > 500_000.0)   // filtrar (lazy)
    .map(|&x| x * 2.0)             // transformar (lazy)
    .sum();                          // terminal — ejecuta todo de una vez

// El compilador fusiona filter + map + sum en un solo loop:
// NO hay memoria intermedia entre filter, map, y sum.
// Es como el "fusion" de transformaciones de Polars / el "whole-stage codegen" de Spark.

// Iteradores en paralelo con Rayon:
use rayon::prelude::*;

let resultado_paralelo: f64 = datos
    .par_iter()                      // iterador PARALELO (Rayon usa ForkJoinPool)
    .filter(|&&x| x > 500_000.0)
    .map(|&x| x * 2.0)
    .sum();

// Con Rayon, los datos se dividen automáticamente entre threads.
// Sin GIL — cada thread procesa su parte en paralelo real.
// Polars usa Rayon internamente para operaciones vectorizadas.
```

**Preguntas:**

1. ¿Los iteradores de Rust son siempre más rápidos que un loop `for` explícito?
   ¿O el compilador los optimiza igual?

2. ¿Rayon es equivalente al `parallelStream()` de Java?
   ¿Cuál es más eficiente y por qué?

3. ¿Los iteradores paralelos de Rayon funcionan con cualquier tipo de datos?
   ¿O hay restricciones de thread safety?

4. ¿La fusión de iteradores (`filter` + `map` + `sum` en un loop) es automática?
   ¿O necesitas configurar algo?

5. ¿Cuándo un loop `for` explícito es más legible que una cadena de iteradores?

---

### Ejercicio 16.1.4 — SIMD: instrucciones vectoriales en Rust

```rust
// SIMD: Single Instruction Multiple Data — procesar N valores con una instrucción CPU.
// Polars y DataFusion lo usan internamente para operaciones numéricas.

// Sin SIMD (loop escalar):
fn suma_escalar(datos: &[f64]) -> f64 {
    let mut suma = 0.0;
    for &x in datos {
        suma += x;
    }
    suma
}

// Con SIMD (instrucciones vectoriales AVX2 — 4 f64 por instrucción):
// El compilador de Rust puede auto-vectorizar si le das permiso:
// RUSTFLAGS="-C target-cpu=native" cargo build --release
// → el compilador genera instrucciones AVX/AVX2 automáticamente para loops simples

// Para control explícito (rare en data engineering — Polars ya lo hace):
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

unsafe fn suma_avx2(datos: &[f64]) -> f64 {
    // Esta función usa instrucciones AVX2 directamente
    // procesando 4 doubles simultáneamente
    // En la práctica, deja que Polars/DataFusion hagan esto por ti
    let mut acum = _mm256_setzero_pd();  // acumulador SIMD de 4 doubles
    let chunks = datos.chunks_exact(4);
    for chunk in chunks {
        let v = _mm256_loadu_pd(chunk.as_ptr());
        acum = _mm256_add_pd(acum, v);
    }
    // Reducir los 4 acumuladores a un escalar:
    let mut resultado = [0.0f64; 4];
    _mm256_storeu_pd(resultado.as_mut_ptr(), acum);
    resultado.iter().sum::<f64>() + chunks.remainder().iter().sum::<f64>()
}
```

**Preguntas:**

1. ¿La auto-vectorización del compilador de Rust es tan efectiva como
   el SIMD manual? ¿Para qué operaciones sí y para cuáles no?

2. ¿Python/NumPy también usa SIMD? ¿Por qué Polars puede ser más rápido
   que NumPy para algunas operaciones si ambos usan SIMD?

3. ¿SIMD requiere que los datos estén alineados en memoria?
   ¿Cómo Arrow garantiza la alineación?

4. ¿Para un data engineer que usa Polars desde Python, necesita saber
   sobre SIMD? ¿O es completamente transparente?

---

### Ejercicio 16.1.5 — Leer: por qué Rust está ganando en infraestructura de datos

**Tipo: Analizar**

```
La tendencia en los últimos 5 años:

  2019: Polars 0.1 — primer DataFrame library en Rust
  2019: DataFusion entra al Apache Software Foundation
  2020: Arrow2 (reescritura de Arrow en Rust puro, más eficiente)
  2021: Delta-rs — el cliente Rust para Delta Lake
  2022: Polars supera a Pandas en benchmarks para la mayoría de operaciones
  2023: Polars 0.19+ — producción en empresas grandes
  2024: DataFusion como motor embebido en InfluxDB, Comet (Spark), Ballista

Por qué Rust ganó frente a C++ para estos proyectos:
  - Seguridad de memoria sin GC: el compilador previene data races y memory leaks
  - Ecosistema moderno: Cargo (build system), crates.io (paquetes), rustfmt
  - Interoperabilidad con Python: PyO3/maturin hace fácil exponer Rust a Python
  - Productividad vs C++: más alto nivel, menos undefined behavior

Por qué Rust no reemplaza a Python como lenguaje de trabajo:
  - La curva de aprendizaje del ownership es real: 2-6 meses para ser productivo
  - La mayoría de los data engineers procesan datos, no construyen motores
  - Python con Polars/Arrow es suficiente para el 95% de los casos de uso
```

**Preguntas:**

1. ¿DataFusion vs DuckDB: ambos son query engines embebidos de alto rendimiento.
   ¿Cuáles son las diferencias clave? ¿Para qué casos cada uno?

2. ¿Por qué empresas como InfluxDB y Databricks (con Comet) apuestan por DataFusion
   en lugar de construir su propio motor desde cero?

3. ¿La tendencia hacia Rust en la capa de infraestructura cambia lo que
   un data engineer necesita saber?

---

## Sección 16.2 — Apache Arrow en Rust: la Base del Ecosistema

### Ejercicio 16.2.1 — Arrow Rust: crear y manipular columnas

```rust
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Arrow en Rust: la implementación de referencia del formato columnar
// Todo el ecosistema (DataFusion, Polars, Delta-rs) se construye sobre esto.

// Crear arrays tipados:
let ids: ArrayRef = Arc::new(StringArray::from(vec!["t1", "t2", "t3"]));
let montos: ArrayRef = Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0]));
let regiones: ArrayRef = Arc::new(StringArray::from(vec!["norte", "sur", "norte"]));

// Definir el schema:
let schema = Arc::new(Schema::new(vec![
    Field::new("id",     DataType::Utf8,    false),
    Field::new("monto",  DataType::Float64, false),
    Field::new("region", DataType::Utf8,    false),
]));

// Crear un RecordBatch (equivalente a pa.Table de Python):
let batch = RecordBatch::try_new(
    schema.clone(),
    vec![ids, montos, regiones],
)?;

println!("Filas: {}", batch.num_rows());
println!("Columnas: {}", batch.num_columns());

// Acceder a una columna:
let col_montos = batch.column_by_name("monto").unwrap();
let montos_typed = col_montos.as_any().downcast_ref::<Float64Array>().unwrap();
let suma: f64 = montos_typed.values().iter().sum();
println!("Suma de montos: {}", suma);  // 450.0

// Operaciones con Arrow compute:
use arrow::compute;
let montos_dobles = compute::multiply_scalar(montos_typed, 2.0)?;
```

**Preguntas:**

1. ¿`Arc<T>` en Rust es equivalente a `Arc` (Atomic Reference Count) en Swift/C++?
   ¿Por qué se usa para `ArrayRef`?

2. ¿Los arrays Arrow en Rust son zero-copy cuando se pasan a Python via PyArrow?

3. ¿`RecordBatch` en Rust Arrow es equivalente a `pa.RecordBatch` en PyArrow?
   ¿Comparten la misma representación en memoria?

4. ¿El `downcast_ref::<Float64Array>()` puede fallar en runtime?
   ¿Por qué Arrow no usa tipos genéricos directamente?

5. ¿Las operaciones `arrow::compute` son SIMD-aceleradas automáticamente?

**Pista:** `Arc<T>` es Atomic Reference Count — permite que múltiples owners
compartan el mismo valor de forma thread-safe. En Arrow, los arrays se comparten
entre RecordBatches y entre threads sin copia. El `downcast_ref` es necesario
porque Arrow usa un tipo dinámico (`dyn Array`) para poder almacenar arrays
de tipos distintos en una misma estructura — en runtime se verifica el tipo y
se obtiene la referencia typed. Sí, `arrow::compute` usa SIMD cuando el compilador
y el target hardware lo permiten — Arrow implementa kernels SIMD explícitos para
las operaciones más comunes.

---

### Ejercicio 16.2.2 — Leer y escribir Parquet desde Rust

```rust
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

// Leer Parquet con el Arrow reader (columnar, eficiente):
fn leer_parquet(ruta: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let archivo = std::fs::File::open(ruta)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(archivo)?;

    println!("Schema: {:?}", builder.schema());
    println!("Filas totales: {}", builder.metadata().file_metadata().num_rows());

    let lector = builder
        .with_batch_size(65536)  // 64K filas por batch
        .build()?;

    lector.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.into())
}

// Escribir Parquet con compresión Snappy:
fn escribir_parquet(
    batches: &[RecordBatch],
    ruta: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let archivo = std::fs::File::create(ruta)?;
    let schema = batches[0].schema();

    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_write_batch_size(65536)
        .build();

    let mut escritor = ArrowWriter::try_new(archivo, schema, Some(props))?;

    for batch in batches {
        escritor.write(batch)?;
    }

    escritor.close()?;
    Ok(())
}
```

**Preguntas:**

1. ¿El reader de Parquet en Rust hace predicate pushdown?
   ¿Puedes filtrar a nivel de row group sin leer todas las filas?

2. ¿`with_batch_size(65536)` afecta cuántos datos se cargan en memoria?
   ¿O solo controla el tamaño del batch devuelto?

3. ¿La escritura de Parquet en Rust con `ArrowWriter` genera el mismo
   formato que la escritura de PySpark o Polars?

4. ¿Puedes leer un Parquet particionado (múltiples archivos en directorios)
   con la librería estándar de Rust? ¿O necesitas DataFusion?

---

### Ejercicio 16.2.3 — Arrow IPC: comunicación zero-copy entre procesos

```rust
use arrow::ipc::{writer::StreamWriter, reader::StreamReader};
use arrow::record_batch::RecordBatch;
use std::io::Cursor;

// Arrow IPC en Rust: serialización eficiente para comunicación entre procesos
// o para guardar datos temporalmente.

fn serializar_arrow(batches: &[RecordBatch]) -> Vec<u8> {
    let schema = batches[0].schema();
    let mut buffer = Vec::new();

    let mut escritor = StreamWriter::try_new(&mut buffer, &schema)
        .expect("No se pudo crear el escritor IPC");

    for batch in batches {
        escritor.write(batch).expect("Error escribiendo batch");
    }
    escritor.finish().expect("Error finalizando");

    buffer
}

fn deserializar_arrow(bytes: &[u8]) -> Vec<RecordBatch> {
    let cursor = Cursor::new(bytes);
    let lector = StreamReader::try_new(cursor, None)
        .expect("No se pudo crear el lector IPC");

    lector
        .filter_map(|b| b.ok())
        .collect()
}

// Benchmark: Arrow IPC vs serde_json para el mismo RecordBatch
// Arrow IPC: ~10ns por elemento (no hay parsing)
// serde_json: ~1μs por elemento (parsing de texto)
// → Arrow IPC es ~100x más rápido para datos numéricos
```

**Restricciones:**
1. Implementar un benchmark comparando Arrow IPC vs JSON para 1M filas
2. ¿El tamaño del buffer Arrow IPC vs JSON para los mismos datos?
3. ¿Cuándo usarías Arrow IPC en lugar de Parquet para comunicación entre procesos?

---

### Ejercicio 16.2.4 — object_store: acceso a S3/GCS/Azure desde Rust

```rust
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;

// object_store: el cliente de almacenamiento en la nube para el ecosistema Rust.
// Usado por DataFusion, Delta-rs, Polars para acceder a S3/GCS/Azure.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let s3 = AmazonS3Builder::new()
        .with_bucket_name("mi-bucket")
        .with_region("us-east-1")
        .build()?;

    // Listar objetos:
    let prefijo = Path::from("datos/ventas/");
    let mut lista = s3.list(Some(&prefijo));
    while let Some(objeto) = lista.next().await.transpose()? {
        println!("Objeto: {} ({} bytes)", objeto.location, objeto.size);
    }

    // Descargar un objeto:
    let ruta = Path::from("datos/ventas/2024/01/parte.parquet");
    let bytes = s3.get(&ruta).await?.bytes().await?;
    println!("Descargado: {} bytes", bytes.len());

    // Subir datos:
    let contenido = bytes::Bytes::from("hola mundo");
    s3.put(&Path::from("resultados/test.txt"), contenido).await?;

    Ok(())
}
```

**Preguntas:**

1. ¿`object_store` usa `tokio` (async) — qué implica para la integración
   con código síncrono como Polars?

2. ¿`object_store` implementa retry automático para errores transitorios de S3?

3. ¿El rendimiento de `object_store` para leer Parquet de S3 es comparable
   al `boto3` de Python o al SDK de Java de AWS?

4. ¿Puedes usar `object_store` con Parquet local (filesystem) con la misma API?

---

### Ejercicio 16.2.5 — Leer: el árbol de dependencias del ecosistema Rust de datos

**Tipo: Analizar**

```
Árbol de dependencias del ecosistema Rust de datos:

         [Polars]          [Delta-rs]
             ↓                  ↓
        [arrow-rs]         [arrow-rs]
             ↓                  ↓
     [parquet-rs]          [parquet-rs]
             ↓                  ↓
       [object_store]     [object_store]
             ↓
         [tokio]

         [DataFusion]
              ↓
         [arrow-rs]
              ↓
      [parquet-rs] + [object_store]

Observaciones:
  - Todos usan arrow-rs como la capa de datos en memoria
  - Todos usan parquet-rs para el formato de archivo
  - Todos usan object_store para acceder a S3/GCS/Azure
  - DataFusion puede usarse como motor de queries en Polars (opcional)
  - Delta-rs usa DataFusion para queries sobre tablas Delta
```

**Preguntas:**

1. ¿El hecho de que todos comparten arrow-rs garantiza que son
   zero-copy entre sí? ¿O hay conversiones intermedias?

2. ¿Polars y DataFusion son competidores o complementarios?
   ¿Hay casos donde usarías ambos?

3. ¿Este árbol de dependencias tiene un "single point of failure"?
   ¿Si arrow-rs tiene un bug crítico, afecta a todo el ecosistema?

---

## Sección 16.3 — Polars en Rust: Cuando Necesitas Más que la API Python

### Ejercicio 16.3.1 — Polars Rust API vs Polars Python API

```rust
use polars::prelude::*;

// Polars en Rust: mismas operaciones que la API Python, pero en Rust.
// Cuándo usarlo directamente: cuando estás construyendo una herramienta
// que necesita procesar datos sin overhead de Python.

fn calcular_revenue(ruta: &str) -> Result<DataFrame, PolarsError> {
    // Lazy frame: construye el plan de ejecución sin ejecutar
    let resultado = LazyFrame::scan_parquet(ruta, ScanArgsParquet::default())?
        .filter(col("monto").gt(lit(0.0)))
        .group_by([col("region")])
        .agg([
            col("monto").sum().alias("revenue"),
            col("user_id").n_unique().alias("usuarios_unicos"),
            col("monto").mean().alias("ticket_promedio"),
        ])
        .sort(
            ["revenue"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()?;

    Ok(resultado)
}

// Comparar con Python:
// df.lazy()
//   .filter(pl.col("monto") > 0)
//   .group_by("region")
//   .agg([pl.sum("monto").alias("revenue"), ...])
//   .collect()

// La API es casi idéntica — la diferencia está en los tipos y el ownership.
// En Rust: el compilador verifica que "monto" existe si estás usando Schema
//          la gestión de memoria es automática y sin GC
//          los errores son Result<T, E>, no excepciones
```

**Preguntas:**

1. ¿La API Rust de Polars y la API Python de Polars generan exactamente
   el mismo plan de ejecución?

2. ¿Para un pipeline de procesamiento de datos puro (sin lógica de negocio
   en Python), cuánto más rápido es Polars Rust que Polars Python?

3. ¿Cuándo tiene sentido usar Polars directamente en Rust vs usarlo desde Python?

4. ¿Polars Rust soporta todas las operaciones de la API Python?
   ¿Hay features solo en Python?

---

### Ejercicio 16.3.2 — Expresiones personalizadas en Polars Rust

```rust
use polars::prelude::*;
use polars_plan::dsl::Expr;

// Polars permite definir expresiones personalizadas en Rust.
// Estas expresiones son ciudadanos de primera clase en el plan de ejecución.
// A diferencia de las UDFs de Python (que rompen la optimización),
// las expresiones Rust se integran en el optimizer de Polars.

// Expresión personalizada: haversine distance
fn distancia_haversine(lat1: Expr, lon1: Expr, lat2: Expr, lon2: Expr) -> Expr {
    let r = 6371.0_f64;  // radio de la Tierra en km

    let d_lat = (lat2.clone() - lat1.clone()).apply(
        |s| Ok(Some(s * std::f64::consts::PI / 180.0)),
        GetOutput::from_type(DataType::Float64),
    );

    // Simplificado — implementación completa requeriría map2 y más expresiones
    // La clave: la función corre dentro de Polars sin crossing de boundary Python-Rust
    (lat1 - lat2).abs().alias("distancia_aproximada")
}

// Usar en un LazyFrame:
let resultado = df.lazy()
    .with_column(
        distancia_haversine(
            col("lat_origen"),
            col("lon_origen"),
            col("lat_destino"),
            col("lon_destino"),
        )
    )
    .collect()?;
```

**Preguntas:**

1. ¿Las expresiones Rust de Polars son optimizadas por el planner de Polars?
   ¿O son tratadas como "opaque UDFs"?

2. ¿Esta misma función en Python (como `map_elements`) sería más lenta?
   ¿En qué factor?

3. ¿Cuándo vale la pena escribir una expresión Rust en lugar de
   usar `map_elements` en Python?

---

### Ejercicio 16.3.3 — Streaming con Polars: procesar más datos que la RAM

```rust
use polars::prelude::*;

// Polars streaming: procesar datasets más grandes que la RAM disponible.
// El engine streaming divide el dataset en chunks y los procesa secuencialmente.

fn procesar_grande(ruta_entrada: &str, ruta_salida: &str) -> Result<(), PolarsError> {
    // scan_parquet es lazy — no carga nada todavía
    let resultado = LazyFrame::scan_parquet(
        ruta_entrada,
        ScanArgsParquet {
            // Permite leer el dataset en chunks:
            ..Default::default()
        }
    )?
    .filter(col("monto").gt(lit(0.0)))
    .group_by([col("region")])
    .agg([col("monto").sum().alias("revenue")])
    // collect_streaming: procesa en chunks, menor uso de memoria
    .collect_streaming()?;

    // Escribir el resultado:
    let mut archivo = std::fs::File::create(ruta_salida)?;
    ParquetWriter::new(&mut archivo)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut resultado.clone())?;

    Ok(())
}
```

**Preguntas:**

1. ¿El modo streaming de Polars soporta todas las operaciones?
   ¿Cuáles no están disponibles en streaming?

2. ¿El streaming de Polars es equivalente al `chunked` reading de Pandas?
   ¿Cuál es más eficiente?

3. ¿El `group_by` en modo streaming puede producir resultados correctos?
   ¿Cómo maneja grupos que están distribuidos en múltiples chunks?

---

### Ejercicio 16.3.4 — Benchmark: Polars Rust vs Polars Python vs Pandas

```rust
// Benchmark: calcular el revenue por región para 100M filas

// Polars Rust:
use std::time::Instant;

let inicio = Instant::now();
let resultado = LazyFrame::scan_parquet("100m_rows.parquet", Default::default())?
    .filter(col("monto").gt(lit(0.0)))
    .group_by([col("region")])
    .agg([col("monto").sum()])
    .collect()?;
println!("Polars Rust: {:?}", inicio.elapsed());
```

```python
# Polars Python:
import polars as pl
import time

inicio = time.perf_counter()
resultado = pl.scan_parquet("100m_rows.parquet") \
    .filter(pl.col("monto") > 0) \
    .group_by("region") \
    .agg(pl.sum("monto")) \
    .collect()
print(f"Polars Python: {time.perf_counter()-inicio:.3f}s")

# Pandas:
import pandas as pd

inicio = time.perf_counter()
df = pd.read_parquet("100m_rows.parquet")
resultado = df[df["monto"] > 0].groupby("region")["monto"].sum()
print(f"Pandas: {time.perf_counter()-inicio:.3f}s")
```

**Restricciones:**
1. Ejecutar los tres benchmarks para 1M, 10M, y 100M filas
2. ¿La diferencia entre Polars Rust y Polars Python es significativa?
   ¿Por qué o por qué no?
3. ¿Para qué tamaño de dataset la diferencia se vuelve relevante?

**Pista:** Para operaciones puramente de DataFrame (filter + groupBy + sum),
la diferencia entre Polars Rust y Polars Python es mínima (< 5%) —
porque el 99% del tiempo se pasa en el motor Rust, no en el wrapper Python.
La diferencia significativa aparece cuando hay UDFs Python o cuando hay
mucha interacción entre Python y el motor (crear muchos DataFrames pequeños).
Pandas puede ser 10-50x más lento que Polars para estos workloads.

---

### Ejercicio 16.3.5 — Leer: Polars como base para herramientas propias

**Tipo: Diseñar**

Un equipo quiere construir una herramienta CLI de procesamiento de datos
que corra en laptops sin instalar Python, R, ni Java:
- Lee Parquet y CSV de S3
- Aplica transformaciones configuradas via YAML
- Escribe resultados a Parquet o CSV
- Debe ser un binario único de < 20 MB

```
Opciones:
  A) Python + Polars + PyInstaller (empaquetar como ejecutable)
     Pro: la lógica es Python, fácil de cambiar
     Con: el ejecutable será > 100 MB con Python runtime

  B) Go con go-polars (bindings Go para Polars)
     Pro: binario pequeño, compilación rápida
     Con: los bindings no están maduros

  C) Rust con Polars nativo
     Pro: binario pequeño (~5 MB), máximo rendimiento, madurez del ecosistema
     Con: curva de aprendizaje, tiempo de compilación lento

  D) C++ con Arrow C++
     Pro: máximo control
     Con: mucho más complejo que Rust, sin ecosystem Polars
```

**Preguntas:**
1. ¿Cuál elegirías y por qué?
2. ¿Hay un "opción E" que no se consideró?
3. ¿Cuánto tiempo tomaría implementar la opción C con un equipo
   sin experiencia previa en Rust?

---

## Sección 16.4 — DataFusion: SQL Embebido en tu Aplicación Rust

### Ejercicio 16.4.1 — DataFusion: el query engine como librería

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;

// DataFusion: un query engine completo que corre dentro de tu proceso.
// No hay servidor, no hay cluster — es una librería que añades a tu aplicación.
// El mismo SQL que escribirías en Spark SQL, pero en Rust, en tu proceso.

#[tokio::main]
async fn main() -> Result<(), datafusion::error::DataFusionError> {
    // Crear el contexto de ejecución:
    let ctx = SessionContext::new();

    // Registrar tablas (Parquet local, S3, memoria):
    ctx.register_parquet("ventas", "datos/ventas.parquet", Default::default())
        .await?;

    ctx.register_parquet("clientes", "datos/clientes.parquet", Default::default())
        .await?;

    // Ejecutar SQL:
    let df = ctx.sql("
        SELECT
            c.region,
            SUM(v.monto) AS revenue,
            COUNT(DISTINCT v.user_id) AS usuarios_unicos,
            AVG(v.monto) AS ticket_promedio
        FROM ventas v
        LEFT JOIN clientes c ON v.user_id = c.id
        WHERE v.monto > 0
          AND v.fecha >= '2024-01-01'
        GROUP BY c.region
        ORDER BY revenue DESC
    ").await?;

    // Recoger los resultados (como Arrow RecordBatches):
    let resultados: Vec<RecordBatch> = df.collect().await?;
    println!("Resultado: {} filas", resultados.iter().map(|b| b.num_rows()).sum::<usize>());

    // O mostrar en pantalla:
    df.show().await?;

    Ok(())
}
```

**Preguntas:**

1. ¿DataFusion usa Parquet predicate pushdown automáticamente?
   ¿Y proyección de columnas (no leer columnas no necesarias)?

2. ¿DataFusion puede leer datos de S3 directamente? ¿Cómo?

3. ¿El SQL de DataFusion es compatible con el SQL de Spark SQL?
   ¿Cuáles son las diferencias principales?

4. ¿DataFusion tiene un optimizer de queries comparable a Catalyst de Spark?

5. ¿Para qué tamaño de datos DataFusion supera a DuckDB?
   ¿Y a qué tamaño DuckDB supera a DataFusion?

**Pista:** DataFusion y DuckDB son competidores directos para análisis en
una sola máquina. DuckDB tiene mejor soporte SQL (más completo, más maduro),
mejor gestión de datos que no caben en RAM, y una API más simple para
análisis interactivo. DataFusion tiene mejor extensibilidad (es una librería,
no un proceso separado), mejor integración con el ecosistema Rust/Arrow,
y mejor base para construir sistemas custom (como InfluxDB IOx o Comet de Spark).
Para análisis puro en una máquina, DuckDB suele ser la mejor elección.
Para construir sistemas que necesitan embeber un query engine, DataFusion gana.

---

### Ejercicio 16.4.2 — Extender DataFusion con UDFs en Rust

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::{create_udf, ColumnarValue};
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

// UDF en DataFusion: función SQL personalizada implementada en Rust.
// A diferencia de las UDFs de Python en Spark, estas corren directamente
// en el motor — sin overhead de cruce de proceso.

fn haversine_udf(args: &[ColumnarValue]) -> Result<ColumnarValue, datafusion::error::DataFusionError> {
    let lat1 = &args[0];
    let lon1 = &args[1];
    let lat2 = &args[2];
    let lon2 = &args[3];

    // Extraer los arrays del ColumnarValue:
    let lat1_arr = match lat1 {
        ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<Float64Array>().unwrap(),
        _ => return Err(datafusion::error::DataFusionError::Internal("tipo inesperado".to_string())),
    };
    // ... (similar para los otros)

    let r = 6371.0_f64;
    let mut resultados = Vec::with_capacity(lat1_arr.len());

    for i in 0..lat1_arr.len() {
        let a_lat = lat1_arr.value(i).to_radians();
        let b_lat = /* lat2 */  0.0_f64.to_radians();
        let d_lat = b_lat - a_lat;
        let d_lon = 0.0_f64;  // simplificado
        let a = (d_lat / 2.0).sin().powi(2)
              + a_lat.cos() * b_lat.cos() * (d_lon / 2.0).sin().powi(2);
        resultados.push(2.0 * r * a.sqrt().asin());
    }

    Ok(ColumnarValue::Array(Arc::new(Float64Array::from(resultados))))
}

// Registrar y usar:
let ctx = SessionContext::new();
ctx.register_udf(create_udf(
    "haversine",
    vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Float64],
    Arc::new(DataType::Float64),
    datafusion::logical_expr::Volatility::Immutable,
    Arc::new(haversine_udf),
));

let resultado = ctx.sql("
    SELECT id, haversine(lat_origen, lon_origen, lat_destino, lon_destino) AS distancia_km
    FROM entregas
    WHERE distancia_km > 50
").await?;
```

**Preguntas:**

1. ¿Las UDFs de DataFusion pueden ser SIMD-aceleradas?
   ¿Cómo lo harías para la función `haversine`?

2. ¿DataFusion puede "ver dentro" de las UDFs para optimizarlas?
   ¿O las trata como cajas negras como Spark con las UDFs Python?

3. ¿El `Volatility::Immutable` de la UDF permite constant folding?
   ¿Qué significa en la práctica?

4. ¿Puedes exponer esta UDF a Python (para usarla desde pandas o polars)?

---

### Ejercicio 16.4.3 — DataFusion como motor de Comet (Spark)

```
Databricks Comet (Apache Comet):
  Un plugin para Spark que reemplaza el motor de ejecución de Spark
  con DataFusion/Arrow para operaciones compatible.

  Arquitectura:
    Tu job PySpark/Scala Spark (sin cambios)
         ↓
    Spark planifica como siempre
         ↓
    Comet intercepta los operadores físicos compatibles
         ↓
    DataFusion/Arrow ejecuta esos operadores en Rust (más rápido)
    Los operadores no soportados vuelven al motor JVM de Spark

  Beneficio: sin cambios en el código, 2-3× más rápido para operaciones numéricas

  Ejemplo de operadores que Comet puede acelerar:
    - Filter, Project (selección de columnas)
    - GroupBy + Agg con funciones básicas (sum, count, avg)
    - Sort
    - Hash Join (para BroadcastHashJoin)
    - Scan de Parquet (con predicate pushdown nativo)

  Limitaciones (2024):
    - No todas las funciones SQL están implementadas en Comet
    - No soporta UDFs Python (siguen yendo al JVM de Spark)
    - El fallback al motor JVM puede causar conversiones costosas
```

**Preguntas:**

1. ¿Comet es una alternativa a reescribir el job en Polars?
   ¿Para qué casos cada enfoque es mejor?

2. ¿El "fallback al motor JVM" cuando Comet no soporta una operación
   causa overhead adicional?

3. ¿Comet puede usarse con PySpark o solo con la API Java/Scala de Spark?

4. ¿DataFusion dentro de Comet es zero-copy respecto a los datos del RDD de Spark?

---

### Ejercicio 16.4.4 — Ballista: DataFusion distribuido

```rust
// Ballista: DataFusion con distribución en cluster.
// Es a DataFusion lo que Spark es a un engine de SQL local.
// Estado actual (2024): experimental, no production-ready en la mayoría de casos.

// La idea:
//   DataFusion es el motor en cada nodo (executor)
//   Ballista añade la capa de distribución (scheduler, coordinador)
//   El SQL es el mismo que DataFusion local

// ¿Por qué no es production-ready aún?
//   - Falta de operadores distribuidos maduros (shuffle, broadcast join)
//   - Sin HA (High Availability) del scheduler
//   - Mucho menos maduro que Spark (15 años de desarrollo vs 4)
//   - Sin ecosistema de conectores comparable a Spark

// Cuándo Ballista podría ser relevante:
//   - Workloads que requieren distribución y máximo rendimiento
//   - Equipos Rust que no quieren introducir JVM
//   - Investigación y prototipado
```

**Preguntas:**

1. ¿Ballista podría reemplazar a Spark en 5 años?
   ¿Qué le falta hoy que Spark tiene?

2. ¿El hecho de que Databricks esté desarrollando Comet (DataFusion como motor en Spark)
   indica que apuestan por DataFusion pero no por reemplazar Spark?

3. ¿Para qué caso de uso concreto recomendarías Ballista en 2024?

---

### Ejercicio 16.4.5 — Leer: DataFusion vs DuckDB vs Spark para análisis local

**Tipo: Comparar**

```
Para el mismo workload: 50 GB de Parquet, query SQL compleja con joins y aggregations,
en una máquina con 32 GB RAM y 16 cores.

DuckDB:
  + SQL más completo (window functions, QUALIFY, etc.)
  + Gestión automática de datos que no caben en RAM (spill to disk)
  + CLI simple, REPL interactivo
  + Integración excelente con Python (via duckdb-python)
  - No es embebible como librería Rust fácilmente
  - Menos extensible para casos de uso custom

DataFusion:
  + Embebible como librería Rust/Python
  + Extensible: UDFs, nuevos operadores, nuevas fuentes de datos
  + Mismo ecosistema Arrow que Polars y Delta-rs
  - SQL menos completo que DuckDB
  - Menos maduro para análisis interactivo

Spark:
  + Muy maduro, enorme ecosistema
  + Distribución automática si los datos no caben en una máquina
  - Overhead de JVM, no embebible
  - Lento para inicializar (SparkSession ~30 segundos)
  - Overkill para 50 GB en una máquina con 32 GB RAM
```

**Restricciones:**
1. Para el workload descrito, rankear las tres opciones y justificar
2. ¿Hay una "cuarta opción" que no se consideró?
3. ¿La respuesta cambia si el dataset es 500 GB en lugar de 50 GB?

---

## Sección 16.5 — Delta-rs: el Lakehouse Sin JVM

### Ejercicio 16.5.1 — Delta-rs: leer y escribir tablas Delta en Rust

```rust
use deltalake::{DeltaTableBuilder, DeltaOps};
use deltalake::arrow::record_batch::RecordBatch;

// Delta-rs: el cliente Rust para Delta Lake.
// Permite leer/escribir tablas Delta sin JVM, directamente desde Rust o Python.

#[tokio::main]
async fn main() -> Result<(), deltalake::DeltaTableError> {
    // Abrir una tabla Delta existente:
    let tabla = DeltaTableBuilder::from_uri("s3://mi-bucket/tabla-ventas")
        .with_storage_options([
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
        ].into())
        .load()
        .await?;

    println!("Versión actual: {}", tabla.version());
    println!("Schema: {:?}", tabla.schema());

    // Leer con DataFusion:
    let ctx = SessionContext::new();
    let _ = ctx.register_table("ventas", Arc::new(tabla))?;

    let df = ctx.sql("SELECT region, SUM(monto) FROM ventas GROUP BY region").await?;
    df.show().await?;

    // Escribir nuevos datos (append):
    let nuevos_datos: Vec<RecordBatch> = vec![/* ... */];
    let operaciones = DeltaOps::try_from_uri("s3://mi-bucket/tabla-ventas").await?;

    let (tabla_actualizada, metricas) = operaciones
        .write(nuevos_datos)
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("Archivos añadidos: {}", metricas.num_added_files);

    Ok(())
}
```

**Preguntas:**

1. ¿Delta-rs puede leer tablas Delta escritas por Spark (y viceversa)?
   ¿El formato es completamente compatible?

2. ¿Delta-rs soporta ACID transactions? ¿Cómo implementa el log de transacciones?

3. ¿El time travel de Delta (leer versiones anteriores) está soportado en Delta-rs?

4. ¿Delta-rs puede hacer MERGE (upsert) como el Delta Lake de Spark?

---

### Ejercicio 16.5.2 — Delta-rs desde Python: el uso más común

```python
# deltalake es el paquete Python que usa Delta-rs como backend.
# Es la forma más popular de usar Delta Lake sin Spark.

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import polars as pl

# Leer una tabla Delta:
tabla = DeltaTable("s3://mi-bucket/ventas")

# Como Arrow:
batches = tabla.to_pyarrow()
print(f"Filas: {batches.num_rows}")

# Como Polars:
df = pl.from_arrow(tabla.to_pyarrow())

# Como Pandas:
df_pandas = tabla.to_pandas()

# Escribir datos nuevos:
df_nuevo = pl.DataFrame({
    "user_id": ["alice", "bob"],
    "monto": [100.0, 200.0],
    "region": ["norte", "sur"],
})

write_deltalake(
    "s3://mi-bucket/ventas",
    df_nuevo.to_arrow(),
    mode="append",
)

# Time travel:
tabla_v3 = DeltaTable("s3://mi-bucket/ventas", version=3)
df_historico = tabla_v3.to_pandas()

# Optimizar la tabla (compactar pequeños archivos):
tabla.optimize.compact()
tabla.vacuum(retention_hours=168)  # eliminar archivos > 7 días
```

**Preguntas:**

1. ¿`write_deltalake()` con `mode="overwrite"` es ACID?
   ¿Qué garantías da si el proceso falla a mitad de la escritura?

2. ¿`tabla.to_pyarrow()` carga todos los datos en memoria?
   ¿O es lazy?

3. ¿El `vacuum()` puede eliminar datos accesibles via time travel?
   ¿Cómo decides cuántas horas de retención?

4. ¿Delta-rs Python es compatible con las tablas Delta creadas por Databricks?
   ¿Y con las de Apache Iceberg?

---

### Ejercicio 16.5.3 — Delta-rs vs Delta Spark: cuándo cada uno

```
Delta Lake original (Spark):
  Requiere: SparkSession, JVM, cluster o máquina con Spark instalado
  Ventajas:
    - MERGE completo con condiciones complejas
    - Integración con el ecosistema Spark (MLlib, Structured Streaming)
    - DML completo: UPDATE, DELETE, MERGE
    - Schema evolution automático con mergeSchema
    - Generación de manifiestos para Athena/Redshift Spectrum

Delta-rs:
  Requiere: pip install deltalake (Rust compilado)
  Ventajas:
    - Sin JVM, instalación trivial
    - Funciona desde Python, Rust, sin servidor
    - Lectura muy rápida (motor Rust, predicate pushdown)
    - Integración nativa con Polars y PyArrow
  Limitaciones (2024):
    - MERGE básico (no todas las condiciones)
    - Algunos tipos de schema evolution no soportados
    - Streaming a Delta (write) experimental
```

**Preguntas:**

1. ¿Para un pipeline batch simple (append diario a una tabla Delta),
   cuál es la elección correcta en 2024?

2. ¿Puede un pipeline usar Delta-rs para escrituras y Spark para lecturas
   (o viceversa)? ¿El formato es 100% compatible?

3. ¿La integración de Delta-rs con Delta Kernel (el proyecto de compatibilidad
   multi-motor de Linux Foundation) mejora la situación?

---

### Ejercicio 16.5.4 — Implementar: un pipeline ETL con Polars + Delta-rs

```python
# Pipeline de producción: Polars para transformaciones, Delta-rs para el lakehouse
# Sin Spark, sin JVM, instalable con pip en cualquier máquina

from deltalake import DeltaTable, write_deltalake
import polars as pl
from datetime import datetime, timedelta
import pyarrow as pa

def pipeline_etl_diario(
    fecha: str,
    ruta_origen: str,
    ruta_destino: str,
) -> dict:
    """
    ETL diario: leer datos crudos, transformar con Polars, escribir a Delta.
    """
    metricas = {}

    # LEER: datos crudos del día
    df_crudo = pl.scan_parquet(f"{ruta_origen}/fecha={fecha}/*.parquet")

    # TRANSFORMAR: con Polars (lazy evaluation)
    df_procesado = (
        df_crudo
        .filter(pl.col("monto") > 0)
        .with_columns([
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("monto").log1p().alias("log_monto"),
            pl.when(pl.col("monto") > 1000)
              .then(pl.lit("premium"))
              .otherwise(pl.lit("standard"))
              .alias("segmento"),
        ])
        .group_by(["region", "segmento"])
        .agg([
            pl.sum("monto").alias("revenue"),
            pl.n_unique("user_id").alias("usuarios"),
            pl.count().alias("transacciones"),
        ])
        .collect()
    )

    metricas["filas_procesadas"] = len(df_procesado)

    # ESCRIBIR: a Delta Lake con particionado
    write_deltalake(
        ruta_destino,
        df_procesado.to_arrow(),
        mode="append",
        partition_by=["region"],
        schema_mode="merge",
    )

    return metricas
```

**Restricciones:**
1. Implementar el pipeline completo con manejo de errores
2. Añadir idempotencia: si se ejecuta dos veces para la misma fecha,
   el resultado debe ser el mismo
3. Medir el throughput (filas/segundo) comparado con una implementación PySpark

---

### Ejercicio 16.5.5 — Leer: el futuro del lakehouse multi-motor

**Tipo: Analizar**

```
Delta Lake, Apache Iceberg, y Apache Hudi son los tres formatos
de tabla abierta principales. El objetivo de todos: que cualquier
motor pueda leer y escribir las tablas.

Delta Kernel (2023):
  Un proyecto de Linux Foundation para estandarizar el acceso a Delta Lake.
  Delta-rs, Spark, Trino, Flink pueden todos usar el mismo kernel.

Iceberg en Rust (iceberg-rust, 2023):
  Apache Iceberg está siendo implementado en Rust.
  Aún en estado alpha.

El patrón emergente:
  Motor de proceso (Spark, Polars, DataFusion, DuckDB)
       ↓
  Formato de tabla (Delta, Iceberg, Hudi)
       ↓
  Almacenamiento (S3, GCS, Azure ADLS)

Cualquier combinación debería funcionar.
```

**Preguntas:**

1. ¿Delta Kernel garantiza que un job en Polars + Delta-rs y un job en Spark
   pueden operar sobre la misma tabla sin conflictos?

2. ¿El ecosistema Rust de datos eventualmente tendrá soporte completo
   para todas las features de Delta, Iceberg, y Hudi?

3. ¿Para un nuevo lakehouse en 2024, qué formato elegirías:
   Delta, Iceberg, o Hudi? ¿Cambia la respuesta si el equipo es Python-first?

---

## Sección 16.6 — Extender Python con Rust: PyO3 y Maturin

### Ejercicio 16.6.1 — PyO3: el bridge Python-Rust

```rust
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

// PyO3: el framework para escribir módulos Python nativos en Rust.
// Polars, Pydantic v2, y muchas otras librerías lo usan.

// Definir una función Python implementada en Rust:
#[pyfunction]
fn calcular_percentil(datos: Vec<f64>, percentil: f64) -> PyResult<f64> {
    if datos.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "datos no puede estar vacío"
        ));
    }

    let mut sorted = datos.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let idx = ((percentil / 100.0) * (sorted.len() - 1) as f64) as usize;
    Ok(sorted[idx])
}

// Definir una clase Python implementada en Rust:
#[pyclass]
struct ProcesadorDatos {
    datos: Vec<f64>,
}

#[pymethods]
impl ProcesadorDatos {
    #[new]
    fn nuevo(datos: Vec<f64>) -> Self {
        ProcesadorDatos { datos }
    }

    fn suma(&self) -> f64 {
        self.datos.iter().sum()
    }

    fn media(&self) -> f64 {
        self.suma() / self.datos.len() as f64
    }

    fn percentil(&self, p: f64) -> PyResult<f64> {
        calcular_percentil(self.datos.clone(), p)
    }
}

// Registrar el módulo:
#[pymodule]
fn mi_modulo_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calcular_percentil, m)?)?;
    m.add_class::<ProcesadorDatos>()?;
    Ok(())
}
```

```python
# Usar desde Python:
import mi_modulo_rust

datos = [1.0, 5.0, 3.0, 8.0, 2.0, 9.0, 4.0]
p95 = mi_modulo_rust.calcular_percentil(datos, 95.0)

proc = mi_modulo_rust.ProcesadorDatos(datos)
print(proc.media())    # 4.57...
print(proc.percentil(75.0))  # 7.0
```

**Preguntas:**

1. ¿La conversión `Vec<f64>` ↔ Python list tiene overhead de copia?
   ¿Cómo se compara con usar PyArrow para transferir los datos?

2. ¿Las excepciones de Rust (`PyErr`) se comportan igual que
   las excepciones de Python cuando llegan al caller Python?

3. ¿`#[pyclass]` genera código que cumple con el GIL de Python?
   ¿Cómo libera el GIL para operaciones costosas?

4. ¿PyO3 soporta async functions (para integrar con tokio)?

---

### Ejercicio 16.6.2 — Maturin: publicar un paquete Rust como wheel Python

```toml
# Cargo.toml para un módulo Python en Rust:
[package]
name = "mi-procesador"
version = "0.1.0"
edition = "2021"

[lib]
name = "mi_procesador"
crate-type = ["cdylib"]  # necesario para crear la .so/.dll de Python

[dependencies]
pyo3 = { version = "0.20", features = ["extension-module"] }
rayon = "1.8"
arrow = "49.0"
```

```bash
# Instalar maturin:
pip install maturin

# Desarrollar localmente (compila e instala en el entorno activo):
maturin develop --release

# Construir wheels para distribución:
maturin build --release
# Genera: target/wheels/mi_procesador-0.1.0-cp311-cp311-linux_x86_64.whl

# Con soporte para múltiples plataformas (usando Docker + cross-compilation):
maturin build --release --target x86_64-apple-darwin
maturin build --release --target x86_64-pc-windows-msvc
```

**Preguntas:**

1. ¿El wheel generado por maturin incluye el runtime de Rust?
   ¿O el usuario necesita tener Rust instalado?

2. ¿Puedes publicar el wheel en PyPI y que cualquier usuario lo instale
   con `pip install mi-procesador`?

3. ¿Cross-compilation para múltiples plataformas (Linux, macOS, Windows)
   es fácil con maturin?

4. ¿El tiempo de compilación de Rust afecta la experiencia de desarrollo?
   ¿Cómo se mitiga?

---

### Ejercicio 16.6.3 — Arrow zero-copy entre Python y Rust via PyO3

```rust
use pyo3::prelude::*;
use pyo3::types::PyAny;
use arrow::array::{Float64Array, ArrayRef};
use arrow::pyarrow::PyArrowType;
use std::sync::Arc;

// Recibir un PyArrow Array en Rust sin copia:
#[pyfunction]
fn procesar_arrow(py: Python, array: PyArrowType<ArrayRef>) -> PyResult<PyArrowType<ArrayRef>> {
    let arr = array.0;  // extraer el ArrayRef de Rust

    // El array Arrow está en memoria compartida — no hay copia
    let datos = arr.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Se esperaba Float64Array"
        ))?;

    // Procesar en Rust (SIMD, Rayon, lo que necesites):
    let resultado: Vec<f64> = datos.values()
        .iter()
        .map(|&x| x * 2.0 + 1.0)
        .collect();

    // Retornar como Arrow array:
    let resultado_arr: ArrayRef = Arc::new(Float64Array::from(resultado));
    Ok(PyArrowType(resultado_arr))
}
```

```python
import pyarrow as pa
import mi_modulo_rust

# El array se pasa a Rust SIN COPIA (zero-copy via Arrow C Data Interface):
arr = pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64())
resultado = mi_modulo_rust.procesar_arrow(arr)
print(resultado)  # [3.0, 5.0, 7.0, 9.0, 11.0]
```

**Preguntas:**

1. ¿La transferencia del array PyArrow → Rust con `PyArrowType` es realmente
   zero-copy? ¿Bajo qué condiciones?

2. ¿Este patrón funciona con columnas Polars (que son Arrow arrays internamente)?

3. ¿Hay restricciones sobre qué operaciones Rust puede hacer sobre
   un array Arrow recibido zero-copy?

4. ¿Este patrón es cómo Polars Python llama al motor Rust internamente?

---

### Ejercicio 16.6.4 — Cuándo escribir extensiones Rust para Python

**Tipo: Analizar**

```
Señales de que necesitas una extensión Rust:

  1. Una operación específica es el cuello de botella (> 50% del tiempo)
     y no existe en Polars/NumPy/Arrow

  2. La lógica requiere acceso a bajo nivel que Python no puede hacer
     eficientemente (parseo de formato binario propietario, hash específico)

  3. Necesitas paralelismo real sin GIL y el workload no cabe en Polars/NumPy

  4. Construyes una librería Python que necesita máxima eficiencia
     (como Pydantic v2, que reescribió su motor de validación en Rust)

Señales de que NO necesitas una extensión Rust:

  1. La operación ya existe en Polars, NumPy, o PyArrow
  2. El cuello de botella es I/O (red, disco) — Rust no ayuda ahí
  3. El cuello de botella es el overhead de Spark, no el cómputo Python
  4. El equipo no tiene experiencia en Rust (curva de aprendizaje de 2-6 meses)
```

**Restricciones:**
Para cada caso, decidir si una extensión Rust es apropiada:
1. Un parser de archivos CSV con formato propietario de un sistema legacy (100 GB/día)
2. Un modelo de scoring de riesgo crediticio (scikit-learn, 100K solicitudes/día)
3. Una función de hash personalizada que se aplica a 1B de registros por hora
4. Un agregador de series temporales que calcula percentiles en ventanas deslizantes

---

### Ejercicio 16.6.5 — Leer: el estado del arte de PyO3 en 2024

**Tipo: Analizar**

```
Proyectos que usan PyO3 en producción (2024):
  - Polars: el DataFrame library más rápido en Python
  - Pydantic v2: el validator de datos más usado en Python
  - cryptography: el paquete criptográfico estándar de Python
  - tokenizers (HuggingFace): tokenizador para LLMs
  - ruff: el linter Python más rápido
  - uv: el package manager Python más rápido

El patrón común:
  Todos son librerías Python que tienen un hot path con requisitos de rendimiento
  que Python puro no puede satisfacer. Usan PyO3 para ese hot path.
  La API Python sigue siendo la interfaz de usuario — Rust es invisible.
```

**Preguntas:**

1. ¿El éxito de Pydantic v2 (reescrita en Rust) indica que más librerías
   Python seguirán el mismo camino?

2. ¿ruff (linter Python escrito en Rust) es un indicador de una tendencia
   más amplia de herramientas de desarrollo Python → Rust?

3. ¿Hay alguna razón por la que una librería de data engineering
   no debería reescribir su hot path en Rust?

---

## Sección 16.7 — Cuándo Rust en Data Engineering

### Ejercicio 16.7.1 — El árbol de decisión: Rust vs Python/Java/Scala

```
¿Necesitas construir un motor de datos (query engine, storage engine, format parser)?
  Sí → Rust (DataFusion, Arrow como base)
  No → continúa...

¿Necesitas procesar datos con máxima eficiencia en una sola máquina?
  Sí → ¿Polars Python es suficiente?
    Sí → usa Polars Python (sin aprender Rust)
    No → ¿la limitación es una operación específica?
      Sí → extensión PyO3 para esa operación
      No → Polars Rust o DataFusion nativo

¿Necesitas procesar datos distribuidos?
  Sí → PySpark / Flink / Beam (Rust no tiene alternativa madura aquí)

¿Construyes herramientas de datos (CLI, pipelines sin servidor)?
  Sí → Rust + Polars/DataFusion/Delta-rs (binarios portables, sin dependencias)
  No → Python es suficiente

¿El equipo tiene o puede adquirir experiencia en Rust?
  No → mantén Python/Java/Scala, usa librerías Rust via Python
```

**Preguntas:**

1. ¿El árbol de decisión cambia si el equipo ya sabe Rust?
   ¿La curva de aprendizaje es el único obstáculo?

2. ¿Para qué caso de uso de data engineering Rust es CLARAMENTE la mejor opción?
   ¿Y para cuál es la peor?

3. ¿La tendencia de usar Rust para la capa de motor pero Python para la capa
   de aplicación es un patrón temporal o permanente?

---

### Ejercicio 16.7.2 — Benchmark final: el mismo workload en cuatro lenguajes

```
Workload: leer 10 GB de Parquet, filtrar, agregar por clave, escribir a Parquet.
Hardware: 16 cores, 32 GB RAM, NVMe SSD.

Referencia de resultados esperados (orientativos):

  Python + Pandas:        180s   (leer: 60s, procesar: 110s, escribir: 10s)
  Python + Polars:         18s   (leer: 8s, procesar: 7s, escribir: 3s)
  PySpark (local[16]):     45s   (overhead de JVM + shuffle)
  Rust + Polars:           16s   (overhead de Python ~2s eliminado)
  Rust + DataFusion:       22s   (más genérico, menos optimizado para DataFrames)
  Java + Spark:            40s   (sin overhead Python, pero con JVM)
  Scala + Spark:           38s   (igual que Java para este workload)

Observaciones:
  - Polars Python ≈ Polars Rust (el overhead de Python es marginal)
  - Polars >> Pandas (10x para este workload)
  - Polars > Spark local (Spark tiene overhead de cluster management)
  - El beneficio de Rust over Python solo aparece en workloads muy intensivos
```

**Preguntas:**

1. ¿Por qué Polars Python es casi tan rápido como Polars Rust?
   ¿En qué workloads la diferencia sería más grande?

2. ¿El tiempo de Spark podría reducirse con configuración (AQE, más particiones)?

3. ¿Estos tiempos cambian significativamente si los datos están en S3
   en lugar de SSD local?

---

### Ejercicio 16.7.3 — El pipeline del sistema de e-commerce en Rust

**Tipo: Implementar**

Implementar el componente de procesamiento batch del sistema de e-commerce
usando Rust + Polars + Delta-rs — el caso donde Rust es apropiado:
una herramienta de análisis que corre en laptops de analistas sin instalar Python:

```rust
use polars::prelude::*;
use deltalake::{DeltaOps, DeltaTableBuilder};
use clap::Parser;
use anyhow::Result;

/// Herramienta CLI de analytics de e-commerce
#[derive(Parser)]
#[command(name = "analytics-ecommerce")]
struct Args {
    /// Fecha a procesar (YYYY-MM-DD)
    #[arg(short, long)]
    fecha: String,

    /// Ruta de los datos de entrada (local o s3://)
    #[arg(short, long)]
    entrada: String,

    /// Ruta de la tabla Delta de salida
    #[arg(short, long)]
    salida: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Procesando fecha: {}", args.fecha);

    // TODO: implementar usando Polars y Delta-rs
    // 1. Leer datos de entrada (Parquet local o S3)
    // 2. Filtrar y transformar con Polars (lazy evaluation)
    // 3. Calcular métricas: revenue por región, usuarios únicos, ticket promedio
    // 4. Escribir a tabla Delta con particionado por fecha
    // 5. Mostrar resumen de métricas

    Ok(())
}
```

**Restricciones:**
1. Implementar la lógica completa
2. El binario compilado debe funcionar sin Python ni Java instalados
3. Añadir `--help` y mensajes de progreso
4. Comparar el tiempo de ejecución con la versión Python (Cap.13) y Java (Cap.15)

---

### Ejercicio 16.7.4 — Reflexión: Rust en el data engineering de 2024

**Tipo: Reflexionar**

```
Lo que Rust ha logrado en data engineering (2024):
  ✓ Dominó la capa de motores (Polars, DataFusion, Arrow, Delta-rs)
  ✓ Demostró que es posible tener velocidad de C sin la inseguridad de C
  ✓ Creó un ecosistema cohesivo (todos usan arrow-rs como base)
  ✓ Hizo Python más rápido via PyO3 (Polars, Pydantic, ruff, uv)

Lo que Rust NO ha logrado (todavía):
  ✗ Procesamiento distribuido maduro (Ballista es experimental)
  ✗ Reemplazar Spark/Flink para workloads que requieren distribución
  ✗ Ecosistema de conectores tan amplio como el de Java/Python
  ✗ Herramientas de orquestación (Airflow, Dagster son Python)

La predicción (2024-2030):
  La capa de motores será Rust.
  La capa de aplicación seguirá siendo Python/Java.
  La frontera Python-Rust se volverá más fluida con mejores herramientas.
  Rust distribuido (si Ballista madura) podría desafiar a Spark.
```

**Preguntas:**

1. ¿Aprender Rust en 2024 es una inversión con buen ROI para un data engineer?
   ¿Para qué perfil de data engineer?

2. ¿El auge de Rust implica que Python se volverá menos relevante?
   ¿O que Python se vuelve más relevante como interfaz de Rust?

3. ¿Hay algún aspecto del data engineering donde Rust no podrá entrar
   nunca o difícilmente?

---

### Ejercicio 16.7.5 — El cierre de la Parte 4: una visión unificada

**Tipo: Sintetizar**

Después de los capítulos 13 (Python), 14 (Scala), 15 (Java), y 16 (Rust),
responder las preguntas de síntesis:

```
Pregunta 1: Para un nuevo proyecto de data engineering en 2024,
  ¿qué lenguaje y qué stack elegirías para cada caso?

  a) Pipeline batch: 500 GB de Parquet, transformaciones SQL, escribir a Delta Lake
  b) Pipeline streaming: Kafka → transformaciones con estado → Kafka (< 100ms)
  c) Motor de datos embebido: herramienta CLI que procesa datos en laptops sin deps
  d) Pipeline ML: features engineering + training + serving (todas las etapas)

Pregunta 2: ¿Cuál es el denominador común entre todos los lenguajes?
  Todos convergen en: Apache Arrow como formato en memoria,
  Apache Parquet como formato en disco, y S3/GCS como almacenamiento.

Pregunta 3: ¿Qué falta en el ecosistema actual?
  Un runtime distribuido en Rust con la madurez de Spark.
  Orquestación nativa en Rust.
  Schema management multi-lenguaje sin fricciones.
```

---

## Resumen del capítulo

**El rol de Rust en data engineering en 2024:**

```
Rust dominó la capa de motores:
  ✓ Polars: el DataFrame library más rápido para una máquina
  ✓ DataFusion: el query engine embebido más extensible
  ✓ Delta-rs: lakehouse sin JVM
  ✓ Arrow/Parquet en Rust: la base de todo el ecosistema

El patrón que ganó:
  Python (interfaz de usuario)
       ↓ PyO3 (zero overhead)
  Rust (motor de procesamiento)
       ↓ Apache Arrow (formato universal)
  Almacenamiento (Parquet + S3)

Cuándo Rust en data engineering:
  ✓ Construir motores o herramientas de infraestructura de datos
  ✓ Extensiones Python para operaciones críticas no disponibles en librerías
  ✓ Herramientas CLI de datos portables (un binario, sin dependencias)
  ✓ Cuando Polars Python es suficiente (es Rust por debajo — ya usas Rust)

Cuándo NO Rust:
  ✗ Procesamiento distribuido (todavía no hay alternativa madura a Spark/Flink)
  ✗ Cuando Python + Polars es suficiente (no hay ganancia significativa)
  ✗ Cuando el equipo no tiene o no quiere adquirir conocimiento de Rust
```

**El cierre de la Parte 4:**

```
Python: el lenguaje de productividad. Orquesta, conecta, y expresa.
Scala:  el lenguaje nativo de Spark. Acceso a APIs internas y Dataset tipado.
Java:   el lenguaje del ecosistema empresarial. Kafka Streams, Spring Batch, legado.
Rust:   el lenguaje de los motores. Sin GC, SIMD, seguridad de memoria.

La tendencia: Python como interfaz, Rust como motor.
La estabilidad: el que más perdura es el más útil en producción —
  y en producción, Java y Python siguen siendo los más productivos.
  Rust y Scala son para casos específicos donde cada uno brilla.

La Parte 5 (Cap.17-20) conecta todo: orquestación, observabilidad,
testing, y el sistema completo de extremo a extremo.
```

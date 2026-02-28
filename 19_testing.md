# Guía de Ejercicios — Cap.19: Testing de Pipelines de Datos

> La observabilidad (Cap.18) detecta problemas en producción.
> El testing previene que esos problemas lleguen a producción.
>
> Testear un pipeline de datos es más difícil que testear un microservicio.
> Un microservicio recibe un request y retorna un response — el contrato
> es claro. Un pipeline recibe datos que no controlas,
> los transforma de formas complejas, y escribe en sistemas distribuidos
> que no puedes mockear fácilmente.
>
> El data engineer que dice "funciona en mi laptop con el CSV de prueba"
> y el que dice "pasa 47 tests incluyendo data skew, schema evolution,
> late events, y un Spark cluster de 3 nodos en CI"
> tienen dos niveles distintos de confianza en su código.
>
> Este capítulo enseña cómo llegar al segundo nivel.

---

## El modelo mental: la pirámide de testing para datos

```
La pirámide de testing clásica (software):

        ┌───────────┐
        │  E2E      │  pocos, lentos, frágiles
        ├───────────┤
        │Integration│  medios en cantidad y velocidad
        ├───────────┤
        │  Unit     │  muchos, rápidos, estables
        └───────────┘

La pirámide de testing para data engineering:

        ┌───────────────────┐
        │  Pipeline E2E     │  el pipeline completo contra datos reales
        │  (staging env)    │  (lento, costoso, máxima confianza)
        ├───────────────────┤
        │  Integration      │  componentes conectados: Spark + Delta Lake
        │  (mini-cluster)   │  (medio, requiere infraestructura)
        ├───────────────────┤
        │  Component        │  una transformación contra datos sintéticos
        │  (local Spark)    │  (rápido, buena cobertura)
        ├───────────────────┤
        │  Unit             │  funciones puras sin framework
        │  (pytest)         │  (muy rápido, cobertura limitada)
        ├───────────────────┤
        │  Contract         │  schema, tipos, constraints entre equipos
        │  (schema registry)│  (automático, previene roturas silenciosas)
        └───────────────────┘

  La diferencia clave: en software, los inputs son predecibles (requests).
  En data engineering, los inputs son impredecibles (datos del mundo real).
  → Necesitas una capa extra: contract tests.
  → Y los tests de componente son más importantes que los unit tests.
```

```
¿Qué testeas en un pipeline de datos?

  1. La LÓGICA de transformación
     "¿La función que calcula revenue maneja correctamente descuentos,
      devoluciones, y monedas distintas?"
     → Unit tests

  2. La INTEGRACIÓN con frameworks
     "¿La transformación funciona en Spark con particiones,
      serialización, y shuffles reales?"
     → Component tests (Spark local)

  3. El CONTRATO de datos
     "¿La tabla upstream tiene las columnas y tipos que espero?
      ¿Si cambian, me entero antes de que rompa mi pipeline?"
     → Contract tests

  4. La INFRAESTRUCTURA
     "¿El pipeline puede leer de S3, escribir a BigQuery,
      y manejar credenciales correctamente?"
     → Integration tests

  5. El COMPORTAMIENTO end-to-end
     "¿Desde la fuente hasta el dashboard, los números son correctos?"
     → E2E tests (staging environment)
```

---

## Tabla de contenidos

- [Sección 19.1 — Unit testing de transformaciones: funciones puras primero](#sección-191--unit-testing-de-transformaciones-funciones-puras-primero)
- [Sección 19.2 — Testing de Spark: local, rápido, confiable](#sección-192--testing-de-spark-local-rápido-confiable)
- [Sección 19.3 — Contract testing: el acuerdo entre productor y consumidor](#sección-193--contract-testing-el-acuerdo-entre-productor-y-consumidor)
- [Sección 19.4 — Testing de streaming: Kafka, Flink, y Beam](#sección-194--testing-de-streaming-kafka-flink-y-beam)
- [Sección 19.5 — Integration testing: contra sistemas reales](#sección-195--integration-testing-contra-sistemas-reales)
- [Sección 19.6 — Testing de DAGs y orquestación](#sección-196--testing-de-dags-y-orquestación)
- [Sección 19.7 — El e-commerce testeado: CI/CD para pipelines de datos](#sección-197--el-e-commerce-testeado-cicd-para-pipelines-de-datos)

---

## Sección 19.1 — Unit Testing de Transformaciones: Funciones Puras Primero

### Ejercicio 19.1.1 — Leer: por qué la mayoría de pipelines no se testean

**Tipo: Leer**

```
Excusas comunes para no testear pipelines de datos:

  1. "Los datos son impredecibles — no puedes testear lo inesperado."
     → Puedes testear que tu código MANEJA lo inesperado correctamente.
       ¿Qué hace tu pipeline con un null en user_id?
       ¿Y con un monto negativo? ¿Y con un string donde esperas un float?

  2. "Levantar Spark para un test es muy lento."
     → Separa la lógica de transformación del framework.
       La función que calcula revenue es Python puro — testeala sin Spark.
       El test de Spark verifica la integración, no la lógica.

  3. "No puedo mockear BigQuery / S3 / Kafka."
     → No mockees TODO. Usa layers:
       - Unit tests: funciones puras, sin I/O
       - Component tests: Spark local con datos sintéticos
       - Integration tests: contra servicios reales en staging

  4. "Los tests se rompen cada vez que cambian los datos."
     → Usa datos sintéticos determinísticos para tests.
       Los datos reales son para validación en producción (Cap.18),
       no para tests en CI.

  5. "No tenemos tiempo para escribir tests."
     → No tienes tiempo para NO escribir tests.
       Cada incidente en producción que un test habría prevenido
       cuesta 10× más que el test.

El principio fundamental:
  Separar la lógica de transformación de la infraestructura.
  La lógica se testea con unit tests (rápido, determinístico).
  La infraestructura se testea con integration tests (lento, completo).
```

**Preguntas:**

1. ¿Qué porcentaje de equipos de data engineering tiene tests automatizados?

2. ¿La cultura de testing en data engineering está más atrasada
   que en software engineering? ¿Por qué?

3. ¿dbt tests (Cap.18) cuentan como "testing de pipelines"?

4. ¿Un pipeline con 100% de cobertura de unit tests
   pero 0% de integration tests es confiable?

5. ¿Es posible testear un pipeline de ML de la misma forma
   que un pipeline de ETL?

---

### Ejercicio 19.1.2 — El patrón: separar lógica de framework

```python
# ANTES (no testeable):
# La lógica de negocio está mezclada con Spark.
def pipeline_ventas(spark, fecha):
    df = spark.read.parquet(f"s3://data-lake/raw/ventas/fecha={fecha}")
    df = df.filter(df.monto > 0)
    df = df.withColumn("revenue_con_iva", df.monto * 1.19)
    df = df.groupBy("region").agg(
        F.sum("revenue_con_iva").alias("revenue"),
        F.countDistinct("user_id").alias("usuarios"),
    )
    df.write.mode("overwrite").parquet(f"s3://data-lake/analytics/metricas/{fecha}")

# DESPUÉS (testeable):
# Paso 1: extraer la lógica a funciones puras.
def calcular_revenue_con_iva(monto: float, tasa_iva: float = 0.19) -> float:
    """Calcular revenue con IVA. Función pura, sin Spark."""
    if monto <= 0:
        return 0.0
    return round(monto * (1 + tasa_iva), 2)

def es_venta_valida(monto: float, user_id: str) -> bool:
    """Validar si una venta es procesable."""
    return monto > 0 and user_id is not None and user_id.strip() != ""

# Paso 2: testear las funciones puras con pytest.
def test_revenue_con_iva():
    assert calcular_revenue_con_iva(100.0) == 119.0
    assert calcular_revenue_con_iva(0.0) == 0.0
    assert calcular_revenue_con_iva(-50.0) == 0.0
    assert calcular_revenue_con_iva(100.0, tasa_iva=0.0) == 100.0

def test_venta_valida():
    assert es_venta_valida(100.0, "alice") is True
    assert es_venta_valida(0.0, "alice") is False
    assert es_venta_valida(100.0, None) is False
    assert es_venta_valida(100.0, "") is False
    assert es_venta_valida(100.0, "  ") is False

# Paso 3: usar las funciones puras en el pipeline de Spark.
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, BooleanType

revenue_udf = F.udf(calcular_revenue_con_iva, FloatType())
valida_udf = F.udf(es_venta_valida, BooleanType())

def pipeline_ventas_testeable(df):
    """Pipeline que usa funciones puras — la lógica ya está testeada."""
    return df.filter(valida_udf(df.monto, df.user_id)) \
             .withColumn("revenue_con_iva", revenue_udf(df.monto)) \
             .groupBy("region").agg(
                 F.sum("revenue_con_iva").alias("revenue"),
                 F.countDistinct("user_id").alias("usuarios"),
             )
```

**Preguntas:**

1. ¿La separación lógica/framework añade complejidad
   o la reduce?

2. ¿UDFs de PySpark tienen overhead de serialización.
   ¿Es un problema para testing o solo para producción?

3. ¿Puedes testear la misma lógica con Pandas DataFrames
   en vez de Spark DataFrames?

4. ¿`calcular_revenue_con_iva` debería manejar monedas distintas?
   ¿Cómo testeas eso?

5. ¿Las funciones puras deben estar en un módulo separado
   del código del pipeline?

**Pista:** El patrón se llama "Functional Core, Imperative Shell".
La lógica de negocio (core) es pura: sin I/O, sin estado, sin side effects.
El pipeline (shell) es imperativo: lee datos, aplica la lógica, escribe resultados.
Testeas el core exhaustivamente con unit tests.
Testeas el shell con integration tests más ligeros.

---

### Ejercicio 19.1.3 — Datos de test: fixtures y factories

```python
import pytest
import polars as pl
from datetime import date

# Fixtures: datos de test reutilizables.

@pytest.fixture
def ventas_normales() -> pl.DataFrame:
    """Dataset de ventas sin anomalías — el caso feliz."""
    return pl.DataFrame({
        "user_id": ["alice", "bob", "carol", "dave", "eve"],
        "monto": [100.0, 200.0, 150.0, 300.0, 50.0],
        "region": ["norte", "sur", "norte", "este", "sur"],
        "fecha": [date(2024, 3, 14)] * 5,
    })

@pytest.fixture
def ventas_con_anomalias() -> pl.DataFrame:
    """Dataset con todos los edge cases que el pipeline debe manejar."""
    return pl.DataFrame({
        "user_id": ["alice", None, "carol", "", "eve", "frank"],
        "monto": [100.0, 200.0, -50.0, 0.0, 50.0, 999_999.99],
        "region": ["norte", "sur", "norte", None, "desconocida", "este"],
        "fecha": [date(2024, 3, 14)] * 6,
    })

@pytest.fixture
def ventas_vacias() -> pl.DataFrame:
    """Dataset vacío — el pipeline no debe fallar."""
    return pl.DataFrame({
        "user_id": pl.Series([], dtype=pl.Utf8),
        "monto": pl.Series([], dtype=pl.Float64),
        "region": pl.Series([], dtype=pl.Utf8),
        "fecha": pl.Series([], dtype=pl.Date),
    })


# Factory: generar datos paramétricos.
def crear_ventas(n: int, monto_range=(10.0, 500.0),
                 regiones=("norte", "sur", "este", "oeste"),
                 fecha=date(2024, 3, 14)) -> pl.DataFrame:
    """Factory para generar N ventas con distribución controlada."""
    import random
    random.seed(42)  # determinístico para tests
    return pl.DataFrame({
        "user_id": [f"user_{i}" for i in range(n)],
        "monto": [round(random.uniform(*monto_range), 2) for _ in range(n)],
        "region": [random.choice(regiones) for _ in range(n)],
        "fecha": [fecha] * n,
    })


# Usar en tests:
def test_pipeline_caso_feliz(ventas_normales):
    resultado = transformar_ventas(ventas_normales)
    assert len(resultado) > 0
    assert resultado["revenue"].sum() > 0
    assert resultado["revenue"].null_count() == 0

def test_pipeline_con_anomalias(ventas_con_anomalias):
    resultado = transformar_ventas(ventas_con_anomalias)
    # No debe haber registros con monto <= 0:
    assert resultado.filter(pl.col("revenue") < 0).height == 0

def test_pipeline_vacio(ventas_vacias):
    resultado = transformar_ventas(ventas_vacias)
    assert len(resultado) == 0  # No falla, retorna vacío

def test_pipeline_escala():
    df = crear_ventas(1_000_000)  # un millón de ventas
    resultado = transformar_ventas(df)
    assert len(resultado) <= 4  # máximo 4 regiones
```

**Preguntas:**

1. ¿`random.seed(42)` garantiza datos determinísticos entre ejecuciones?
   ¿Y entre versiones de Python?

2. ¿Fixtures de pytest vs factory functions — cuándo cada una?

3. ¿Testear con 1 millón de filas es un unit test o un performance test?

4. ¿Los edge cases del fixture `ventas_con_anomalias` cubren
   los escenarios que realmente ocurren en producción?

5. ¿Deberías usar datos reales anonimizados como fixtures?

---

### Ejercicio 19.1.4 — Parametrize: testear múltiples escenarios

```python
import pytest

# pytest.mark.parametrize: ejecutar el mismo test con distintos inputs.

@pytest.mark.parametrize("monto,esperado", [
    (100.0, 119.0),      # caso normal
    (0.0, 0.0),          # cero → cero
    (-50.0, 0.0),        # negativo → cero (no hay revenue negativo)
    (0.01, 0.01),        # centavo mínimo
    (999_999.99, 1_189_999.99),  # monto grande
    (100.0 / 3, 39.68),  # decimales con redondeo
])
def test_revenue_con_iva(monto, esperado):
    assert calcular_revenue_con_iva(monto) == esperado


@pytest.mark.parametrize("user_id,monto,esperado", [
    ("alice", 100.0, True),     # válido
    (None, 100.0, False),       # null user_id
    ("", 100.0, False),         # empty user_id
    ("  ", 100.0, False),       # whitespace user_id
    ("alice", 0.0, False),      # monto cero
    ("alice", -1.0, False),     # monto negativo
    ("alice", None, False),     # null monto
])
def test_venta_valida(user_id, monto, esperado):
    assert es_venta_valida(monto, user_id) == esperado


# Parametrize con IDs legibles en el output del test:
@pytest.mark.parametrize("regiones,n_esperado", [
    (["norte", "norte", "sur"], 2),
    (["norte"], 1),
    ([], 0),
    (["norte", "sur", "este", "oeste"], 4),
], ids=["dos_regiones", "una_region", "vacio", "todas_las_regiones"])
def test_regiones_unicas(regiones, n_esperado):
    df = pl.DataFrame({"region": regiones, "monto": [100.0] * len(regiones),
                        "user_id": [f"u{i}" for i in range(len(regiones))]})
    resultado = transformar_ventas(df)
    assert len(resultado) == n_esperado
```

**Preguntas:**

1. ¿Cuántos parámetros en un `parametrize` son demasiados?
   ¿10? ¿50? ¿100?

2. ¿Los `ids` mejoran la legibilidad del output de pytest?

3. ¿`parametrize` con fixtures es posible?
   ¿Cómo se combina `parametrize` con `@pytest.fixture`?

4. ¿Property-based testing (Hypothesis) reemplaza a `parametrize`?

5. ¿Cada parámetro es un test independiente que puede fallar por separado?

---

### Ejercicio 19.1.5 — Property-based testing: testear con datos que no imaginaste

**Tipo: Implementar**

```python
from hypothesis import given, strategies as st, settings, assume
import polars as pl

# Property-based testing: en vez de elegir inputs específicos,
# describes las PROPIEDADES que deben cumplirse para CUALQUIER input válido.

# Hypothesis genera cientos de inputs aleatorios y verifica las propiedades.

@given(monto=st.floats(min_value=0.01, max_value=1_000_000, allow_nan=False))
def test_revenue_siempre_mayor_que_monto(monto):
    """Para cualquier monto positivo, revenue_con_iva >= monto."""
    resultado = calcular_revenue_con_iva(monto)
    assert resultado >= monto

@given(monto=st.floats(max_value=0.0, allow_nan=False, allow_infinity=False))
def test_revenue_cero_para_monto_no_positivo(monto):
    """Para cualquier monto <= 0, revenue = 0."""
    assert calcular_revenue_con_iva(monto) == 0.0

@given(
    montos=st.lists(
        st.floats(min_value=0.01, max_value=10_000, allow_nan=False),
        min_size=1, max_size=1000,
    ),
    regiones=st.lists(
        st.sampled_from(["norte", "sur", "este", "oeste"]),
        min_size=1, max_size=1000,
    ),
)
@settings(max_examples=50)
def test_transformacion_no_pierde_revenue(montos, regiones):
    """La suma de revenue por región == la suma total."""
    n = min(len(montos), len(regiones))
    montos = montos[:n]
    regiones = regiones[:n]

    df = pl.DataFrame({
        "user_id": [f"u{i}" for i in range(n)],
        "monto": montos,
        "region": regiones,
    })
    resultado = transformar_ventas(df)

    revenue_total = resultado["revenue"].sum()
    revenue_esperado = sum(calcular_revenue_con_iva(m) for m in montos)
    assert abs(revenue_total - revenue_esperado) < 0.01  # tolerancia float
```

**Preguntas:**

1. ¿Property-based testing encuentra bugs que `parametrize` no encuentra?

2. ¿`max_examples=50` es suficiente? ¿Cuántas iteraciones en CI?

3. ¿Hypothesis puede generar DataFrames completos?
   ¿Existe una estrategia para eso?

4. ¿Los tests de Hypothesis son determinísticos?
   ¿Cómo reproduces un fallo?

5. ¿Cuándo property-based testing es overkill para data engineering?

**Pista:** Hypothesis guarda los inputs que producen fallos en una database local
y los reproduce en cada ejecución. Si un test falla con el input
`monto=0.005`, Hypothesis recordará ese caso y lo verificará siempre.
Para DataFrames, puedes usar `pandera.hypothesis` o construir estrategias
custom combinando `st.fixed_dictionaries` + `st.lists`.

---

## Sección 19.2 — Testing de Spark: Local, Rápido, Confiable

### Ejercicio 19.2.1 — Leer: la SparkSession local para tests

**Tipo: Leer**

```python
# Spark puede correr en modo "local" — sin cluster, sin HDFS, sin YARN.
# Ideal para tests: rápido de levantar, mismo motor de ejecución.

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """SparkSession local para tests — se crea una vez por sesión de tests."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    yield spark
    spark.stop()

# ¿Por qué estas configuraciones?
# local[2]:           2 cores — suficiente para tests, detecta race conditions
# shuffle.partitions=2: por defecto son 200 — absurdo para tests con 10 filas
# ui.enabled=false:   no necesitas la Spark UI en CI
# bindAddress:        evita problemas de DNS en containers de CI

# ¿scope="session"?
# Crear una SparkSession tarda ~5 segundos.
# Con scope="session", se crea UNA vez para TODOS los tests.
# Sin scope="session", se crea una por cada test → 5s × 50 tests = 4 minutos de overhead.
```

**Preguntas:**

1. ¿`local[2]` vs `local[*]` para tests — cuál es mejor?

2. ¿`shuffle.partitions=2` puede ocultar bugs
   que solo aparecen con 200 particiones?

3. ¿La SparkSession de tests es idéntica a la de producción?
   ¿Qué diferencias importan?

4. ¿`scope="session"` introduce dependencias entre tests?

5. ¿Levantar Spark en CI (GitHub Actions, GitLab CI)
   requiere configuración especial?

---

### Ejercicio 19.2.2 — Component tests: testear transformaciones en Spark

```python
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from datetime import date

# El test de componente verifica la transformación EN Spark,
# con datos sintéticos controlados.

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

@pytest.fixture
def schema_ventas():
    return StructType([
        StructField("user_id", StringType(), nullable=True),
        StructField("monto", FloatType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("fecha", DateType(), nullable=False),
    ])

@pytest.fixture
def df_ventas(spark, schema_ventas):
    data = [
        Row(user_id="alice", monto=100.0, region="norte", fecha=date(2024, 3, 14)),
        Row(user_id="bob", monto=200.0, region="sur", fecha=date(2024, 3, 14)),
        Row(user_id="carol", monto=150.0, region="norte", fecha=date(2024, 3, 14)),
    ]
    return spark.createDataFrame(data, schema=schema_ventas)


def calcular_metricas(df):
    """La transformación que queremos testear."""
    return df.filter(F.col("monto") > 0) \
             .groupBy("region") \
             .agg(
                 F.sum("monto").alias("revenue"),
                 F.countDistinct("user_id").alias("usuarios_unicos"),
                 F.count("*").alias("transacciones"),
             )


class TestCalcularMetricas:

    def test_agregacion_por_region(self, df_ventas):
        resultado = calcular_metricas(df_ventas)
        assert resultado.count() == 2  # norte y sur

        norte = resultado.filter(F.col("region") == "norte").collect()[0]
        assert norte["revenue"] == 250.0  # 100 + 150
        assert norte["usuarios_unicos"] == 2  # alice + carol
        assert norte["transacciones"] == 2

    def test_filtra_montos_negativos(self, spark, schema_ventas):
        data = [
            Row(user_id="alice", monto=-50.0, region="norte", fecha=date(2024, 3, 14)),
            Row(user_id="bob", monto=100.0, region="norte", fecha=date(2024, 3, 14)),
        ]
        df = spark.createDataFrame(data, schema=schema_ventas)
        resultado = calcular_metricas(df)
        assert resultado.collect()[0]["revenue"] == 100.0  # solo bob

    def test_dataset_vacio(self, spark, schema_ventas):
        df = spark.createDataFrame([], schema=schema_ventas)
        resultado = calcular_metricas(df)
        assert resultado.count() == 0

    def test_un_usuario_multiples_transacciones(self, spark, schema_ventas):
        data = [
            Row(user_id="alice", monto=100.0, region="norte", fecha=date(2024, 3, 14)),
            Row(user_id="alice", monto=200.0, region="norte", fecha=date(2024, 3, 14)),
        ]
        df = spark.createDataFrame(data, schema=schema_ventas)
        resultado = calcular_metricas(df)
        fila = resultado.collect()[0]
        assert fila["revenue"] == 300.0
        assert fila["usuarios_unicos"] == 1
        assert fila["transacciones"] == 2
```

**Preguntas:**

1. ¿Estos tests prueban la lógica o prueban Spark?

2. ¿`.collect()` en tests es aceptable? ¿Es lento?

3. ¿Los datos del fixture deben cubrir todos los edge cases
   o solo el caso feliz?

4. ¿`class TestCalcularMetricas` vs funciones sueltas
   — cuál es mejor para organizar tests de Spark?

5. ¿Cuánto tiempo tardan 50 tests de Spark en CI?

---

### Ejercicio 19.2.3 — chispa y pyspark-test: aserciones para DataFrames

```python
# Comparar DataFrames en tests es tedioso con .collect().
# Librerías como chispa y pyspark-test lo simplifican.

# Opción 1: chispa
from chispa import assert_df_equality, assert_column_equality
from pyspark.sql import Row

def test_con_chispa(spark):
    input_df = spark.createDataFrame([
        Row(region="norte", monto=100.0),
        Row(region="sur", monto=200.0),
    ])

    resultado = calcular_metricas(input_df)

    esperado = spark.createDataFrame([
        Row(region="norte", revenue=100.0, usuarios_unicos=1, transacciones=1),
        Row(region="sur", revenue=200.0, usuarios_unicos=1, transacciones=1),
    ])

    assert_df_equality(resultado, esperado, ignore_row_order=True)

# assert_df_equality compara:
# - Mismo schema (columnas y tipos)
# - Mismas filas (contenido exacto)
# - ignore_row_order=True: no importa el orden de las filas
# - ignore_nullable=True: no compara nullable del schema

# Opción 2: comparar columnas específicas
def test_revenue_columna(spark):
    input_df = spark.createDataFrame([
        Row(region="norte", monto=100.0),
        Row(region="norte", monto=200.0),
    ])
    resultado = calcular_metricas(input_df)
    assert resultado.select("revenue").collect()[0]["revenue"] == 300.0

# Opción 3: snapshot testing (guardar el resultado esperado)
def test_snapshot(spark, tmp_path):
    resultado = calcular_metricas(df_ventas)
    resultado.toPandas().to_csv(tmp_path / "actual.csv", index=False)
    # La primera vez: guardar como snapshot
    # Después: comparar con el snapshot guardado
```

**Preguntas:**

1. ¿`assert_df_equality` compara tipos exactos o permite coerción?

2. ¿Comparar floats en DataFrames requiere tolerancia (epsilon)?

3. ¿Snapshot testing es útil para DataFrames grandes?

4. ¿`chispa` funciona con PySpark y con Spark Connect?

5. ¿Polars tiene un equivalente a `chispa` para aserciones?

**Pista:** Para comparar floats, `chispa` no tiene tolerancia built-in.
El patrón es: redondear las columnas float antes de comparar,
o usar `assert_approx_df_equality` si existe en tu versión.
Para Polars, puedes usar `pl.testing.assert_frame_equal()`
con `check_exact=False` y `atol`/`rtol` para tolerancia numérica.

---

### Ejercicio 19.2.4 — Testear data skew y particiones

```python
# Los bugs más difíciles en Spark aparecen con data skew.
# Un test que funciona con 10 filas puede fallar con 1 millón
# si una partición tiene el 90% de los datos.

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

def test_skew_no_afecta_resultado(spark):
    """Verificar que data skew no produce resultados incorrectos."""
    # Crear datos con skew extremo: 99% en una región
    data = (
        [Row(user_id=f"u{i}", monto=100.0, region="norte") for i in range(990)]
        + [Row(user_id=f"u{i}", monto=100.0, region="sur") for i in range(10)]
    )
    df = spark.createDataFrame(data)
    resultado = calcular_metricas(df)

    norte = resultado.filter(F.col("region") == "norte").collect()[0]
    sur = resultado.filter(F.col("region") == "sur").collect()[0]

    assert norte["transacciones"] == 990
    assert sur["transacciones"] == 10

def test_particiones_no_afectan_resultado(spark):
    """El resultado debe ser idéntico con 1 partición o con 100."""
    data = [Row(user_id=f"u{i}", monto=float(i), region="norte") for i in range(100)]
    df = spark.createDataFrame(data)

    resultado_1 = calcular_metricas(df.repartition(1))
    resultado_100 = calcular_metricas(df.repartition(100))

    # Mismo resultado independientemente del particionamiento:
    assert resultado_1.collect()[0]["revenue"] == resultado_100.collect()[0]["revenue"]

def test_null_handling_en_groupby(spark):
    """Nulls en la columna de group by: ¿se ignoran o crean un grupo?"""
    data = [
        Row(user_id="alice", monto=100.0, region="norte"),
        Row(user_id="bob", monto=200.0, region=None),
    ]
    df = spark.createDataFrame(data)
    resultado = calcular_metricas(df)

    # En Spark, groupBy incluye nulls como un grupo separado.
    # ¿Es el comportamiento deseado?
    regiones = [r["region"] for r in resultado.select("region").collect()]
    assert None not in regiones or "null region handled explicitly"
```

**Preguntas:**

1. ¿Los tests de skew deben ejecutarse en CI o son demasiado lentos?

2. ¿`repartition(1)` vs `repartition(100)` — ¿puede cambiar el resultado
   de un `groupBy`? ¿De un `join`?

3. ¿El manejo de nulls en `groupBy` es consistente entre Spark y Polars?

4. ¿Un test con 990 filas skewed es suficiente para detectar
   problemas que aparecen con 990 millones?

5. ¿`coalesce` vs `repartition` en tests — ¿importa?

---

### Ejercicio 19.2.5 — Implementar: test suite completa para una transformación

**Tipo: Implementar**

```python
# Implementar una test suite completa para el pipeline de métricas
# del sistema de e-commerce.

# La transformación:
# ventas_raw + clientes_dim → metricas_diarias
# (join, filtro, agregación, cálculo de revenue con IVA)

# Tests requeridos:
# 1. Caso feliz: datos normales → resultado correcto
# 2. Edge case: ventas vacías → resultado vacío (sin error)
# 3. Edge case: clientes sin ventas → no aparecen en resultado
# 4. Edge case: ventas sin match en clientes → manejo de LEFT JOIN
# 5. Data skew: 99% de ventas en una región → resultado correcto
# 6. Null handling: nulls en monto, region, user_id
# 7. Tipos: monto como string en vez de float → error claro
# 8. Duplicados: mismo user_id + timestamp duplicado
# 9. Parametrize: múltiples tasas de IVA
# 10. Property: sum(revenue_por_region) == revenue_total
```

**Restricciones:**
1. Implementar los 10 tests listados
2. Usar `@pytest.fixture` para datos reutilizables
3. Usar `chispa` o `assert_df_equality` para comparar DataFrames
4. Medir el tiempo de ejecución de la suite completa
5. Cada test debe ser independiente (no depender del orden de ejecución)

---

## Sección 19.3 — Contract Testing: el Acuerdo entre Productor y Consumidor

### Ejercicio 19.3.1 — Leer: por qué los schema changes rompen pipelines

**Tipo: Leer**

```
El escenario más común de fallo en producción:

  Equipo Backend:
    "Renombramos la columna 'precio' a 'price' en PostgreSQL.
     También añadimos 'discount_code' como STRING nullable."

  Equipo Data Engineering:
    "Nuestro pipeline hace SELECT precio FROM ventas.
     Después del deploy de backend: ColumnNotFoundException."

  ¿Quién tiene la culpa?
    → Nadie. No hay un contrato explícito entre productor y consumidor.

Contract testing = definir explícitamente qué schema y semántica
espera el consumidor, y verificar en CI/CD que el productor lo cumple.

Tipos de contratos:
  1. Schema contract: columnas, tipos, nullable
  2. Semantic contract: rangos de valores, unicidad, cardinalidad
  3. SLA contract: freshness, volumen mínimo, latencia

¿Dónde vive el contrato?
  - Schema Registry (Kafka): para eventos de streaming
  - dbt schema.yml: para modelos SQL
  - Great Expectations suite: para validación en pipeline
  - Protobuf / Avro schema files: para datos serializados
  - JSON Schema: para APIs REST
```

**Preguntas:**

1. ¿Quién define el contrato: el productor o el consumidor?

2. ¿Un schema contract en un Schema Registry previene
   cambios backward-incompatible?

3. ¿Los contratos deben estar en código (versionados en Git)?

4. ¿Un contrato que es demasiado estricto bloquea la evolución del schema?

5. ¿Contract testing existe en la práctica en data engineering
   o es aspiracional?

---

### Ejercicio 19.3.2 — Schema Registry: contratos para Kafka

```python
# Confluent Schema Registry: almacena y valida schemas de Avro/Protobuf/JSON Schema.
# Previene que un productor envíe datos incompatibles.

# Schema Avro para el evento de venta:
schema_venta_v1 = {
    "type": "record",
    "name": "Venta",
    "namespace": "com.ecommerce.events",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "monto", "type": "double"},
        {"name": "region", "type": "string"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

# Evolución del schema (v2: añadir campo opcional):
schema_venta_v2 = {
    "type": "record",
    "name": "Venta",
    "namespace": "com.ecommerce.events",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "monto", "type": "double"},
        {"name": "region", "type": "string"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "discount_code", "type": ["null", "string"], "default": None},
        # ↑ campo nuevo, nullable con default null → backward compatible
    ]
}

# Compatibilidad:
# BACKWARD: consumer nuevo puede leer datos del producer viejo ✓
# FORWARD: consumer viejo puede leer datos del producer nuevo ✓
# FULL: ambos ✓
# NONE: no hay validación (peligroso)

# Test: verificar que el schema nuevo es compatible:
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def test_schema_backward_compatible():
    """Verificar que v2 es backward compatible con v1."""
    client = SchemaRegistryClient({"url": "http://schema-registry:8081"})
    # Schema Registry rechaza el registro si no es compatible:
    try:
        client.register_schema("ventas-value", schema_venta_v2)
    except Exception as e:
        pytest.fail(f"Schema v2 no es compatible con v1: {e}")
```

**Preguntas:**

1. ¿BACKWARD vs FORWARD vs FULL compatibility — cuál para producción?

2. ¿Renombrar una columna es backward compatible?
   ¿Cómo manejas un rename?

3. ¿Eliminar un campo es backward compatible?

4. ¿Schema Registry es necesario si ya tienes Protobuf con versionado?

5. ¿Schema Registry funciona para batch (Parquet files)
   o solo para streaming (Kafka)?

---

### Ejercicio 19.3.3 — Pandera: contract testing para DataFrames

```python
import pandera as pa
import polars as pl

# Pandera: validación de schemas para DataFrames (Pandas y Polars).
# Define el contrato como un schema declarativo.

# Schema contract para la tabla ventas_raw:
schema_ventas = pa.DataFrameSchema({
    "user_id": pa.Column(str, nullable=False, unique=False,
                         checks=pa.Check.str_length(min_value=1, max_value=100)),
    "monto": pa.Column(float, nullable=False,
                       checks=[
                           pa.Check.greater_than(0),
                           pa.Check.less_than(1_000_000),
                       ]),
    "region": pa.Column(str, nullable=False,
                        checks=pa.Check.isin(["norte", "sur", "este", "oeste", "centro"])),
    "fecha": pa.Column("datetime64[ns]", nullable=False),
}, coerce=True)

# Schema contract para la tabla metricas_diarias (output):
schema_metricas = pa.DataFrameSchema({
    "region": pa.Column(str, nullable=False),
    "revenue": pa.Column(float, nullable=False,
                         checks=pa.Check.greater_than_or_equal_to(0)),
    "usuarios_unicos": pa.Column(int, nullable=False,
                                 checks=pa.Check.greater_than(0)),
    "transacciones": pa.Column(int, nullable=False,
                               checks=pa.Check.greater_than_or_equal_to(1)),
}, checks=pa.Check(
    lambda df: df["transacciones"].ge(df["usuarios_unicos"]).all(),
    error="transacciones debe ser >= usuarios_unicos",
))


# Usar en tests:
def test_output_cumple_contrato(df_ventas):
    resultado = calcular_metricas(df_ventas)
    schema_metricas.validate(resultado.to_pandas())  # valida contra el contrato

# Usar en el pipeline (producción):
def pipeline_con_contrato(df_raw):
    # Validar input:
    schema_ventas.validate(df_raw.to_pandas())
    # Transformar:
    resultado = calcular_metricas(df_raw)
    # Validar output:
    schema_metricas.validate(resultado.to_pandas())
    return resultado
```

**Preguntas:**

1. ¿Pandera valida en runtime o en tiempo de definición?

2. ¿Pandera con Polars — ¿es estable o es mejor usar Pandas?

3. ¿Validar input Y output duplica la validación?
   ¿Siempre se deben validar ambos?

4. ¿`coerce=True` es una buena práctica?
   ¿Puede ocultar errores de tipos?

5. ¿Pandera reemplaza a Great Expectations o son complementarios?

---

### Ejercicio 19.3.4 — dbt contracts: schema enforcement en SQL

```yaml
# dbt v1.5+ introduce model contracts: enforcement de schema en el modelo.

# models/analytics/metricas_diarias.yml
version: 2
models:
  - name: metricas_diarias
    config:
      contract:
        enforced: true    # dbt FALLA si el output no cumple el contrato
    columns:
      - name: fecha
        data_type: date
        constraints:
          - type: not_null
      - name: region
        data_type: varchar
        constraints:
          - type: not_null
          - type: accepted_values
            values: ['norte', 'sur', 'este', 'oeste', 'centro']
      - name: revenue
        data_type: float64
        constraints:
          - type: not_null
          - type: check
            expression: "revenue >= 0"
      - name: usuarios_unicos
        data_type: int64
        constraints:
          - type: not_null
          - type: check
            expression: "usuarios_unicos > 0"
      - name: transacciones
        data_type: int64
        constraints:
          - type: not_null

# Con contract.enforced = true:
# - dbt verifica que el modelo produce exactamente estas columnas
# - dbt verifica que los tipos coinciden
# - Si añades una columna al SELECT que no está en el contrato → error
# - Si cambias el tipo de una columna → error
# - El contrato protege a los consumidores downstream
```

**Preguntas:**

1. ¿dbt contracts son schema contracts o incluyen semantic contracts?

2. ¿Si el contrato falla, dbt no materializa el modelo?
   ¿O materializa y reporta el error?

3. ¿Los constraints de dbt se ejecutan en la base de datos?

4. ¿Puedes testear los contratos localmente antes de hacer deploy?

5. ¿dbt contracts + Pandera es redundante?

---

### Ejercicio 19.3.5 — Implementar: sistema de contratos para el e-commerce

**Tipo: Implementar**

```python
# Diseñar un sistema de contratos para el pipeline de e-commerce:
#
# 1. Contrato de la fuente (PostgreSQL → extract):
#    Schema esperado de la tabla orders en PostgreSQL
#    Freshness: datos de ayer deben existir
#    Volumen: entre 5000 y 500000 filas por día
#
# 2. Contrato del raw layer (extract → transform):
#    Schema de ventas_raw (Parquet en S3)
#    Tipos exactos, no nulls en PK
#
# 3. Contrato de Kafka (eventos de click):
#    Avro schema en Schema Registry
#    Backward compatible
#
# 4. Contrato del analytics layer (transform → dashboard):
#    Schema de metricas_diarias
#    Revenue >= 0, usuarios > 0, transacciones >= usuarios
#
# Implementar:
# a) Los contratos como código (Pandera + Avro + dbt)
# b) Tests que verifican los contratos en CI
# c) Validación runtime en el pipeline
# d) Alerta cuando un contrato se rompe
```

**Restricciones:**
1. Al menos 4 contratos cubriendo el pipeline end-to-end
2. Cada contrato testeable en CI sin necesidad de sistemas externos
3. Validación runtime que bloquea el pipeline si el contrato falla
4. Documentación generada automáticamente desde los contratos

---

## Sección 19.4 — Testing de Streaming: Kafka, Flink, y Beam

### Ejercicio 19.4.1 — Leer: por qué testear streaming es más difícil

**Tipo: Leer**

```
Batch testing:                     Streaming testing:
  Input: un archivo/tabla            Input: un flujo infinito de eventos
  Output: un archivo/tabla           Output: resultados que cambian con el tiempo
  Determinístico: mismo input        No-determinístico: el ORDEN de los eventos
    → mismo output                     puede cambiar el resultado
  Sin tiempo: los datos ya existen   El tiempo ES una variable: watermarks, windows
  Una ejecución: inicio → fin        Continuo: el job no termina

Desafíos específicos de testing de streaming:

  1. Event time vs processing time:
     Un evento con timestamp 10:00 que llega a las 10:05
     ¿entra en la window de 10:00-10:10?
     → Depende del watermark y de la allowed lateness.

  2. Windowing:
     ¿La ventana se cierra exactamente cuando esperas?
     ¿Qué pasa con eventos en el boundary (exactamente a las 10:10)?

  3. Out-of-order events:
     Un evento de las 09:55 llega después de un evento de las 10:05.
     ¿Tu pipeline maneja esto correctamente?

  4. State:
     Un operador con estado mantiene contadores.
     Si se reinicia, ¿el estado se recupera correctamente?

  5. Exactly-once:
     Si el test falla a mitad del processing,
     ¿los datos se duplican al re-ejecutar?
```

**Preguntas:**

1. ¿Se puede testear streaming sin ejecutar Kafka/Flink reales?

2. ¿Los test harnesses de Flink y Beam son suficientes
   o necesitas un cluster de staging?

3. ¿Testear watermarks requiere manipular el tiempo en el test?

4. ¿Los tests de streaming son inherentemente más lentos
   que los de batch?

5. ¿Testear exactly-once es posible en un test unitario?

---

### Ejercicio 19.4.2 — TopologyTestDriver: testear Kafka Streams sin Kafka

```java
// Kafka Streams incluye TopologyTestDriver:
// un driver de test que ejecuta la topología IN-MEMORY, sin broker.

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class VentasStreamTest {

    @Test
    void testContarVentasPorRegion() {
        // Configurar la topología:
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Venta> ventas = builder.stream("ventas");
        KTable<String, Long> conteo = ventas
            .groupBy((key, venta) -> venta.getRegion())
            .count();
        conteo.toStream().to("conteo-por-region");

        Topology topology = builder.build();

        // Crear el test driver (sin Kafka broker):
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, Venta> input = driver.createInputTopic(
                "ventas", new StringSerializer(), new VentaSerializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                "conteo-por-region", new StringDeserializer(), new LongDeserializer());

            // Enviar eventos:
            input.pipeInput("k1", new Venta("alice", 100.0, "norte"));
            input.pipeInput("k2", new Venta("bob", 200.0, "norte"));
            input.pipeInput("k3", new Venta("carol", 150.0, "sur"));

            // Verificar output:
            Map<String, Long> resultados = output.readKeyValuesToMap();
            assertEquals(2L, resultados.get("norte"));
            assertEquals(1L, resultados.get("sur"));
        }
    }
}
```

**Preguntas:**

1. ¿TopologyTestDriver ejecuta la topología completa
   incluyendo serialización/deserialización?

2. ¿Puedes testear windowed aggregations con TopologyTestDriver?
   ¿Cómo avanzas el tiempo?

3. ¿TopologyTestDriver soporta state stores (RocksDB)?

4. ¿Un test con TopologyTestDriver es un unit test o un component test?

5. ¿Existe un equivalente de TopologyTestDriver para Flink?

---

### Ejercicio 19.4.3 — Beam TestPipeline: testear pipelines de Beam

```python
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.trigger import AfterWatermark
import pytest

class TestVentasPipeline:

    def test_contar_por_region(self):
        """Test básico: contar ventas por región."""
        with TestPipeline() as p:
            input_data = [
                {"user_id": "alice", "monto": 100.0, "region": "norte"},
                {"user_id": "bob", "monto": 200.0, "region": "norte"},
                {"user_id": "carol", "monto": 150.0, "region": "sur"},
            ]

            resultado = (
                p
                | beam.Create(input_data)
                | beam.Map(lambda x: (x["region"], x["monto"]))
                | beam.CombinePerKey(sum)
            )

            assert_that(resultado, equal_to([("norte", 300.0), ("sur", 150.0)]))

    def test_windowing_fixed(self):
        """Test con ventanas fijas de 10 segundos."""
        with TestPipeline() as p:
            # Eventos con timestamps explícitos:
            input_data = [
                TimestampedValue({"region": "norte", "monto": 100.0}, 1),   # t=1s
                TimestampedValue({"region": "norte", "monto": 200.0}, 5),   # t=5s
                TimestampedValue({"region": "norte", "monto": 300.0}, 15),  # t=15s (nueva ventana)
            ]

            resultado = (
                p
                | beam.Create(input_data)
                | beam.WindowInto(FixedWindows(10))
                | beam.Map(lambda x: (x["region"], x["monto"]))
                | beam.CombinePerKey(sum)
            )

            # Ventana [0,10): 100 + 200 = 300
            # Ventana [10,20): 300
            assert_that(resultado, equal_to([
                ("norte", 300.0),  # window [0, 10)
                ("norte", 300.0),  # window [10, 20)
            ]))

    def test_late_data_dropped(self):
        """Eventos que llegan después de que la ventana cierra."""
        with TestPipeline() as p:
            input_data = [
                TimestampedValue({"region": "norte", "monto": 100.0}, 1),
                TimestampedValue({"region": "norte", "monto": 200.0}, 25),
                # El watermark avanzará más allá de la ventana [0,10)
                # Un evento "tardío" para la ventana [0,10):
                TimestampedValue({"region": "norte", "monto": 50.0}, 3),
            ]

            resultado = (
                p
                | beam.Create(input_data)
                | beam.WindowInto(FixedWindows(10))
                | beam.Map(lambda x: (x["region"], x["monto"]))
                | beam.CombinePerKey(sum)
            )
            # El comportamiento depende del watermark y allowed_lateness
```

**Preguntas:**

1. ¿`TestPipeline` ejecuta con el DirectRunner?
   ¿El comportamiento es idéntico al DataflowRunner o FlinkRunner?

2. ¿Los tests de windowing con `TimestampedValue` son realistas?
   ¿El watermark avanza correctamente?

3. ¿Cómo testeas triggers custom (early, on-time, late)?

4. ¿`assert_that` con `equal_to` compara sin orden?

5. ¿Puedes testear un pipeline de Beam end-to-end
   (lectura de Kafka, escritura a BigQuery) en un test?

---

### Ejercicio 19.4.4 — Flink: test harness para operadores con estado

```java
// Flink proporciona test harnesses para testear operadores individualmente.

import org.apache.flink.streaming.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DetectorFraudeTest {

    @Test
    void testDetectaTransaccionSospechosa() throws Exception {
        // KeyedOneInputStreamOperatorTestHarness:
        // testea un operador con estado sin levantar un cluster Flink.

        DetectorFraudeFunction function = new DetectorFraudeFunction(
            umbralMonto: 10_000.0,
            ventanaMinutos: 5
        );

        KeyedOneInputStreamOperatorTestHarness<String, Transaccion, Alerta> harness =
            ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                t -> t.getUserId(),
                Types.STRING
            );

        // Avanzar el tiempo y enviar eventos:
        harness.open();

        // Transacción normal:
        harness.processElement(
            new Transaccion("alice", 100.0, "norte"), 1000L);  // t=1s
        assertTrue(harness.extractOutputValues().isEmpty());

        // Transacción sospechosa (> umbral):
        harness.processElement(
            new Transaccion("alice", 15_000.0, "norte"), 2000L);  // t=2s
        assertEquals(1, harness.extractOutputValues().size());

        Alerta alerta = harness.extractOutputValues().get(0);
        assertEquals("alice", alerta.getUserId());
        assertEquals(15_000.0, alerta.getMonto());

        harness.close();
    }

    @Test
    void testEstadoSeSalvaEnCheckpoint() throws Exception {
        // Verificar que el estado sobrevive a un checkpoint + restore.
        // ...
        // harness.snapshot(checkpointId, timestamp);
        // harness.close();
        // newHarness = createHarness();
        // newHarness.initializeState(snapshot);
        // Verificar que el estado es consistente.
    }
}
```

**Preguntas:**

1. ¿El test harness de Flink ejecuta el operador completo
   incluyendo state backends?

2. ¿Puedes testear checkpoints y recovery con el harness?

3. ¿El harness maneja event time y watermarks?

4. ¿Cómo testeas un operador que lee de Kafka dentro de Flink?

5. ¿Los test harnesses de Flink funcionan con Python (PyFlink)?

---

### Ejercicio 19.4.5 — Implementar: test suite para streaming de e-commerce

**Tipo: Implementar**

```python
# Implementar tests para el pipeline de streaming del e-commerce:
#
# Pipeline: Kafka (clicks + compras) → Flink/Beam → métricas real-time
#
# Tests requeridos:
# 1. Contar clicks por página en ventana de 1 minuto
# 2. Calcular revenue por región en ventana de 5 minutos
# 3. Late events: click que llega 2 minutos tarde
# 4. Out-of-order: eventos que llegan desordenados
# 5. Session window: agrupar actividad de un usuario por sesión
# 6. Schema evolution: campo nuevo en el evento de Kafka
# 7. Deduplicación: mismo evento enviado dos veces (at-least-once)
# 8. State recovery: verificar que el estado sobrevive un restart

# Elegir framework (Beam TestPipeline o Kafka TopologyTestDriver)
# y justificar la elección.
```

**Restricciones:**
1. Implementar al menos 6 de los 8 tests
2. Cada test debe ser determinístico y reproducible
3. Los tests no deben requerir Kafka ni Flink corriendo
4. Medir tiempo total de la suite (target: < 30 segundos)

---

## Sección 19.5 — Integration Testing: Contra Sistemas Reales

### Ejercicio 19.5.1 — Leer: cuándo los mocks no son suficientes

**Tipo: Leer**

```
Unit/component tests con datos sintéticos cubren la lógica.
Pero hay bugs que solo aparecen con sistemas reales:

  1. Credenciales y permisos:
     El service account no tiene permiso para escribir en BigQuery.
     → No lo descubres hasta que ejecutas contra BigQuery real.

  2. Formatos de serialización:
     Parquet escrito por Spark 3.4 tiene un campo decimal
     que BigQuery interpreta como STRING.
     → No lo descubres con datos en memoria.

  3. Límites del sistema:
     BigQuery tiene un límite de 1500 columnas por tabla.
     Tu pipeline genera 1501 columnas.
     → No lo descubres sin BigQuery real.

  4. Network y latencia:
     La query a PostgreSQL tarda 30 segundos en staging
     pero 5 minutos en producción (tabla más grande).
     → No lo descubres sin un entorno realista.

  5. Concurrencia:
     Dos pipelines escriben a la misma tabla de Delta Lake.
     Con optimistic concurrency, uno de los dos falla.
     → No lo descubres sin ejecución concurrente real.

Cuándo testear con sistemas reales:
  - Antes de deployar un pipeline NUEVO
  - Después de un cambio en la INFRAESTRUCTURA (migrar a nuevo cluster)
  - Después de un cambio en CREDENCIALES o PERMISOS
  - Periódicamente (nightly) para detectar drift

Cuándo NO:
  - En cada commit (demasiado lento, demasiado costoso)
  - Para verificar lógica de transformación (usa component tests)
```

**Preguntas:**

1. ¿Testcontainers puede reemplazar sistemas reales para integration tests?

2. ¿Un entorno de staging que replica producción es costoso?
   ¿Cuánto cuesta mantenerlo?

3. ¿Los integration tests deben correr en CI o solo antes de deploy?

4. ¿Testear contra BigQuery real genera costos de query?

5. ¿Mocks vs fakes vs reales — cuándo cada uno?

---

### Ejercicio 19.5.2 — Testcontainers: servicios reales en containers desechables

```python
import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer
import polars as pl

@pytest.fixture(scope="session")
def postgres():
    """PostgreSQL real en un container desechable."""
    with PostgresContainer("postgres:16") as pg:
        # Crear tablas de test:
        conn = pg.get_connection_url()
        engine = create_engine(conn)
        engine.execute("""
            CREATE TABLE ventas (
                user_id VARCHAR(100),
                monto DECIMAL(10,2),
                region VARCHAR(50),
                fecha DATE
            )
        """)
        # Insertar datos de test:
        engine.execute("""
            INSERT INTO ventas VALUES
            ('alice', 100.00, 'norte', '2024-03-14'),
            ('bob', 200.00, 'sur', '2024-03-14'),
            ('carol', 150.00, 'norte', '2024-03-14')
        """)
        yield pg

@pytest.fixture(scope="session")
def kafka():
    """Kafka real en un container desechable."""
    with KafkaContainer("confluentinc/cp-kafka:7.5.0") as k:
        yield k

def test_extraer_de_postgres_real(postgres):
    """Verificar que el extract funciona contra PostgreSQL real."""
    conn = postgres.get_connection_url()
    df = pl.read_database("SELECT * FROM ventas WHERE fecha = '2024-03-14'",
                          connection=conn)
    assert len(df) == 3
    assert df["monto"].sum() == 450.0

def test_producir_y_consumir_kafka(kafka):
    """Verificar que el pipeline lee y escribe correctamente en Kafka."""
    bootstrap = kafka.get_bootstrap_server()
    # Producir evento:
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    producer.send("ventas", b'{"user_id": "alice", "monto": 100.0}')
    producer.flush()

    # Consumir evento:
    consumer = KafkaConsumer("ventas", bootstrap_servers=bootstrap,
                             auto_offset_reset="earliest",
                             consumer_timeout_ms=5000)
    mensajes = list(consumer)
    assert len(mensajes) == 1
```

**Preguntas:**

1. ¿Testcontainers es lento? ¿Cuánto tarda en levantar PostgreSQL?

2. ¿`scope="session"` reutiliza el container para todos los tests?

3. ¿Puedes usar Testcontainers para Spark (levantar un mini-cluster)?

4. ¿Testcontainers en CI (GitHub Actions) requiere Docker-in-Docker?

5. ¿Testcontainers reemplaza a un entorno de staging?

---

### Ejercicio 19.5.3 — Test del pipeline completo en staging

```python
# Integration test end-to-end: ejecutar el pipeline completo
# contra un entorno de staging con datos reales (anonimizados).

import pytest
from datetime import date

@pytest.fixture(scope="module")
def staging_config():
    """Configuración del entorno de staging."""
    return {
        "postgres_conn": "postgresql://staging:password@staging-db:5432/ecommerce",
        "s3_bucket": "s3://staging-data-lake",
        "bigquery_project": "ecommerce-staging",
        "bigquery_dataset": "analytics_test",
    }

class TestPipelineE2E:

    def test_pipeline_completo(self, staging_config):
        """Ejecutar el pipeline completo y verificar resultados."""
        fecha = date(2024, 3, 14)

        # 1. Ejecutar extract:
        extraer_ventas(staging_config["postgres_conn"],
                       staging_config["s3_bucket"], fecha)

        # 2. Verificar que los datos aterrizaron en S3:
        df_raw = pl.read_parquet(
            f"{staging_config['s3_bucket']}/raw/ventas/fecha={fecha}/")
        assert len(df_raw) > 0

        # 3. Ejecutar transform:
        transformar_metricas(staging_config["s3_bucket"],
                             staging_config["bigquery_project"], fecha)

        # 4. Verificar resultado en BigQuery:
        resultado = bq_client.query(f"""
            SELECT * FROM `{staging_config['bigquery_project']}
                          .{staging_config['bigquery_dataset']}
                          .metricas_diarias`
            WHERE fecha = '{fecha}'
        """).result().to_dataframe()

        assert len(resultado) > 0
        assert resultado["revenue"].sum() > 0
        assert resultado["revenue"].min() >= 0

    def test_idempotencia(self, staging_config):
        """Ejecutar dos veces y verificar que el resultado es idéntico."""
        fecha = date(2024, 3, 14)

        pipeline_completo(staging_config, fecha)
        resultado_1 = leer_metricas(staging_config, fecha)

        pipeline_completo(staging_config, fecha)
        resultado_2 = leer_metricas(staging_config, fecha)

        assert resultado_1.equals(resultado_2)

    def test_backfill_30_dias(self, staging_config):
        """Backfill de 30 días no duplica datos."""
        for i in range(30):
            fecha = date(2024, 3, 1) + timedelta(days=i)
            pipeline_completo(staging_config, fecha)

        total = bq_client.query(f"""
            SELECT COUNT(*) as n, COUNT(DISTINCT fecha) as dias
            FROM `{staging_config['bigquery_project']}
                  .{staging_config['bigquery_dataset']}
                  .metricas_diarias`
            WHERE fecha BETWEEN '2024-03-01' AND '2024-03-30'
        """).result().to_dataframe()

        assert total["dias"].iloc[0] == 30
```

**Preguntas:**

1. ¿Los E2E tests deben correr en cada PR o solo antes de deploy?

2. ¿Datos de staging deben ser una copia de producción (anonimizada)
   o datos sintéticos?

3. ¿Cómo limpias el staging después de correr tests?

4. ¿El test de idempotencia es el test más importante del pipeline?

5. ¿30 días de backfill en un test es realista?
   ¿Cuánto tarda?

---

### Ejercicio 19.5.4 — Medir: cost of testing vs cost of bugs

**Tipo: Analizar**

```
Cálculo del costo de testing vs. no testing:

  Costo de testing:
    - Escribir tests:           4 horas por pipeline
    - Mantener tests:           1 hora/mes por pipeline
    - Infraestructura CI:       $200/mes (runners, containers)
    - Staging environment:      $500/mes
    - Total año 1 (20 pipelines): ~$15,000

  Costo de NO testing:
    - Incidente por bug en prod:   8 horas × $150/hora = $1,200 por incidente
    - Frecuencia sin tests:        2 incidentes/mes
    - Total año:                   $28,800
    - + Datos incorrectos en dashboard: decisiones erróneas → $???
    - + Confianza del equipo en los datos: baja → más verificación manual

  ROI del testing: ($28,800 - $15,000) / $15,000 = 92%
  (sin contar el valor de datos confiables y decisiones correctas)
```

**Preguntas:**

1. ¿El cálculo de ROI convence a un engineering manager?

2. ¿4 horas por pipeline para escribir tests es realista?

3. ¿Qué porcentaje del tiempo de desarrollo debe ir a testing?

4. ¿Test coverage (%) es una métrica útil para pipelines de datos?

5. ¿Cuál es el mínimo de tests que todo pipeline debe tener?

---

### Ejercicio 19.5.5 — Implementar: integration test con Testcontainers

**Tipo: Implementar**

```python
# Implementar integration tests para el pipeline de e-commerce usando
# Testcontainers para PostgreSQL y Kafka.

# 1. Levantar PostgreSQL con datos de test → ejecutar extract → verificar output
# 2. Levantar Kafka → producir eventos → consumir y transformar → verificar
# 3. Pipeline completo: PostgreSQL → extract → transform → verificar metricas
# 4. Test de schema evolution: añadir columna a PostgreSQL → verificar que
#    el pipeline maneja el cambio gracefully
# 5. Test de volumen: insertar 100K filas → verificar performance aceptable
```

**Restricciones:**
1. Todos los tests deben usar Testcontainers (no mocks)
2. Cada test debe ser independiente y limpiarse después
3. Los tests deben correr en < 2 minutos totales
4. Incluir un `conftest.py` con fixtures reutilizables

---

## Sección 19.6 — Testing de DAGs y Orquestación

### Ejercicio 19.6.1 — Leer: qué testear en un DAG de Airflow

**Tipo: Leer**

```
Un DAG de Airflow tiene tres niveles de testing:

  Nivel 1: DAG validation (¿el DAG es válido?)
  ────────────────────────────────────────────
  - ¿Se parsea sin errores?
  - ¿No tiene ciclos?
  - ¿Los task_ids son únicos?
  - ¿El schedule es válido?
  → Rápido, ejecutar en cada commit.

  Nivel 2: Task logic (¿las tareas hacen lo correcto?)
  ────────────────────────────────────────────
  - ¿La función Python de cada PythonOperator es correcta?
  - ¿Los templates Jinja se resuelven correctamente?
  - ¿Los XComs pasan los datos esperados?
  → Component tests con datos sintéticos.

  Nivel 3: DAG behavior (¿el DAG se comporta correctamente?)
  ────────────────────────────────────────────
  - ¿Las dependencias son correctas? (A antes de B)
  - ¿El branching funciona? (si X → rama A, si no → rama B)
  - ¿Los reintentos se activan?
  - ¿Los SLAs se verifican?
  → Más difícil, requiere ejecutar el DAG o simular el scheduler.
```

**Preguntas:**

1. ¿La mayoría de los equipos solo hacen Nivel 1 (DAG validation)?

2. ¿Testear templates Jinja de Airflow es posible?

3. ¿Airflow tiene un framework de testing integrado?

4. ¿Los tests de DAG deben ejecutarse con las dependencias
   de Airflow instaladas?

5. ¿Dagster y Prefect son más fáciles de testear que Airflow?

---

### Ejercicio 19.6.2 — DAG validation: el test mínimo indispensable

```python
# Test mínimo: verificar que todos los DAGs se parsean sin errores.
# Este test previene el error #1 en Airflow:
# "un import error en un DAG rompe el parsing de TODOS los DAGs"

import pytest
from airflow.models import DagBag
from pathlib import Path

@pytest.fixture(scope="session")
def dagbag():
    """Cargar todos los DAGs del directorio."""
    dag_folder = str(Path(__file__).parent.parent / "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)

class TestDAGValidation:

    def test_no_import_errors(self, dagbag):
        """Ningún DAG tiene errores de import."""
        assert len(dagbag.import_errors) == 0, \
            f"Errores de import: {dagbag.import_errors}"

    def test_dag_ids_unicos(self, dagbag):
        """No hay DAG IDs duplicados."""
        dag_ids = [dag.dag_id for dag in dagbag.dags.values()]
        assert len(dag_ids) == len(set(dag_ids))

    def test_no_ciclos(self, dagbag):
        """Ningún DAG tiene ciclos en las dependencias."""
        for dag_id, dag in dagbag.dags.items():
            assert not dag.test_cycle(), f"Ciclo detectado en {dag_id}"

    def test_tiene_owner(self, dagbag):
        """Cada DAG tiene un owner definido."""
        for dag_id, dag in dagbag.dags.items():
            assert dag.owner != "airflow", \
                f"DAG {dag_id} no tiene owner (usa default 'airflow')"

    def test_tiene_tags(self, dagbag):
        """Cada DAG tiene al menos un tag para organización."""
        for dag_id, dag in dagbag.dags.items():
            assert len(dag.tags) > 0, f"DAG {dag_id} no tiene tags"

    def test_retries_configurados(self, dagbag):
        """Cada DAG tiene reintentos configurados."""
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                assert task.retries >= 1, \
                    f"Task {dag_id}.{task.task_id} no tiene retries"

    @pytest.mark.parametrize("dag_id,expected_tasks", [
        ("ecommerce_daily_metrics", ["extraer", "validar", "transformar", "cargar"]),
        ("fraud_detection", ["ingestar", "detectar", "alertar"]),
    ])
    def test_tareas_esperadas(self, dagbag, dag_id, expected_tasks):
        """Verificar que los DAGs críticos tienen las tareas esperadas."""
        dag = dagbag.dags.get(dag_id)
        assert dag is not None, f"DAG {dag_id} no encontrado"
        task_ids = [t.task_id for t in dag.tasks]
        for expected in expected_tasks:
            assert expected in task_ids, \
                f"Task '{expected}' no encontrada en {dag_id}"
```

**Preguntas:**

1. ¿Este test debe correr en cada PR o solo en merges a main?

2. ¿`test_retries_configurados` es demasiado opinado?
   ¿Hay tareas que NO deberían tener retries?

3. ¿`test_tiene_owner` previene el "DAG huérfano"?

4. ¿DagBag necesita conexiones reales (PostgreSQL, GCS)
   para parsear los DAGs?

5. ¿Cuánto tarda el parsing de 50 DAGs en CI?

---

### Ejercicio 19.6.3 — Testing de Dagster assets

```python
import dagster as dg
from dagster import build_asset_context
import polars as pl

# Dagster tiene soporte de testing de primera clase.
# Puedes ejecutar assets individuales sin levantar el servidor.

def test_ventas_raw():
    """Testear un asset individual con contexto simulado."""
    context = build_asset_context(partition_key="2024-03-14")
    resultado = ventas_raw(context)
    assert isinstance(resultado, pl.DataFrame)
    assert len(resultado) > 0
    assert "user_id" in resultado.columns

def test_metricas_diarias():
    """Testear un asset con sus dependencias inyectadas."""
    # Crear datos de input sintéticos:
    ventas = pl.DataFrame({
        "user_id": ["alice", "bob"],
        "monto": [100.0, 200.0],
        "region": ["norte", "sur"],
    })
    clientes = pl.DataFrame({
        "user_id": ["alice", "bob"],
        "segmento": ["premium", "standard"],
    })

    context = build_asset_context(partition_key="2024-03-14")
    resultado = metricas_diarias(context, ventas_raw=ventas, clientes_dim=clientes)

    assert len(resultado) == 2
    assert resultado["revenue"].sum() > 0

def test_materializacion_completa():
    """Testear la materialización de múltiples assets."""
    result = dg.materialize(
        assets=[ventas_raw, clientes_dim, metricas_diarias],
        partition_key="2024-03-14",
        resources={"io_manager": dg.mem_io_manager},  # IO Manager in-memory
    )
    assert result.success
    # Verificar que todos los assets se materializaron:
    assert result.output_for_node("metricas_diarias") is not None
```

**Preguntas:**

1. ¿`build_asset_context` simula el contexto completo
   incluyendo IO Managers?

2. ¿`mem_io_manager` es suficiente para tests o necesitas
   testear con el IO Manager real?

3. ¿Dagster puede ejecutar tests sin el dagster-webserver corriendo?

4. ¿Testear assets individualmente vs testear el grafo completo
   — cuándo cada uno?

5. ¿Dagster facilita el testing respecto a Airflow?
   ¿En qué medida?

---

### Ejercicio 19.6.4 — Testing de Prefect flows

```python
from prefect import flow, task
from prefect.testing.utilities import prefect_test_harness
import pytest

@task
def extraer(fecha: str) -> dict:
    return {"filas": 100, "fecha": fecha}

@task
def transformar(data: dict) -> dict:
    return {"resultado": data["filas"] * 2}

@flow
def mi_pipeline(fecha: str):
    data = extraer(fecha)
    return transformar(data)

# Prefect 3.x: los flows son funciones Python normales.
# Puedes ejecutarlos directamente en tests:

def test_flow_directo():
    """Ejecutar el flow como una función Python normal."""
    resultado = mi_pipeline("2024-03-14")
    assert resultado["resultado"] == 200

# Con el test harness (para features como retries, caching):
@pytest.fixture(autouse=True)
def prefect_test():
    with prefect_test_harness():
        yield

def test_flow_con_harness():
    """Ejecutar con el harness para probar retries y caching."""
    resultado = mi_pipeline("2024-03-14")
    assert resultado["resultado"] == 200

def test_task_individual():
    """Testear una task aislada — es solo una función."""
    data = extraer.fn("2024-03-14")  # .fn() ejecuta sin el wrapper de Prefect
    assert data == {"filas": 100, "fecha": "2024-03-14"}
```

**Preguntas:**

1. ¿`.fn()` bypasea completamente Prefect?
   ¿Los decoradores afectan el comportamiento?

2. ¿`prefect_test_harness` es necesario para tests simples?

3. ¿Prefect es más fácil de testear que Airflow
   precisamente porque los flows son funciones normales?

4. ¿Cómo testeas retries en Prefect? ¿Puedes forzar un fallo?

5. ¿Los tests de Prefect necesitan un server corriendo?

---

### Ejercicio 19.6.5 — Implementar: test suite para los DAGs del e-commerce

**Tipo: Implementar**

```python
# Implementar tests para los DAGs/flows/assets del sistema de e-commerce.
#
# Nivel 1: Validation
#   - Todos los DAGs parsean sin error
#   - Cada DAG tiene owner, tags, retries
#   - No hay ciclos ni task_ids duplicados
#
# Nivel 2: Logic
#   - La función de extract retorna datos del formato esperado
#   - La función de transform produce métricas correctas
#   - El quality gate rechaza datos inválidos
#
# Nivel 3: Behavior
#   - El DAG de backfill es idempotente
#   - El branching condicional funciona
#   - Los SLAs están configurados correctamente

# Elegir Airflow, Dagster, o Prefect y justificar.
```

**Restricciones:**
1. Al menos 15 tests cubriendo los tres niveles
2. Tests ejecutables en CI sin Airflow/Dagster server corriendo
3. Tiempo total de la suite < 30 segundos
4. Incluir un `Makefile` o `pyproject.toml` con el comando de tests

---

## Sección 19.7 — El E-commerce Testeado: CI/CD para Pipelines de Datos

### Ejercicio 19.7.1 — Leer: CI/CD para data engineering vs software engineering

**Tipo: Leer**

```
CI/CD para microservicios:              CI/CD para pipelines de datos:
  git push → lint → unit tests            git push → lint → unit tests
  → build docker → integration tests      → DAG validation → component tests
  → deploy to staging → E2E tests         → schema contract check
  → deploy to production                  → deploy DAGs → staging E2E
                                          → canary run → production

Diferencias clave:

  1. El "deploy" es diferente:
     Software: desplegar un nuevo container/pod.
     Datos: desplegar un nuevo DAG + potencialmente backfill.

  2. El "rollback" es diferente:
     Software: volver al container anterior.
     Datos: volver al DAG anterior + ¿qué pasa con los datos
            que ya se procesaron con el DAG nuevo?

  3. Los tests son diferentes:
     Software: mocks fáciles (HTTP stubs).
     Datos: mocks difíciles (¿cómo mockeas BigQuery completo?).

  4. El entorno de staging es diferente:
     Software: staging = copia del servicio con datos de test.
     Datos: staging = copia del pipeline + datos reales anonimizados
            + infraestructura de data lake + warehouse.

  5. El "canary" es diferente:
     Software: 5% del tráfico al nuevo servicio.
     Datos: ejecutar el pipeline nuevo para UNA fecha
            y comparar resultado con el pipeline viejo.
```

**Preguntas:**

1. ¿El canary para pipelines de datos es viable en la práctica?

2. ¿Blue-green deployment aplica a DAGs?

3. ¿GitOps (ArgoCD, Flux) funciona para desplegar DAGs?

4. ¿Cómo haces rollback de un pipeline que ya escribió datos incorrectos?

5. ¿Feature flags para pipelines de datos — ¿tiene sentido?

---

### Ejercicio 19.7.2 — El pipeline de CI: de commit a deploy

```yaml
# GitHub Actions workflow para CI de pipelines de datos:

name: data-pipeline-ci
on:
  pull_request:
    paths: ["dags/**", "transformations/**", "tests/**"]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install ruff
      - run: ruff check dags/ transformations/

  unit-tests:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements-test.txt
      - run: pytest tests/unit/ -v --tb=short

  dag-validation:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install apache-airflow==2.8.0
      - run: pytest tests/dag_validation/ -v

  component-tests:
    runs-on: ubuntu-latest
    needs: [unit-tests, dag-validation]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { java-version: "17" }  # para PySpark
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements-test.txt pyspark==3.5.0
      - run: pytest tests/component/ -v --tb=short

  integration-tests:
    runs-on: ubuntu-latest
    needs: component-tests
    if: github.event.pull_request.draft == false
    services:
      postgres:
        image: postgres:16
        env: { POSTGRES_DB: test, POSTGRES_PASSWORD: test }
        ports: ["5432:5432"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements-test.txt
      - run: pytest tests/integration/ -v --tb=short

  contract-check:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - run: pip install pandera
      - run: pytest tests/contracts/ -v
```

**Preguntas:**

1. ¿Instalar Airflow en CI solo para DAG validation es pesado?
   ¿Hay una alternativa más ligera?

2. ¿Java es necesario en CI para PySpark? ¿Puedes evitarlo?

3. ¿Los integration tests con Testcontainers son más flexibles
   que los `services` de GitHub Actions?

4. ¿Este pipeline de CI tarda cuánto? ¿5 minutos? ¿15? ¿30?

5. ¿Los contract checks deben bloquear el merge
   o solo generar un warning?

---

### Ejercicio 19.7.3 — Data diff: comparar output antes y después del cambio

```python
# Data diff: la técnica más efectiva para validar cambios en pipelines.
# Ejecutar el pipeline viejo y el nuevo con los mismos datos
# y comparar los resultados.

import polars as pl

def data_diff(df_antes: pl.DataFrame, df_despues: pl.DataFrame,
              key_columns: list[str]) -> dict:
    """Comparar dos DataFrames y reportar diferencias."""

    # Filas que existen en ambos:
    merged = df_antes.join(df_despues, on=key_columns,
                           how="full", suffix="_new")

    # Filas nuevas (no estaban antes):
    nuevas = df_despues.join(df_antes, on=key_columns, how="anti")

    # Filas eliminadas (no están después):
    eliminadas = df_antes.join(df_despues, on=key_columns, how="anti")

    # Filas modificadas (misma key, distintos valores):
    comunes = df_antes.join(df_despues, on=key_columns, how="inner",
                            suffix="_new")
    value_columns = [c for c in df_antes.columns if c not in key_columns]
    modificadas = comunes.filter(
        pl.any_horizontal([
            pl.col(c) != pl.col(f"{c}_new") for c in value_columns
        ])
    )

    return {
        "total_antes": len(df_antes),
        "total_despues": len(df_despues),
        "filas_nuevas": len(nuevas),
        "filas_eliminadas": len(eliminadas),
        "filas_modificadas": len(modificadas),
        "sin_cambios": len(df_antes) - len(eliminadas) - len(modificadas),
    }

# Uso en CI:
def test_cambio_no_rompe_output():
    """Verificar que un cambio en el pipeline no altera los resultados."""
    datos_input = crear_ventas(1000)

    resultado_antes = pipeline_viejo(datos_input)
    resultado_despues = pipeline_nuevo(datos_input)

    diff = data_diff(resultado_antes, resultado_despues,
                     key_columns=["region", "fecha"])

    # El cambio no debería modificar el output:
    assert diff["filas_nuevas"] == 0, f"Filas nuevas inesperadas: {diff}"
    assert diff["filas_eliminadas"] == 0, f"Filas eliminadas: {diff}"
    # O si es un cambio intencional, verificar que es lo esperado:
    # assert diff["filas_modificadas"] == 5, "Esperamos 5 filas modificadas"
```

**Preguntas:**

1. ¿Data diff es viable para tablas de millones de filas?

2. ¿datafold (SaaS) automatiza data diffs en CI?

3. ¿Comparar floats en un data diff requiere tolerancia?

4. ¿El data diff debe comparar el output completo
   o solo una muestra?

5. ¿Data diff es el "test de regresión" para pipelines de datos?

---

### Ejercicio 19.7.4 — Testing en producción: shadow mode y feature flags

```python
# Cuando no puedes testear todo en staging, puedes testear en producción
# de forma segura:

# Shadow mode: ejecutar el pipeline nuevo EN PARALELO con el viejo,
# comparar resultados, pero solo usar los del viejo.

def pipeline_con_shadow(fecha, config):
    # Pipeline principal (producción):
    resultado_prod = pipeline_v1(fecha, config)
    guardar(resultado_prod, tabla="metricas_diarias")

    # Shadow pipeline (experimental):
    try:
        resultado_shadow = pipeline_v2(fecha, config)
        guardar(resultado_shadow, tabla="metricas_diarias_shadow")

        # Comparar (async, no bloquea):
        diff = data_diff(resultado_prod, resultado_shadow,
                         key_columns=["region"])
        if diff["filas_modificadas"] > 0:
            log.warning("shadow_diff_detected", diff=diff)
            enviar_alerta_slack(f"Shadow diff: {diff}")
    except Exception as e:
        log.error("shadow_pipeline_failed", error=str(e))
        # El pipeline de producción NO se afecta.

# Feature flags: activar la nueva versión gradualmente.
def pipeline_con_feature_flag(fecha, config):
    if feature_flag("use_new_transform"):
        resultado = pipeline_v2(fecha, config)
    else:
        resultado = pipeline_v1(fecha, config)
    guardar(resultado)
```

**Preguntas:**

1. ¿Shadow mode duplica el costo de procesamiento?

2. ¿Feature flags para pipelines de datos son simples
   o añaden complejidad innecesaria?

3. ¿Cuánto tiempo debes mantener el shadow mode
   antes de confiar en el pipeline nuevo?

4. ¿LaunchDarkly / Unleash funcionan para feature flags de pipelines?

5. ¿Shadow mode con streaming (Flink) es posible?

---

### Ejercicio 19.7.5 — Implementar: CI/CD completo para el e-commerce

**Tipo: Implementar**

```python
# Diseñar e implementar el pipeline de CI/CD completo para el sistema
# de e-commerce:
#
# CI (en cada PR):
#   1. Lint (ruff)
#   2. Unit tests (pytest, funciones puras)
#   3. DAG validation (parseo, owners, retries)
#   4. Contract checks (Pandera schemas)
#   5. Component tests (Spark local)
#   6. Integration tests (Testcontainers para PostgreSQL + Kafka)
#
# CD (en merge a main):
#   7. Deploy DAGs a staging
#   8. E2E test en staging (1 fecha)
#   9. Data diff (staging vs producción para misma fecha)
#   10. Deploy DAGs a producción
#   11. Canary run (1 fecha en producción)
#   12. Verificar métricas post-deploy

# Implementar:
# a) GitHub Actions workflow completo
# b) Makefile con targets para cada paso
# c) Al menos 20 tests distribuidos en las categorías
# d) Data diff entre versiones del pipeline
```

**Restricciones:**
1. El CI debe completar en < 10 minutos
2. Los tests deben ser independientes (sin orden)
3. El CD debe tener gates manuales para producción
4. Incluir rollback automático si el canary falla

---

## Resumen del capítulo

**Testing de pipelines: la pirámide en práctica**

```
Nivel 5: E2E (staging)
  Pipeline completo contra datos reales anonimizados.
  → Correr antes de deploy, no en cada commit.
  → Verifica integración, credenciales, formatos.

Nivel 4: Integration (Testcontainers)
  Componentes reales (PostgreSQL, Kafka) en containers desechables.
  → Correr en CI, en PRs no-draft.
  → Verifica I/O, serialización, compatibilidad.

Nivel 3: Component (Spark local)
  Transformaciones en el framework real con datos sintéticos.
  → Correr en cada commit.
  → Verifica que la transformación funciona en Spark/Polars/Beam.

Nivel 2: Contract (Pandera, Schema Registry)
  Verificar que inputs y outputs cumplen el schema esperado.
  → Correr en cada commit.
  → Previene roturas silenciosas por cambios upstream.

Nivel 1: Unit (pytest)
  Funciones puras sin framework, con datos controlados.
  → Correr en cada commit.
  → Verifica lógica de negocio: cálculos, filtros, validaciones.
```

**El principio del testing de datos:**

```
Un pipeline de datos no testeado es una promesa sin garantía.
Funciona hasta que deja de funcionar.
Y cuando deja de funcionar, descubres que no tenías
forma de saber cuándo empezó a fallar.

  Sin tests:    "funciona en mi laptop" → producción → rezar
  Con tests:    "pasa 47 tests, 12 contracts, y 3 E2E scenarios"
                → producción → confianza
                → si algo falla, el test suite lo habría prevenido

La inversión en testing no se mide en coverage —
se mide en incidentes prevenidos y en confianza ganada.
```

**Conexión con el Cap.20 (El sistema completo):**

> Has construido pipelines (Partes 1-4), los orquestas (Cap.17),
> los observas (Cap.18), y los testeas (Cap.19).
> El Cap.20 integra todo: el sistema de e-commerce completo
> funcionando end-to-end, con orquestación, observabilidad,
> y testing como partes inseparables del sistema de datos.
> No es un capítulo nuevo — es la culminación de todo el libro.

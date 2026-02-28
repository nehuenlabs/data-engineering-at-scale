# Guía de Ejercicios — Cap.17: Orquestación — Airflow, Dagster, Prefect

> Un pipeline que no se ejecuta solo no es un pipeline — es un script.
>
> La orquestación es lo que convierte scripts en sistemas:
> definir qué debe ejecutarse, en qué orden, cuándo,
> qué hacer si falla, y cómo saber si funcionó.
>
> Airflow no ejecuta tus datos — ejecuta tus tareas.
> Dagster no mueve tus datos — declara qué datos deberían existir.
> Prefect no transforma tus datos — garantiza que la transformación ocurra.
>
> La diferencia entre un data engineer junior y uno senior
> no es qué frameworks conoce — es cómo opera lo que construyó.
> La orquestación es donde el "funciona en mi laptop"
> se convierte en "funciona todos los días a las 3am sin intervención humana".

---

## El modelo mental: orquestación como grafo de dependencias

```
Lo que un orquestador hace:

  DAG (Directed Acyclic Graph) de tareas:

  extraer_datos ──→ validar_schema ──→ transformar ──→ cargar ──→ notificar
                                           │
                                           ├──→ calcular_metricas ──→ publicar_dashboard
                                           │
                                           └──→ exportar_csv ──→ enviar_email

  El orquestador:
  1. Sabe el orden (extraer ANTES de transformar)
  2. Sabe las dependencias (transformar SOLO SI validar_schema pasa)
  3. Sabe el schedule (todos los días a las 3:00 UTC)
  4. Sabe qué hacer si falla (reintentar 3 veces, esperar 5 min entre cada una)
  5. Sabe cómo notificar (Slack si falla, email si tarda más de 1 hora)

Lo que un orquestador NO hace:

  ✗ No ejecuta la transformación de datos (eso lo hace Spark/Polars/SQL)
  ✗ No almacena los datos (eso lo hace S3/BigQuery/Delta Lake)
  ✗ No mueve los datos entre sistemas (eso lo hace el código que orquesta)
  ✗ No monitorea la calidad de los datos (eso lo hace Great Expectations/dbt tests)

  El orquestador es el director de orquesta — no toca ningún instrumento.
```

```
Los tres paradigmas de orquestación en 2024:

  Airflow (2014)               Dagster (2019)              Prefect (2018)
  ─────────────────            ─────────────────           ─────────────────
  Modelo: tareas               Modelo: assets              Modelo: flujos
  Foco: "ejecutar T2           Foco: "el asset A           Foco: "ejecutar esta
   después de T1"               debería existir             función Python
                                 con este schema"            de forma confiable"

  DAG define el flujo          Graph define qué datos      Flow define el código
  Operator ejecuta             Op produce/consume          Task ejecuta
  XCom pasa metadata           IO Manager persiste         Result persiste
  Sensor espera                Sensor/Schedule dispara     Trigger/Schedule dispara

  Madurez: 10 años             Madurez: 5 años             Madurez: 6 años
  Ecosistema: enorme           Ecosistema: creciendo       Ecosistema: medio
  Adopción: estándar           Adopción: rápida            Adopción: nicho

  El 70% de la industria       La alternativa más          La opción más "Pythonic"
  usa Airflow en 2024.         seria a Airflow.            y ligera.
```

---

## Tabla de contenidos

- [Sección 17.1 — Qué es orquestación: DAGs, dependencias, idempotencia](#sección-171--qué-es-orquestación-dags-dependencias-idempotencia)
- [Sección 17.2 — Airflow: el estándar de facto](#sección-172--airflow-el-estándar-de-facto)
- [Sección 17.3 — Dagster: assets y software-defined data](#sección-173--dagster-assets-y-software-defined-data)
- [Sección 17.4 — Prefect: orquestación como código Python](#sección-174--prefect-orquestación-como-código-python)
- [Sección 17.5 — Patrones de producción: backfill, reintentos, SLAs](#sección-175--patrones-de-producción-backfill-reintentos-slas)
- [Sección 17.6 — Orquestación de streaming y pipelines híbridos](#sección-176--orquestación-de-streaming-y-pipelines-híbridos)
- [Sección 17.7 — El e-commerce orquestado: integrando todo](#sección-177--el-e-commerce-orquestado-integrando-todo)

---

## Sección 17.1 — Qué es Orquestación: DAGs, Dependencias, Idempotencia

### Ejercicio 17.1.1 — Leer: por qué cron no es un orquestador

**Tipo: Leer**

```
cron vs orquestador:

  cron:
    0 3 * * * /opt/pipeline/extraer.sh
    0 4 * * * /opt/pipeline/transformar.sh
    0 5 * * * /opt/pipeline/cargar.sh

  Problemas:
  1. Si extraer.sh tarda 2 horas (en vez de 1), transformar.sh arranca
     sobre datos incompletos → resultados corruptos.
  2. Si transformar.sh falla, cargar.sh ejecuta de todas formas
     → carga datos del día anterior (o nada).
  3. Si extraer.sh falla a las 3:15am, nadie se entera hasta las 9am.
  4. Si necesitas re-ejecutar el pipeline del martes,
     tienes que cambiar las fechas manualmente en cada script.
  5. Si añades un paso entre transformar y cargar,
     tienes que recalcular todos los horarios de cron.

  orquestador:
    extraer >> transformar >> cargar

  Soluciones:
  1. transformar espera a que extraer TERMINE (no a que sea una hora).
  2. Si transformar falla, cargar NO ejecuta.
  3. Alerta inmediata por Slack/email/PagerDuty.
  4. Backfill: re-ejecutar el pipeline del martes con un click.
  5. Añadir un paso es añadir un nodo al grafo — las dependencias se resuelven solas.
```

**Preguntas:**

1. ¿cron tiene alguna ventaja sobre un orquestador para tareas simples?
   ¿Cuándo es suficiente?

2. ¿Un orquestador puede reemplazar completamente a cron en un servidor?

3. ¿Qué pasa si el orquestador mismo se cae?
   ¿Quién orquesta al orquestador?

4. ¿systemd timers son mejor que cron? ¿Resuelven alguno de los problemas listados?

5. ¿Un pipeline que solo tiene una tarea necesita un orquestador?

**Pista:** cron es suficiente para tareas aisladas sin dependencias:
rotar logs, limpiar `/tmp`, enviar un reporte diario. Pero en cuanto
tienes dos tareas donde la segunda depende de la primera,
cron ya no puede expresar esa relación. systemd timers añaden
dependencias entre unidades y notificación de fallos, pero no tienen
UI, backfill, ni gestión de estado — no son un orquestador de datos.

---

### Ejercicio 17.1.2 — DAGs: por qué dirigidos y por qué acíclicos

```
DAG = Directed Acyclic Graph

  Directed: cada arista tiene dirección (A → B, no A ↔ B).
            "A debe completar ANTES de que B empiece."

  Acyclic:  no hay ciclos (A → B → C → A es inválido).
            Si A depende de C y C depende de A, ¿quién va primero?
            → deadlock conceptual.

  Ejemplo válido:
    A → B → D
    A → C → D
    B no depende de C (pueden ejecutar en paralelo).
    D espera a B Y C.

  Ejemplo inválido (ciclo):
    A → B → C → A
    ¿Quién ejecuta primero? Nadie puede empezar.

  En el contexto de datos:
    extraer_ventas ──────────→ join_ventas_clientes ──→ cargar
    extraer_clientes ────────↗
    La extracción de ventas y clientes son independientes → paralelas.
    El join necesita ambas → espera a las dos.

  ¿Por qué no permitir ciclos?
  Un pipeline de datos tiene un inicio (fuentes) y un fin (destinos).
  Los ciclos implicarían que un resultado alimenta su propia entrada
  en la misma ejecución → indefinido, no reproducible.

  Si necesitas "loops" (re-procesar hasta que converja),
  el patrón correcto es: un DAG por iteración,
  con un sensor que verifica la condición de convergencia.
```

**Preguntas:**

1. ¿Spark también modela el procesamiento como un DAG?
   ¿Es el mismo concepto que el DAG de Airflow?

2. ¿Un pipeline con branching condicional (si X → hacer A, si no → hacer B)
   es un DAG? ¿Cómo lo representas?

3. ¿Puede un DAG tener nodos sin dependencias entrantes ni salientes?
   ¿Para qué serviría?

4. ¿Cuántas tareas puede tener un DAG antes de que se vuelva inmanejable?

5. ¿Los DAGs de Airflow se definen en tiempo de parseo o en tiempo de ejecución?

---

### Ejercicio 17.1.3 — Idempotencia: el principio más importante de orquestación

```python
# Idempotencia: ejecutar la misma operación N veces produce el mismo resultado
# que ejecutarla 1 vez.

# NO idempotente (peligroso):
def cargar_ventas(fecha):
    df = leer_ventas(fecha)
    df.write.mode("append").saveAsTable("ventas")
    # Si ejecutas dos veces, los datos se duplican.

# Idempotente (seguro):
def cargar_ventas_idempotente(fecha):
    df = leer_ventas(fecha)
    df.write.mode("overwrite") \
        .option("replaceWhere", f"fecha = '{fecha}'") \
        .saveAsTable("ventas")
    # Si ejecutas dos veces, el resultado es el mismo.
    # La partición del día se reemplaza completamente.

# ¿Por qué importa?
# 1. Si un pipeline falla a la mitad y lo re-ejecutas,
#    los pasos que ya completaron NO deben duplicar datos.
# 2. Si haces backfill de 30 días, cada día debe poder
#    re-ejecutarse sin afectar a los demás.
# 3. Si Airflow ejecuta una tarea dos veces por un bug del scheduler,
#    el resultado debe ser correcto.

# Patrones de idempotencia:
# a) UPSERT: INSERT ... ON CONFLICT UPDATE
# b) Overwrite partición: reemplazar la partición completa
# c) DELETE + INSERT: borrar los datos del período y re-insertar
# d) Staging table: escribir a una tabla temporal, luego swap atómico
```

**Preguntas:**

1. ¿Un pipeline que llama a una API externa (ej: enviar email)
   puede ser idempotente? ¿Cómo?

2. ¿`mode("overwrite")` sin `replaceWhere` es idempotente?
   ¿Qué riesgo tiene?

3. ¿DELETE + INSERT en una transacción SQL es idempotente?
   ¿Y si la base de datos no soporta transacciones?

4. ¿La idempotencia es responsabilidad del orquestador
   o del código del pipeline?

5. ¿Un pipeline de streaming (Flink, Kafka Streams) necesita
   ser idempotente de la misma forma que un pipeline batch?

**Pista:** La idempotencia es responsabilidad del código, no del orquestador.
Airflow puede reintentar una tarea, pero no puede garantizar que el resultado
sea correcto si la tarea no es idempotente. Para side effects externos
(enviar email, llamar API), el patrón es: verificar si ya se ejecutó antes
(ej: guardar un "receipt" en una tabla de control) y saltar si ya se hizo.

---

### Ejercicio 17.1.4 — Execution date vs run date: la confusión más común

```python
# Airflow (y otros orquestadores) distinguen entre:
# - data interval: el período de datos que el pipeline procesa
# - run date: cuándo ejecuta realmente el pipeline

# Ejemplo:
# DAG programado para ejecutar diariamente a las 3:00 UTC
# El run del 2024-03-15T03:00:00 procesa datos del 2024-03-14

# En Airflow 2.x:
# - data_interval_start = 2024-03-14T00:00:00
# - data_interval_end   = 2024-03-15T00:00:00
# - logical_date        = 2024-03-14T00:00:00 (antes: execution_date)
# - run date real       = 2024-03-15T03:00:00

# ¿Por qué?
# El pipeline del "día 14 de marzo" no puede ejecutar DURANTE
# el 14 de marzo — los datos del día 14 aún no están completos.
# Ejecuta al INICIO del siguiente intervalo (00:00 del 15 = 03:00 UTC).

# Esto confunde a TODOS los usuarios nuevos de Airflow.
# "¿Por qué el execution_date es ayer y no hoy?"

# En Dagster y Prefect, la semántica es más intuitiva:
# Dagster: particiones explícitas con fechas claras
# Prefect: no tiene el concepto de execution_date — la lógica de fechas es tuya

# El patrón correcto en Airflow:
def mi_tarea(**context):
    # Datos a procesar:
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")
    # fecha = "2024-03-14" (el día que QUEREMOS procesar)

    # NO usar:
    # datetime.now()  ← esto da la fecha de ejecución, no la fecha de datos
```

**Preguntas:**

1. ¿Por qué Airflow eligió esta semántica de "ejecutar al final del intervalo"?

2. ¿`logical_date` y `execution_date` son lo mismo en Airflow 2.x?

3. ¿Si un DAG se ejecuta manualmente (trigger manual), cuál es el `data_interval`?

4. ¿Dagster resolvió este problema? ¿Cómo maneja las fechas de datos?

5. ¿Para un pipeline de streaming, el concepto de `data_interval` tiene sentido?

---

### Ejercicio 17.1.5 — Leer: anatomía de un fallo en producción por falta de orquestación

**Tipo: Diagnosticar**

```
Incidente: Dashboard de revenue muestra $0 para el día de ayer.

Timeline:
  03:00 UTC — cron ejecuta extract.sh → extrae ventas del día anterior de PostgreSQL
  03:45 UTC — extract.sh termina OK
  04:00 UTC — cron ejecuta transform.py → lee el CSV de extract.sh, calcula métricas
              → FALLA: PostgreSQL estaba en mantenimiento, extract.sh retornó 0 filas
              → transform.py no falla (0 filas es válido) → escribe metricas.csv con $0
  05:00 UTC — cron ejecuta load.sh → carga metricas.csv a BigQuery → $0 revenue
  09:00 UTC — el VP de ventas ve el dashboard → pánico → escala al CTO

Problemas:
  1. extract.sh no verificó si el resultado tenía datos (quality check ausente)
  2. transform.py no falló con 0 filas (no tiene mínimo esperado)
  3. No hay alerta por datos anómalos (revenue = $0 debería ser imposible)
  4. No hay forma de re-ejecutar solo el día anterior sin riesgo de duplicados
  5. El VP se enteró 6 horas después del fallo
```

**Preguntas:**

1. ¿Cuál de los 5 problemas resuelve un orquestador directamente?
   ¿Cuáles requieren código adicional?

2. ¿Cómo implementarías un "quality gate" entre extract y transform?

3. ¿El pipeline debería fallar si extract devuelve 0 filas?
   ¿O debería completar y alertar?

4. ¿Cómo detectas "revenue = $0 es anómalo" sin hardcodear el número?

5. ¿Diseña el DAG de Airflow que resolvería este incidente,
   incluyendo quality checks y alertas.

---

## Sección 17.2 — Airflow: el Estándar de Facto

### Ejercicio 17.2.1 — Leer: la arquitectura de Airflow

**Tipo: Leer**

```
Arquitectura de Airflow:

  ┌─────────────────────────────────────────────────┐
  │                   Webserver                      │
  │  UI para monitorear DAGs, ver logs, trigger      │
  │  runs, ver estado de tareas.                     │
  └──────────────────────┬──────────────────────────┘
                         │ lee estado de
  ┌──────────────────────▼──────────────────────────┐
  │               Metadata Database                  │
  │  PostgreSQL o MySQL. Guarda:                     │
  │  - Definiciones de DAGs                          │
  │  - Estado de cada DagRun y TaskInstance           │
  │  - XCom (metadata entre tareas)                  │
  │  - Variables y Connections                       │
  └──────────────────────┬──────────────────────────┘
                         │ lee/escribe
  ┌──────────────────────▼──────────────────────────┐
  │                  Scheduler                       │
  │  El corazón de Airflow:                          │
  │  - Parsea los archivos .py del dag_folder        │
  │  - Decide qué tareas deben ejecutarse            │
  │  - Crea TaskInstances y las envía al Executor    │
  │  - Maneja reintentos, timeouts, SLAs             │
  └──────────────────────┬──────────────────────────┘
                         │ envía tareas a
  ┌──────────────────────▼──────────────────────────┐
  │                   Executor                       │
  │  Quien ejecuta las tareas:                       │
  │  - LocalExecutor: procesos en la misma máquina   │
  │  - CeleryExecutor: workers distribuidos (Redis)  │
  │  - KubernetesExecutor: un pod por tarea          │
  └──────────────────────────────────────────────────┘

¿Por qué importa la arquitectura?
  - El Scheduler es el single point of failure (SPOF)
    → en Airflow 2.x puedes tener múltiples schedulers (HA)
  - La metadata DB debe ser rápida y disponible
    → si la DB se cae, Airflow se detiene completamente
  - El Executor determina la escalabilidad
    → KubernetesExecutor escala a miles de tareas concurrentes
```

**Preguntas:**

1. ¿El Scheduler parsea TODOS los archivos .py del dag_folder en cada ciclo?
   ¿Qué impacto tiene en rendimiento si tienes 500 DAGs?

2. ¿KubernetesExecutor vs CeleryExecutor — cuándo cada uno?

3. ¿El Webserver puede caerse sin afectar la ejecución de los pipelines?

4. ¿XCom es un mecanismo adecuado para pasar datos grandes entre tareas?

5. ¿Airflow 2.x con múltiples schedulers es truly HA?
   ¿Qué componente sigue siendo SPOF?

---

### Ejercicio 17.2.2 — El primer DAG: pipeline batch de e-commerce

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-alerts@empresa.com"],
}

with DAG(
    dag_id="ecommerce_daily_metrics",
    default_args=default_args,
    description="Pipeline diario de métricas de e-commerce",
    schedule="0 3 * * *",  # todos los días a las 3:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "daily", "metrics"],
    max_active_runs=1,
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    def extraer_ventas(**context):
        fecha = context["data_interval_start"].strftime("%Y-%m-%d")
        # Lógica de extracción desde PostgreSQL
        # Escribe a GCS: gs://data-lake/raw/ventas/fecha={fecha}/
        print(f"Extrayendo ventas del {fecha}")

    extraer = PythonOperator(
        task_id="extraer_ventas",
        python_callable=extraer_ventas,
    )

    validar = PythonOperator(
        task_id="validar_schema",
        python_callable=lambda **ctx: print("Validando schema..."),
    )

    transformar = BigQueryInsertJobOperator(
        task_id="transformar_metricas",
        configuration={
            "query": {
                "query": """
                    INSERT INTO `proyecto.dataset.metricas_diarias`
                    SELECT
                        DATE('{{ data_interval_start | ds }}') AS fecha,
                        region,
                        SUM(monto) AS revenue,
                        COUNT(DISTINCT user_id) AS usuarios_unicos,
                        COUNT(*) AS transacciones
                    FROM `proyecto.dataset.ventas_raw`
                    WHERE fecha = '{{ data_interval_start | ds }}'
                    GROUP BY 1, 2
                """,
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    notificar = PythonOperator(
        task_id="notificar_completado",
        python_callable=lambda **ctx: print("Pipeline completado"),
    )

    fin = EmptyOperator(task_id="fin")

    inicio >> extraer >> validar >> transformar >> notificar >> fin
```

**Preguntas:**

1. ¿`catchup=False` es una buena práctica? ¿Qué pasa si lo dejas en `True`?

2. ¿`max_active_runs=1` evita que dos ejecuciones del mismo DAG corran
   simultáneamente? ¿Por qué es importante?

3. ¿La template `{{ data_interval_start | ds }}` se resuelve en tiempo
   de parseo o en tiempo de ejecución?

4. ¿`retry_exponential_backoff` es mejor que `retry_delay` fijo?
   ¿Cuándo no lo es?

5. ¿Por qué usar `EmptyOperator` para inicio y fin?
   ¿Es solo estético o tiene utilidad?

---

### Ejercicio 17.2.3 — TaskFlow API: Python nativo en Airflow 2.x

```python
from airflow.decorators import dag, task
from datetime import datetime
import json

@dag(
    dag_id="ecommerce_taskflow",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce"],
)
def ecommerce_taskflow():

    @task()
    def extraer_ventas(fecha: str) -> dict:
        """Extraer ventas y retornar metadata."""
        # Simular extracción:
        registros = 15_432
        ruta = f"gs://data-lake/raw/ventas/fecha={fecha}/"
        return {"registros": registros, "ruta": ruta, "fecha": fecha}

    @task()
    def extraer_clientes() -> dict:
        """Extraer tabla de clientes (dimensión, no depende de fecha)."""
        return {"registros": 50_000, "ruta": "gs://data-lake/dim/clientes/"}

    @task()
    def validar(metadata_ventas: dict, metadata_clientes: dict) -> dict:
        """Quality gate: verificar que los datos tienen sentido."""
        if metadata_ventas["registros"] < 100:
            raise ValueError(
                f"Solo {metadata_ventas['registros']} ventas — "
                f"esperamos al menos 100. ¿Fuente caída?"
            )
        if metadata_clientes["registros"] == 0:
            raise ValueError("Tabla de clientes vacía.")
        return {**metadata_ventas, "validado": True}

    @task()
    def transformar(metadata: dict) -> dict:
        """Ejecutar transformación en BigQuery/Spark."""
        fecha = metadata["fecha"]
        # spark.read.parquet(metadata["ruta"]).groupBy("region")...
        return {"tabla_destino": f"metricas_diarias/fecha={fecha}"}

    @task()
    def notificar(resultado: dict):
        """Enviar notificación de éxito."""
        print(f"Pipeline completado: {resultado['tabla_destino']}")

    # Definir el grafo (las dependencias se infieren de los argumentos):
    fecha = "{{ data_interval_start | ds }}"
    ventas = extraer_ventas(fecha)
    clientes = extraer_clientes()
    validado = validar(ventas, clientes)
    resultado = transformar(validado)
    notificar(resultado)

ecommerce_taskflow()
```

**Preguntas:**

1. ¿TaskFlow API es más legible que la API clásica de operators?
   ¿Tiene limitaciones?

2. ¿Los `return` de cada task se almacenan en XCom?
   ¿Qué pasa si retornas un DataFrame de 1 GB?

3. ¿Las dependencias se infieren de los argumentos de las funciones?
   ¿Puedes tener dependencias que no pasan datos?

4. ¿`extraer_ventas` y `extraer_clientes` ejecutan en paralelo?
   ¿Airflow lo decide automáticamente?

5. ¿TaskFlow API funciona con KubernetesExecutor?
   ¿Cada task corre en un pod separado?

**Pista:** TaskFlow serializa los return values vía XCom (por defecto en la metadata DB).
Para datos grandes, usa un XCom backend externo (S3, GCS) o mejor aún:
no pases los datos — pasa la ruta donde están. El patrón correcto es
retornar metadata (ruta, número de registros, schema) no los datos mismos.

---

### Ejercicio 17.2.4 — Sensors, branching, y dynamic tasks

```python
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id="ecommerce_con_sensors",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def pipeline_con_sensors():

    # Sensor: esperar a que el archivo exista antes de procesar
    esperar_archivo = FileSensor(
        task_id="esperar_archivo_ventas",
        filepath="/data/exports/ventas_{{ ds }}.csv",
        poke_interval=300,         # verificar cada 5 minutos
        timeout=3600,              # fallar después de 1 hora
        mode="reschedule",         # liberar el worker slot mientras espera
    )

    # Branching: decidir el camino según el volumen de datos
    def decidir_ruta(**context):
        registros = context["task_instance"].xcom_pull(task_ids="contar_registros")
        if registros > 1_000_000:
            return "procesar_spark"    # datos grandes → Spark
        else:
            return "procesar_polars"   # datos pequeños → Polars local

    branch = BranchPythonOperator(
        task_id="decidir_ruta",
        python_callable=decidir_ruta,
    )

    procesar_spark = EmptyOperator(task_id="procesar_spark")
    procesar_polars = EmptyOperator(task_id="procesar_polars")

    # Dynamic task mapping (Airflow 2.3+):
    @task()
    def generar_fechas() -> list[str]:
        """Generar lista de fechas para procesar en paralelo."""
        return ["2024-03-01", "2024-03-02", "2024-03-03"]

    @task()
    def procesar_fecha(fecha: str):
        print(f"Procesando {fecha}")

    # Cada fecha genera una tarea independiente:
    fechas = generar_fechas()
    procesar_fecha.expand(fecha=fechas)

pipeline_con_sensors()
```

**Preguntas:**

1. ¿`mode="reschedule"` vs `mode="poke"` en un Sensor — cuál es más eficiente?

2. ¿BranchPythonOperator puede elegir múltiples rutas?
   ¿Qué pasa con las tareas downstream de la ruta no elegida?

3. ¿Dynamic task mapping reemplaza a los DAGs generados dinámicamente?

4. ¿Qué pasa si `generar_fechas` retorna 10,000 fechas?
   ¿Airflow puede manejar 10,000 tareas dinámicas?

5. ¿Un Sensor que hace polling cada 5 minutos durante 1 hora
   consume recursos del cluster durante toda la hora?

---

### Ejercicio 17.2.5 — Leer: diagnosticar un DAG lento en Airflow

**Tipo: Diagnosticar**

```
Síntoma: el DAG "daily_metrics" debería completar en 30 minutos
         pero lleva 3 horas. Las tareas individuales son rápidas (2-5 min).

Datos del Airflow UI:
  Task                 State      Duration   Start Time      End Time
  ──────────────────   ────────   ────────   ──────────────  ──────────────
  extraer_ventas       success    3m         03:00:15        03:03:15
  extraer_clientes     success    2m         03:03:30        03:05:30
  validar              queued     —          03:05:45        (esperando)
                       success    1m         03:47:00        03:48:00
  transformar          queued     —          03:48:15        (esperando)
                       success    5m         04:32:00        04:37:00
  cargar               success    2m         04:37:15        04:39:15

  ¿Ves el problema? Las tareas están en "queued" durante 40+ minutos.
  El tiempo total es 3h, pero el tiempo de ejecución real es 13 minutos.

Posibles causas:
  1. Pool lleno: el pool "default" tiene max 16 slots y hay 50 DAGs activos
  2. Worker saturado: CeleryExecutor con 4 workers de 8 slots = 32 slots totales
  3. Scheduler lag: el scheduler tarda en parsear 500 DAGs
  4. Dependency check: el scheduler verifica dependencias cada 30 segundos
```

**Preguntas:**

1. ¿Cómo distingues "la tarea es lenta" de "la tarea está esperando un slot"?

2. ¿Pools de Airflow pueden priorizar DAGs críticos sobre DAGs de menor prioridad?

3. ¿Qué configuración del Scheduler reduce el lag entre "queued" y "running"?

4. ¿KubernetesExecutor elimina el problema de slots limitados?

5. ¿Cómo monitoreas la "queue time" de las tareas de forma sistemática?

**Pista:** La diferencia entre `queued` (la tarea está lista para ejecutar)
y `running` (la tarea está ejecutando) es el slot del executor.
Si `validar` estuvo en `queued` durante 42 minutos, significa que no había
un worker slot disponible. La solución: aumentar workers, usar pools para
priorizar, o migrar a KubernetesExecutor que escala dinámicamente.
`scheduler_heartbeat_sec` y `min_file_process_interval` controlan
la velocidad del scheduler.

---

## Sección 17.3 — Dagster: Assets y Software-Defined Data

### Ejercicio 17.3.1 — Leer: el paradigma de assets vs tareas

**Tipo: Leer**

```
Airflow piensa en TAREAS:                Dagster piensa en ASSETS:
  "ejecutar extract.py"                    "el asset ventas_raw debe existir"
  "ejecutar transform.py"                  "el asset metricas_diarias debe existir"
  "ejecutar load.py"                       "metricas_diarias depende de ventas_raw"

La diferencia es sutil pero profunda:

  Airflow: defines QUÉ HACER (tareas) y en QUÉ ORDEN.
           El resultado (los datos) es un efecto secundario.

  Dagster: defines QUÉ DATOS DEBERÍAN EXISTIR (assets).
           Las tareas son la forma de producirlos.

Implicación práctica:
  En Airflow, si quieres saber "¿de dónde viene la tabla metricas_diarias?",
  tienes que leer el código del DAG y rastrear qué tarea la produce.

  En Dagster, metricas_diarias ES un asset declarado con sus dependencias.
  La UI muestra un grafo de assets (datos) no de tareas (ejecución).

  Es como la diferencia entre programación imperativa y declarativa:
  Airflow: "haz esto, luego esto, luego esto"
  Dagster: "quiero que esto exista, y depende de aquello"
```

**Preguntas:**

1. ¿El modelo de assets de Dagster es similar al modelo de dbt
   (donde defines modelos SQL y las dependencias se infieren)?

2. ¿Puedes usar Dagster como orquestador tradicional (tareas sin assets)?

3. ¿El modelo de assets facilita el data lineage?

4. ¿Airflow 2.x con datasets se acerca al modelo de assets de Dagster?

5. ¿Para un pipeline que solo mueve archivos de A a B,
   el modelo de assets tiene sentido?

---

### Ejercicio 17.3.2 — El primer pipeline en Dagster

```python
import dagster as dg
import polars as pl
from datetime import datetime

# Un asset es una función que produce datos:
@dg.asset(
    description="Ventas diarias extraídas de PostgreSQL",
    metadata={"source": "postgresql", "update_frequency": "daily"},
    group_name="raw",
)
def ventas_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Extraer ventas del día."""
    fecha = context.partition_key  # "2024-03-14"
    context.log.info(f"Extrayendo ventas del {fecha}")

    # Simular extracción:
    df = pl.DataFrame({
        "user_id": ["alice", "bob", "carol"],
        "monto": [150.0, 75.0, 300.0],
        "region": ["norte", "sur", "norte"],
        "fecha": [fecha] * 3,
    })
    context.add_output_metadata({
        "num_registros": len(df),
        "regiones": df["region"].unique().to_list(),
    })
    return df


@dg.asset(
    description="Tabla de clientes (dimensión)",
    group_name="raw",
)
def clientes_dim() -> pl.DataFrame:
    return pl.DataFrame({
        "user_id": ["alice", "bob", "carol"],
        "segmento": ["premium", "standard", "premium"],
        "pais": ["CL", "CL", "AR"],
    })


@dg.asset(
    description="Métricas diarias de revenue por región y segmento",
    deps=[ventas_raw, clientes_dim],
    group_name="analytics",
)
def metricas_diarias(
    context: dg.AssetExecutionContext,
    ventas_raw: pl.DataFrame,
    clientes_dim: pl.DataFrame,
) -> pl.DataFrame:
    """Join + agregación → métricas de negocio."""
    df = ventas_raw.join(clientes_dim, on="user_id", how="left")
    resultado = df.group_by(["region", "segmento"]).agg([
        pl.sum("monto").alias("revenue"),
        pl.count("user_id").alias("transacciones"),
        pl.mean("monto").alias("ticket_promedio"),
    ])
    context.add_output_metadata({"num_filas": len(resultado)})
    return resultado


# Definir el schedule y el repositorio:
daily_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job("daily_metrics", selection=[metricas_diarias]),
    cron_schedule="0 3 * * *",
)

defs = dg.Definitions(
    assets=[ventas_raw, clientes_dim, metricas_diarias],
    schedules=[daily_schedule],
)
```

**Preguntas:**

1. ¿`context.add_output_metadata()` es equivalente a XCom de Airflow?

2. ¿Las dependencias `deps=[ventas_raw, clientes_dim]` se resuelven
   automáticamente o tienes que materializar los assets manualmente?

3. ¿`group_name` organiza los assets visualmente? ¿Tiene impacto funcional?

4. ¿Dagster ejecuta `ventas_raw` y `clientes_dim` en paralelo
   si no tienen dependencia entre sí?

5. ¿Qué pasa si `ventas_raw` falla? ¿`metricas_diarias` se salta
   o queda en estado pendiente?

---

### Ejercicio 17.3.3 — IO Managers: cómo Dagster persiste los datos

```python
import dagster as dg
import polars as pl
from pathlib import Path

# IO Managers separan la LÓGICA de la PERSISTENCIA:
# El asset produce un DataFrame, el IO Manager decide DÓNDE guardarlo.

class ParquetIOManager(dg.IOManager):
    """Persistir assets como archivos Parquet en disco o S3."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def handle_output(self, context: dg.OutputContext, obj: pl.DataFrame):
        """Guardar el output del asset."""
        path = self.base_path / f"{context.asset_key.path[-1]}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        obj.write_parquet(str(path))
        context.log.info(f"Guardado {len(obj)} filas en {path}")

    def load_input(self, context: dg.InputContext) -> pl.DataFrame:
        """Cargar el input para un asset downstream."""
        path = self.base_path / f"{context.asset_key.path[-1]}.parquet"
        return pl.read_parquet(str(path))


# Registrar el IO Manager:
defs = dg.Definitions(
    assets=[ventas_raw, clientes_dim, metricas_diarias],
    resources={
        "io_manager": ParquetIOManager(base_path="/data/dagster/assets"),
    },
)

# Ventaja: si mañana quieres guardar en S3 en vez de disco local,
# cambias el IO Manager — NO cambias los assets.
# Los assets son agnósticos al almacenamiento.

# Dagster incluye IO Managers para:
# - S3/GCS: dagster-aws, dagster-gcp
# - BigQuery: dagster-gcp
# - Snowflake: dagster-snowflake
# - DuckDB: dagster-duckdb
```

**Preguntas:**

1. ¿IO Managers son equivalentes a los hooks de Airflow?
   ¿Cuál es la diferencia conceptual?

2. ¿Un IO Manager puede manejar diferentes formatos según el tipo del asset?
   (ej: Parquet para DataFrames, JSON para dicts)

3. ¿El IO Manager sabe cuándo un asset ya fue materializado?
   ¿Puede evitar re-calcular si los datos ya existen?

4. ¿Dagster tiene un IO Manager de BigQuery production-ready?

5. ¿Si cambias el IO Manager de Parquet a S3,
   los assets downstream necesitan cambios?

---

### Ejercicio 17.3.4 — Particiones en Dagster: backfill sin dolor

```python
import dagster as dg

# Particiones: cada asset puede tener múltiples "versiones" por fecha, región, etc.
daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC",
)

@dg.asset(
    partitions_def=daily_partitions,
    group_name="raw",
)
def ventas_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    fecha = context.partition_key  # "2024-03-14"
    # Extraer ventas SOLO de esta fecha
    return extraer_ventas_de_postgres(fecha)

@dg.asset(
    partitions_def=daily_partitions,
    deps=[ventas_raw],
    group_name="analytics",
)
def metricas_diarias(context: dg.AssetExecutionContext, ventas_raw: pl.DataFrame):
    # Procesar ventas SOLO de esta partición
    return calcular_metricas(ventas_raw)

# Backfill: materializar todas las particiones de marzo
# En la UI de Dagster: seleccionar rango → "Materialize"
# → Dagster ejecuta cada partición como un job independiente

# En código:
backfill_job = dg.define_asset_job(
    "backfill_marzo",
    selection=[metricas_diarias],
    partitions_def=daily_partitions,
)
```

**Preguntas:**

1. ¿Las particiones de Dagster son equivalentes al backfill de Airflow?
   ¿Cuál es más intuitivo?

2. ¿Puedes particionar por algo que no sea fecha?
   (ej: por región, por cliente)

3. ¿Si la partición del 14 de marzo falla, las demás siguen ejecutando?

4. ¿Dagster sabe que la partición del 14 ya fue materializada
   y no la re-ejecuta en el siguiente run?

5. ¿Cómo manejas dependencias entre particiones?
   (ej: el asset de hoy depende del asset de ayer)

**Pista:** Dagster tiene `TimeWindowPartitionMapping` para expresar
dependencias entre particiones: "la partición del día 15 depende de
las particiones del día 13, 14, y 15 del asset upstream".
Esto permite cálculos tipo rolling window donde cada partición
necesita datos de los N días anteriores.

---

### Ejercicio 17.3.5 — Implementar: migrar un DAG de Airflow a Dagster

**Tipo: Implementar**

```python
# Dado este DAG de Airflow:
# extraer_ventas >> validar >> transformar >> cargar >> notificar

# Reimplementar en Dagster como assets:
# - ventas_raw (asset, grupo "raw")
# - ventas_validadas (asset, grupo "staging", con quality checks)
# - metricas_diarias (asset, grupo "analytics")
# - reporte_email (asset, grupo "reporting", envía email)

# Requisitos:
# 1. Particiones diarias
# 2. IO Manager para Parquet local (desarrollo) y S3 (producción)
# 3. Quality checks: mínimo 100 registros, no nulls en columnas clave
# 4. Metadata en cada asset (num_registros, columnas, tamaño en bytes)
```

**Restricciones:**
1. Implementar los 4 assets con dependencias explícitas
2. Implementar el IO Manager con soporte local y S3
3. Incluir Dagster asset checks para quality gates
4. Comparar la cantidad de código con el DAG de Airflow equivalente

---

## Sección 17.4 — Prefect: Orquestación como Código Python

### Ejercicio 17.4.1 — Leer: la filosofía de Prefect

**Tipo: Leer**

```
Prefect parte de una premisa distinta:
  "Si ya sabes escribir funciones Python, ya sabes usar Prefect."

Airflow:   DAGs en Python, pero con un framework pesado que requiere
           instalar un webserver, scheduler, metadata DB, executor.
           Tu código se adapta a Airflow.

Dagster:   Assets declarativos con un framework que gestiona el ciclo
           de vida de los datos. Tu código se adapta a Dagster.

Prefect:   Decoradores sobre funciones Python normales.
           Prefect se adapta a tu código.

  @flow
  def mi_pipeline():
      datos = extraer()
      resultado = transformar(datos)
      cargar(resultado)

  # Eso es un pipeline de Prefect. Puedes ejecutarlo como:
  # 1. Script normal: python mi_pipeline.py
  # 2. Con Prefect server: prefect deployment run mi_pipeline
  # 3. En la nube: Prefect Cloud

Ventajas:
  - La curva de aprendizaje más baja de los tres
  - Funciona sin servidor (para desarrollo y scripts simples)
  - Manejo de errores natural (try/except de Python)
  - Logging automático de cada paso

Desventajas:
  - Ecosistema más pequeño que Airflow
  - Sin concepto nativo de assets (como Dagster)
  - Menos integraciones pre-construidas
  - Prefect Cloud es el modelo de negocio → features premium en la nube
```

**Preguntas:**

1. ¿Prefect sin servidor es viable para producción?

2. ¿Los decoradores `@flow` y `@task` de Prefect tienen limitaciones
   respecto a las funciones Python normales?

3. ¿Prefect puede orquestar tareas en Kubernetes como Airflow?

4. ¿Prefect 2.x vs Prefect 3.x — qué cambió?

5. ¿Para un equipo que ya usa Airflow, ¿hay razón para migrar a Prefect?

---

### Ejercicio 17.4.2 — El mismo pipeline en Prefect

```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import polars as pl

@task(
    retries=3,
    retry_delay_seconds=[60, 300, 900],  # backoff: 1m, 5m, 15m
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    log_prints=True,
)
def extraer_ventas(fecha: str) -> pl.DataFrame:
    """Extraer ventas de PostgreSQL."""
    print(f"Extrayendo ventas del {fecha}")
    # Simular:
    return pl.DataFrame({
        "user_id": ["alice", "bob", "carol"],
        "monto": [150.0, 75.0, 300.0],
        "region": ["norte", "sur", "norte"],
    })

@task(retries=2)
def extraer_clientes() -> pl.DataFrame:
    return pl.DataFrame({
        "user_id": ["alice", "bob", "carol"],
        "segmento": ["premium", "standard", "premium"],
    })

@task()
def validar(df: pl.DataFrame, min_registros: int = 100) -> pl.DataFrame:
    """Quality gate."""
    if len(df) < min_registros:
        raise ValueError(f"Solo {len(df)} registros (mínimo: {min_registros})")
    return df

@task()
def transformar(ventas: pl.DataFrame, clientes: pl.DataFrame) -> pl.DataFrame:
    return ventas.join(clientes, on="user_id", how="left") \
        .group_by(["region", "segmento"]) \
        .agg(pl.sum("monto").alias("revenue"))

@flow(
    name="ecommerce-daily-metrics",
    description="Pipeline diario de métricas de e-commerce",
    retries=1,
    log_prints=True,
)
def pipeline_ecommerce(fecha: str):
    """Pipeline completo — ejecutar como función Python normal."""
    ventas = extraer_ventas(fecha)
    clientes = extraer_clientes()
    ventas_ok = validar(ventas, min_registros=1)  # min=1 para test
    resultado = transformar(ventas_ok, clientes)
    print(f"Resultado: {resultado}")
    return resultado

# Ejecutar directamente:
if __name__ == "__main__":
    pipeline_ecommerce("2024-03-14")
```

**Preguntas:**

1. ¿`cache_key_fn=task_input_hash` es equivalente a la idempotencia?
   ¿O es solo memoización?

2. ¿`retry_delay_seconds=[60, 300, 900]` es un backoff manual.
   ¿Prefect tiene backoff exponencial automático?

3. ¿Un `@flow` puede llamar a otro `@flow`? ¿Cómo se comportan los reintentos?

4. ¿Puedes ejecutar `pipeline_ecommerce()` sin tener Prefect server corriendo?

5. ¿Cómo programas este flow para que ejecute diariamente a las 3:00 UTC
   en Prefect?

---

### Ejercicio 17.4.3 — Concurrencia en Prefect: tasks paralelas

```python
from prefect import flow, task
from prefect.futures import wait
import polars as pl
import time

@task()
def procesar_region(region: str) -> dict:
    """Procesar una región — tarea independiente por región."""
    time.sleep(2)  # simular procesamiento
    return {"region": region, "revenue": 1000.0 * hash(region) % 100}

@flow()
def pipeline_paralelo():
    regiones = ["norte", "sur", "este", "oeste", "centro"]

    # Lanzar todas las tareas en paralelo con .submit():
    futures = [procesar_region.submit(r) for r in regiones]

    # Esperar a que todas completen:
    resultados = [f.result() for f in futures]

    # O usar wait() para control fino:
    # completados, pendientes = wait(futures, timeout=60)

    print(f"Procesadas {len(resultados)} regiones")
    return resultados

@flow()
def pipeline_con_map():
    """Alternativa: .map() ejecuta la tarea para cada input."""
    regiones = ["norte", "sur", "este", "oeste", "centro"]
    resultados = procesar_region.map(regiones)
    return [r.result() for r in resultados]
```

**Preguntas:**

1. ¿`.submit()` usa threads, procesos, o algo más?
   ¿Cómo se configura el executor en Prefect?

2. ¿`.map()` de Prefect es equivalente a `.expand()` de Airflow?

3. ¿Prefect puede distribuir tasks entre múltiples workers?
   ¿O todo corre en el mismo proceso?

4. ¿Si una de las 5 regiones falla, las demás siguen ejecutando?

5. ¿El paralelismo de Prefect tiene límite?
   ¿Puedes lanzar 10,000 tasks concurrentes?

---

### Ejercicio 17.4.4 — Deployments: de script a servicio

```python
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret

# Deployment: convertir un flow en un servicio programado

# Opción 1: desde código (Prefect 3.x)
if __name__ == "__main__":
    pipeline_ecommerce.serve(
        name="ecommerce-daily",
        cron="0 3 * * *",
        tags=["ecommerce", "daily"],
        parameters={"fecha": "{{ now | format_datetime('%Y-%m-%d') }}"},
    )

# Opción 2: con Docker
# pipeline_ecommerce.deploy(
#     name="ecommerce-daily-docker",
#     work_pool_name="docker-pool",
#     image="mi-registry/pipeline:latest",
#     cron="0 3 * * *",
# )

# Opción 3: con Kubernetes
# pipeline_ecommerce.deploy(
#     name="ecommerce-daily-k8s",
#     work_pool_name="k8s-pool",
#     cron="0 3 * * *",
#     job_variables={"cpu": "2", "memory": "4Gi"},
# )
```

**Preguntas:**

1. ¿`.serve()` es equivalente a correr un scheduler local?
   ¿Es production-ready?

2. ¿`.deploy()` con Docker crea un container por cada run?

3. ¿Los "work pools" de Prefect son equivalentes a los executors de Airflow?

4. ¿Cómo manejas secretos (passwords, API keys) en Prefect?

5. ¿Prefect Cloud vs Prefect self-hosted — cuál para producción?

---

### Ejercicio 17.4.5 — Comparar: el mismo pipeline en Airflow, Dagster, y Prefect

**Tipo: Comparar**

```
Pipeline: extraer → validar → transformar → cargar
para el sistema de e-commerce del libro.

Criterios de comparación:
  1. Líneas de código
  2. Complejidad de setup (instalar + configurar)
  3. Testing (¿cómo testeas el pipeline?)
  4. Debugging (¿cómo diagnosticas un fallo?)
  5. Backfill (¿cómo re-ejecutas el mes de marzo?)
  6. Escalabilidad (¿cómo escala a 100 DAGs/flows/assets?)
  7. Data lineage (¿puedes rastrear de dónde vienen los datos?)
```

**Restricciones:**
1. Implementar el pipeline completo en los tres frameworks
2. Llenar la tabla de comparación con datos concretos
3. Identificar cuándo cada framework es la mejor opción
4. ¿Hay un cuarto framework que vale la pena considerar? (Mage, Kestra, etc.)

---

## Sección 17.5 — Patrones de Producción: Backfill, Reintentos, SLAs

### Ejercicio 17.5.1 — Backfill: re-procesar datos históricos

```python
# Backfill: ejecutar el pipeline para fechas pasadas.
# Escenarios comunes:
# 1. Bug en la transformación → fix → re-ejecutar los últimos 30 días
# 2. Nueva columna añadida → recalcular todo el histórico
# 3. Nuevo cliente → procesar sus datos retroactivamente
# 4. Migración de sistema → re-ingestar todo desde la fuente

# En Airflow:
# CLI: airflow dags backfill -s 2024-03-01 -e 2024-03-31 ecommerce_daily_metrics
# UI: Clear → seleccionar rango de fechas → Clear

# PELIGRO: si el pipeline no es idempotente, el backfill DUPLICA datos.

# Patrón seguro de backfill en Airflow:
def cargar_idempotente(**context):
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")

    # Paso 1: borrar datos existentes de la partición
    bq_client.query(f"""
        DELETE FROM `proyecto.dataset.metricas_diarias`
        WHERE fecha = '{fecha}'
    """).result()

    # Paso 2: insertar datos nuevos
    bq_client.query(f"""
        INSERT INTO `proyecto.dataset.metricas_diarias`
        SELECT ...
        FROM `proyecto.dataset.ventas_raw`
        WHERE fecha = '{fecha}'
    """).result()

    # Si ejecutas dos veces, el resultado es el mismo:
    # DELETE borra lo que insertó la primera vez,
    # INSERT inserta los mismos datos.

# Patrón con Delta Lake (MERGE):
def cargar_con_merge(**context):
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")
    spark.sql(f"""
        MERGE INTO metricas_diarias target
        USING staging source
        ON target.fecha = source.fecha AND target.region = source.region
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

**Preguntas:**

1. ¿Backfill con `catchup=True` en Airflow genera un DagRun por cada día?
   ¿Cuántos DagRuns se crean para 365 días?

2. ¿El backfill respeta `max_active_runs`?
   ¿Ejecuta todos los días en paralelo?

3. ¿El patrón DELETE + INSERT es atómico en BigQuery?
   ¿Qué pasa si falla entre el DELETE y el INSERT?

4. ¿MERGE en Delta Lake es mejor que DELETE + INSERT para backfill?

5. ¿Dagster facilita el backfill respecto a Airflow?
   ¿Cómo seleccionas particiones para re-materializar?

---

### Ejercicio 17.5.2 — Reintentos inteligentes: no todos los errores son iguales

```python
from airflow.decorators import task
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

# Errores transitorios (reintentar tiene sentido):
# - Timeout de red
# - 429 Too Many Requests
# - 503 Service Unavailable
# - Connection refused (servicio reiniciando)

# Errores permanentes (reintentar NO tiene sentido):
# - 404 Not Found
# - Schema inválido
# - Permiso denegado
# - Datos corruptos

@task(
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
)
def extraer_de_api(**context):
    """Extraer datos de una API — con reintentos inteligentes."""
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")

    try:
        response = requests.get(
            f"https://api.empresa.com/ventas?fecha={fecha}",
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as e:
        if e.response.status_code in (429, 503):
            raise  # Airflow reintentará (error transitorio)
        elif e.response.status_code == 404:
            # No reintentar — los datos no existen
            context["task_instance"].log.warning(f"No hay datos para {fecha}")
            return {"registros": 0}
        else:
            raise  # Otros errores → reintentar por defecto


# Patrón avanzado con tenacity (dentro de la tarea):
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
)
def llamar_api_con_retry(url: str) -> dict:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
```

**Preguntas:**

1. ¿Los reintentos de Airflow y los reintentos de `tenacity` se acumulan?
   Si Airflow reintenta 3 veces y tenacity 5 veces, ¿son 15 intentos totales?

2. ¿Exponential backoff con jitter es mejor que exponential backoff puro?
   ¿Por qué?

3. ¿Cómo distingues un error transitorio de uno permanente
   cuando la API no retorna códigos HTTP claros?

4. ¿Reintentar una tarea de Spark que falló por OutOfMemoryError tiene sentido?

5. ¿Cuántos reintentos son "demasiados"?
   ¿Cuál es el criterio para pasar de "reintentar" a "alertar y parar"?

---

### Ejercicio 17.5.3 — SLAs y alertas: saber antes de que el VP pregunte

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# SLA: Service Level Agreement
# "El pipeline de métricas diarias debe completar antes de las 6:00 UTC"

def alerta_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback cuando se incumple el SLA."""
    mensaje = f"SLA MISS: {dag.dag_id} no completó a tiempo.\n"
    mensaje += f"Tareas bloqueadas: {[t.task_id for t in blocking_task_list]}"
    enviar_slack(canal="#data-alerts", mensaje=mensaje)
    enviar_pagerduty(severity="high", mensaje=mensaje)

with DAG(
    dag_id="ecommerce_con_sla",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    sla_miss_callback=alerta_sla_miss,
) as dag:

    extraer = PythonOperator(
        task_id="extraer",
        python_callable=extraer_ventas,
        sla=timedelta(hours=1),  # debe completar en máximo 1 hora
    )

    transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar_metricas,
        sla=timedelta(hours=2),  # máximo 2 horas desde el inicio del DAG
    )

    cargar = PythonOperator(
        task_id="cargar",
        python_callable=cargar_a_bigquery,
        sla=timedelta(hours=3),  # todo debe estar listo en 3 horas
    )

# Alertas adicionales que NO son SLA:
# 1. Callback on_failure: se ejecuta cuando una tarea falla
# 2. Callback on_success: se ejecuta cuando una tarea completa
# 3. Callback on_retry: se ejecuta en cada reintento
def alerta_fallo(context):
    task = context["task_instance"]
    enviar_slack(
        canal="#data-alerts",
        mensaje=f"❌ {task.dag_id}.{task.task_id} falló: {context['exception']}"
    )

extraer = PythonOperator(
    task_id="extraer",
    python_callable=extraer_ventas,
    on_failure_callback=alerta_fallo,
)
```

**Preguntas:**

1. ¿SLA en Airflow mide el tiempo desde el `data_interval_start`
   o desde el momento en que la tarea empieza a ejecutar?

2. ¿Dagster y Prefect tienen SLAs equivalentes?

3. ¿Alertar por Slack vs PagerDuty — cuándo cada uno?

4. ¿Un pipeline que siempre completa en 25 minutos pero un día tarda 26
   debería generar una alerta?

5. ¿Cómo defines SLAs razonables para un pipeline nuevo
   del que no tienes datos históricos?

---

### Ejercicio 17.5.4 — Circuit breakers y dependency checks

```python
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException

# Patrón: verificar dependencias externas antes de ejecutar

# 1. ExternalTaskSensor: esperar a que otro DAG complete
esperar_ingesta = ExternalTaskSensor(
    task_id="esperar_ingesta_raw",
    external_dag_id="ingesta_ventas_raw",
    external_task_id="cargar_a_s3",
    timeout=7200,  # esperar máximo 2 horas
    mode="reschedule",
)

# 2. Circuit breaker: verificar la salud del sistema antes de ejecutar
@task()
def verificar_salud():
    """No ejecutar si el sistema downstream está caído."""
    import requests
    try:
        r = requests.get("https://bigquery.googleapis.com/health", timeout=5)
        if r.status_code != 200:
            raise AirflowSkipException("BigQuery no disponible — skip pipeline")
    except requests.Timeout:
        raise AirflowSkipException("BigQuery timeout — skip pipeline")

# 3. Data quality gate: verificar los datos ANTES de procesarlos
@task()
def quality_gate(**context):
    """Verificar que los datos de entrada cumplen el mínimo."""
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")
    count = bq_client.query(f"""
        SELECT COUNT(*) as n FROM `raw.ventas` WHERE fecha = '{fecha}'
    """).result().to_dataframe()["n"][0]

    if count < 1000:
        raise ValueError(f"Solo {count} ventas para {fecha} — mínimo esperado: 1000")
    if count > 10_000_000:
        raise ValueError(f"{count} ventas para {fecha} — posible duplicación")

    return {"count": count, "fecha": fecha}
```

**Preguntas:**

1. ¿`ExternalTaskSensor` crea un acoplamiento fuerte entre DAGs?
   ¿Cómo lo reduces?

2. ¿`AirflowSkipException` es mejor que fallar la tarea?
   ¿Cuándo skip y cuándo fail?

3. ¿Un quality gate que verifica mínimos y máximos es suficiente?
   ¿Qué otras validaciones son útiles?

4. ¿Airflow Datasets (2.4+) reemplazan al ExternalTaskSensor?

5. ¿En Dagster, las dependencias entre assets eliminan la necesidad
   de ExternalTaskSensor?

---

### Ejercicio 17.5.5 — Implementar: sistema de alertas multi-nivel

**Tipo: Implementar**

```python
# Diseñar un sistema de alertas con tres niveles:

# Nivel 1 — INFO (Slack #data-alerts):
#   - Pipeline completado exitosamente
#   - Backfill completado
#   - Tarea reintentada (1er reintento)

# Nivel 2 — WARNING (Slack #data-alerts + email):
#   - SLA miss (pipeline no completó a tiempo)
#   - Quality gate con valores borderline
#   - Tarea reintentada (2do reintento)
#   - Pipeline tarda 2× más de lo normal

# Nivel 3 — CRITICAL (PagerDuty + Slack #incidents):
#   - Pipeline falla después de todos los reintentos
#   - Revenue = $0 (imposible en producción)
#   - Datos del día anterior no disponibles para el negocio
#   - Tarea bloqueada por más de 1 hora en "queued"

# Requisitos:
# 1. Callback genérico que determine el nivel basado en el contexto
# 2. Deduplicación (no enviar 100 alertas por el mismo incidente)
# 3. Escalamiento automático (si Level 2 no se resuelve en 30 min → Level 3)
```

**Restricciones:**
1. Implementar los callbacks de Airflow para los tres niveles
2. Incluir deduplicación basada en DAG + fecha + tipo de error
3. Implementar escalamiento automático con un DAG sensor
4. ¿Cómo testeas el sistema de alertas sin generar alertas reales?

---

## Sección 17.6 — Orquestación de Streaming y Pipelines Híbridos

### Ejercicio 17.6.1 — Leer: ¿se orquesta un pipeline de streaming?

**Tipo: Leer**

```
Batch pipelines se orquestan naturalmente:
  "ejecutar A, luego B, luego C, una vez al día"

Streaming pipelines son diferentes:
  "Flink job que corre 24/7, procesa eventos en tiempo real"

¿Qué hay que orquestar en streaming?

  1. Deployment: desplegar o actualizar el job de Flink/Kafka Streams
  2. Health monitoring: verificar que el job está corriendo y procesando
  3. Savepoint/Checkpoint: tomar savepoints antes de actualizaciones
  4. Recovery: reiniciar el job desde un savepoint si falla
  5. Scaling: ajustar el parallelism según la carga

  Lo que NO se orquesta:
  - La ejecución de cada evento (eso lo hace Flink internamente)
  - El windowing y las agregaciones (eso es lógica del framework)
  - El exactly-once (eso son checkpoints de Flink)

¿Qué herramienta para qué?

  Orquestación batch:       Airflow / Dagster / Prefect
  Despliegue streaming:     Kubernetes (FlinkDeployment CRD), CI/CD
  Monitoreo streaming:      Prometheus + Grafana, datadog
  Gestión de Flink jobs:    Flink REST API, Ververica Platform

  El error más común:
  Intentar usar Airflow para orquestar un job de Flink evento por evento.
  Airflow no está diseñado para esto. El job de Flink se despliega UNA VEZ
  y corre continuamente. Airflow puede gestionar el DESPLIEGUE,
  no la ejecución.
```

**Preguntas:**

1. ¿Un Airflow Sensor que verifica cada 5 minutos
   si el Flink job está corriendo es un buen patrón?

2. ¿Kubernetes Operator de Airflow puede desplegar un job de Flink?

3. ¿Dagster tiene soporte nativo para streaming?

4. ¿El patrón "Lambda Architecture" (batch + streaming) necesita
   un orquestador para ambos? ¿Cómo se coordinan?

5. ¿Un micro-batch de Spark Structured Streaming
   se orquesta como batch o como streaming?

---

### Ejercicio 17.6.2 — Pipelines híbridos: batch que alimenta streaming y viceversa

```python
# Patrón común: pipeline híbrido de e-commerce
#
# Streaming (24/7):
#   Kafka → Flink → métricas_realtime (BigQuery)
#   (clicks, compras, eventos — procesados en segundos)
#
# Batch (diario, 3:00 UTC):
#   PostgreSQL → Spark → metricas_diarias (BigQuery)
#   (dimensiones, reconciliación, reportes completos)
#
# El batch necesita los datos del streaming:
#   - metricas_realtime como input para reconciliación
#   - Verificar que streaming y batch coinciden
#
# El streaming necesita datos del batch:
#   - Tabla de clientes (actualizada diariamente) como lookup table
#   - Reglas de fraude (actualizadas semanalmente)

# DAG de Airflow que coordina el pipeline híbrido:
from airflow import DAG
from airflow.providers.apache.flink.sensors.flink import FlinkJobStatusSensor

with DAG("ecommerce_hibrido", schedule="0 3 * * *") as dag:

    # Verificar que el streaming está corriendo y sano:
    verificar_flink = FlinkJobStatusSensor(
        task_id="verificar_flink_corriendo",
        job_id="{{ var.value.flink_job_id }}",
        target_status="RUNNING",
        timeout=300,
    )

    # Ejecutar batch:
    batch_metricas = PythonOperator(
        task_id="batch_metricas_diarias",
        python_callable=ejecutar_spark_batch,
    )

    # Reconciliar batch vs streaming:
    reconciliar = PythonOperator(
        task_id="reconciliar_batch_streaming",
        python_callable=reconciliar_metricas,
    )

    # Actualizar lookup table del streaming:
    actualizar_lookup = PythonOperator(
        task_id="actualizar_lookup_clientes",
        python_callable=publicar_tabla_clientes_a_kafka,
    )

    verificar_flink >> batch_metricas >> reconciliar
    batch_metricas >> actualizar_lookup
```

**Preguntas:**

1. ¿Reconciliar batch y streaming es necesario?
   ¿Cuánta discrepancia es aceptable?

2. ¿La lookup table publicada a Kafka — es un KTable de Kafka Streams
   o un GlobalKTable?

3. ¿Si el Flink job se cae a las 2:00 UTC y el batch inicia a las 3:00 UTC,
   el DAG debe esperar a que Flink se recupere?

4. ¿El patrón de "streaming para real-time, batch para corrección"
   es el estándar en 2024?

5. ¿Kappa Architecture (solo streaming, sin batch) elimina
   la necesidad de orquestación batch?

---

### Ejercicio 17.6.3 — Orquestar dbt con Airflow y Dagster

```python
# dbt (data build tool) es el estándar para transformaciones SQL.
# ¿Cómo se integra con los orquestadores?

# Opción 1: Airflow + dbt (via BashOperator o DbtCloudRunJobOperator)
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/dbt && dbt run --select tag:daily",
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/dbt && dbt test --select tag:daily",
)

extraer >> dbt_run >> dbt_test >> notificar

# Opción 2: Dagster + dbt (integración nativa)
# Dagster trata cada modelo dbt como un asset de Dagster.
from dagster_dbt import DbtCliResource, dbt_assets

@dbt_assets(manifest=Path("target/manifest.json"))
def mis_modelos_dbt(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Cada modelo dbt aparece como un asset en la UI de Dagster,
# con lineage, particiones, y metadata automáticas.

# Opción 3: Prefect + dbt
from prefect_dbt.cli.commands import DbtCoreOperation

@task
def ejecutar_dbt():
    DbtCoreOperation(
        commands=["dbt run --select tag:daily"],
        project_dir="/opt/dbt",
    ).run()
```

**Preguntas:**

1. ¿Dagster + dbt es la integración más madura de las tres?

2. ¿Airflow con `cosmos` (astronomer-cosmos) mejora la integración con dbt?

3. ¿dbt debe ejecutar dentro del orquestador o en un servicio separado?

4. ¿Cada modelo dbt como tarea de Airflow genera demasiadas tareas?

5. ¿dbt Cloud necesita un orquestador externo?

---

### Ejercicio 17.6.4 — Airflow Datasets: event-driven orchestration

```python
from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

# Airflow 2.4+ introduce Datasets: orquestación basada en eventos.
# En lugar de "ejecutar B a las 3:00", es "ejecutar B cuando A produce datos nuevos".

# Dataset = URI que representa un conjunto de datos
ventas_dataset = Dataset("s3://data-lake/raw/ventas/")
metricas_dataset = Dataset("s3://data-lake/analytics/metricas/")

# DAG productor: genera datos y notifica al dataset
with DAG(
    dag_id="productor_ventas",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
) as dag_productor:

    @task(outlets=[ventas_dataset])
    def extraer_y_guardar():
        # ... extraer ventas y guardar en S3
        pass  # Airflow marca ventas_dataset como "actualizado"


# DAG consumidor: se dispara cuando el dataset se actualiza
with DAG(
    dag_id="consumidor_metricas",
    schedule=[ventas_dataset],  # se dispara por el dataset, NO por cron
    start_date=datetime(2024, 1, 1),
) as dag_consumidor:

    @task()
    def calcular_metricas():
        # Se ejecuta automáticamente cuando ventas_dataset se actualiza
        pass

# Ventaja: el consumidor no sabe cuándo ejecuta el productor.
# Si el productor cambia de schedule, el consumidor se adapta automáticamente.
```

**Preguntas:**

1. ¿Datasets de Airflow es la respuesta de Airflow al modelo de assets de Dagster?

2. ¿Un DAG puede depender de múltiples datasets (AND lógico)?

3. ¿Datasets funciona entre DAGs de distintos Airflow deployments?

4. ¿El Dataset verifica realmente si los datos cambiaron en S3?
   ¿O solo sabe que la tarea con `outlets` completó?

5. ¿Datasets elimina la necesidad de ExternalTaskSensor?

---

### Ejercicio 17.6.5 — Implementar: orquestación del pipeline de ML

**Tipo: Implementar**

```python
# Diseñar la orquestación de un pipeline de ML para el e-commerce:
#
# 1. Feature engineering (diario):
#    ventas_raw + clientes → features table
#
# 2. Model training (semanal):
#    features table → entrenar modelo → registrar en MLflow
#
# 3. Model validation (automático después de training):
#    modelo nuevo vs modelo en producción → métricas de comparison
#
# 4. Model deployment (manual, con aprobación):
#    si validation pasa → promover modelo a producción
#
# 5. Inference (batch diario):
#    datos nuevos + modelo en producción → predicciones
#
# 6. Monitoring (diario):
#    predicciones vs actuals → detectar data drift

# Decidir: ¿un DAG o múltiples DAGs?
# ¿Airflow, Dagster, o Prefect para ML?
# ¿Cómo se integra con MLflow/Weights&Biases?
```

**Restricciones:**
1. Diseñar los DAGs/assets/flows con dependencias claras
2. El training no debe bloquear la inference diaria
3. El deployment manual debe requerir aprobación humana
4. Incluir rollback automático si el modelo nuevo performa peor

---

## Sección 17.7 — El E-commerce Orquestado: Integrando Todo

### Ejercicio 17.7.1 — Leer: la arquitectura de orquestación del sistema completo

**Tipo: Leer**

```
El sistema de e-commerce (el hilo conductor del libro) orquestado:

  ┌─────────────────────────────────────────────────────────┐
  │                 FUENTES DE DATOS                         │
  │  PostgreSQL (ventas)   Kafka (eventos)   API (inventario)│
  └──────┬─────────────────────┬─────────────────┬──────────┘
         │                     │                 │
  ┌──────▼─────────────────────▼─────────────────▼──────────┐
  │              INGESTA (Airflow DAGs)                       │
  │  extraer_ventas    ingestar_eventos    sync_inventario    │
  │  (diario, 3am)     (streaming, Flink)  (cada 6 horas)    │
  └──────┬─────────────────────┬─────────────────┬──────────┘
         │                     │                 │
  ┌──────▼─────────────────────▼─────────────────▼──────────┐
  │              DATA LAKE (S3 / GCS)                        │
  │  raw/ventas/    raw/eventos/    raw/inventario/           │
  │  (Parquet)      (Parquet)       (Parquet)                │
  └──────┬─────────────────────┬─────────────────┬──────────┘
         │                     │                 │
  ┌──────▼─────────────────────▼─────────────────▼──────────┐
  │           TRANSFORMACIÓN (dbt + Spark, orquestado)       │
  │  staging → intermediate → marts                          │
  │  (Dagster assets o Airflow + cosmos)                     │
  └──────┬─────────────────────────────────────────┬────────┘
         │                                         │
  ┌──────▼────────────┐               ┌────────────▼────────┐
  │  ANALYTICS         │               │  ML PIPELINE         │
  │  métricas diarias  │               │  features → training │
  │  revenue dashboard │               │  → inference          │
  │  (BigQuery/DWH)    │               │  (MLflow + Airflow)   │
  └────────────────────┘               └─────────────────────┘

  Orquestadores:
  - Airflow: ingesta batch, coordinación de Flink, dbt runs
  - Dagster: transformación SQL (assets), lineage
  - Kubernetes: deployment de Flink, scaling
  - CI/CD: deployment de dbt models, Spark jobs
```

**Preguntas:**

1. ¿Usar Airflow Y Dagster en el mismo stack es buena práctica
   o complejidad innecesaria?

2. ¿Qué orquesta el deploy del job de Flink?
   ¿Airflow o Kubernetes directamente?

3. ¿El data lineage en esta arquitectura es end-to-end
   o fragmentado por herramienta?

4. ¿Cuántos DAGs tendría un sistema real como este?
   ¿10? ¿50? ¿100+?

5. ¿Un solo orquestador puede manejar todo?
   ¿O es mejor separar por dominio?

---

### Ejercicio 17.7.2 — Diseñar: la estrategia de deployment de DAGs

```
Problema: tienes 50 DAGs de Airflow en producción.
¿Cómo gestionas cambios, tests, y deploys?

  Opción 1: Git monorepo + CI/CD
    - Todos los DAGs en un solo repo
    - CI: lint (ruff), test (pytest), DAG validation
    - CD: sync al dag_folder (S3/GCS/NFS)
    - Ventaja: simple, un solo lugar para buscar
    - Riesgo: un DAG roto rompe el import de todos

  Opción 2: DAGs como paquetes Python
    - Cada DAG (o grupo de DAGs) es un package pip
    - CI/CD instala los packages en el Airflow environment
    - Ventaja: versionado, dependencias explícitas
    - Riesgo: complejidad de packaging, conflictos de dependencias

  Opción 3: Dagster (opinionated)
    - Dagster tiene una CLI de deployment integrada
    - Cada "code location" es un módulo Python
    - El Dagster webserver descubre assets automáticamente
    - Ventaja: built-in deployment, no necesitas CI/CD custom
    - Riesgo: lock-in a Dagster
```

**Preguntas:**

1. ¿Cómo testeas un DAG de Airflow antes de desplegarlo?

2. ¿`dag_bag.process_file()` es suficiente como test?
   ¿O necesitas ejecutar las tareas en un entorno de staging?

3. ¿Canary deployments aplican a DAGs?
   (ej: desplegar el cambio solo para una fecha y verificar)

4. ¿Helm charts para Airflow en Kubernetes simplifican el deployment?

5. ¿Dagster CLI vs Airflow + CI/CD — cuál es más robusto?

---

### Ejercicio 17.7.3 — Implementar: DAG maestro del e-commerce

**Tipo: Implementar**

```python
# Implementar el DAG maestro que coordina todo el sistema de e-commerce.
# Este DAG no procesa datos — coordina los pipelines que sí lo hacen.

# Estructura:
# 1. Verificar salud de las fuentes (PostgreSQL, Kafka, API)
# 2. Disparar ingesta batch (o verificar que el streaming está OK)
# 3. Disparar dbt transformations
# 4. Verificar quality gates en las tablas resultantes
# 5. Disparar inference de ML
# 6. Generar reportes y notificar
# 7. Cleanup: archivar logs, borrar staging tables

# Requisitos:
# - SLA: completar antes de las 7:00 UTC
# - Alertas multi-nivel (Slack + PagerDuty)
# - Idempotente (safe to re-run)
# - Backfill-friendly (safe to run para fechas pasadas)
```

**Restricciones:**
1. Implementar el DAG completo con al menos 10 tareas
2. Incluir branching condicional (si es lunes → generar reporte semanal)
3. Incluir SLAs por tarea y por DAG
4. Implementar un dashboard de estado (qué pipelines completaron hoy)

---

### Ejercicio 17.7.4 — El costo de la orquestación: cuándo es demasiado

**Tipo: Analizar**

```
Señales de que tu orquestación es demasiado compleja:

  1. Más tiempo debuggeando Airflow que los pipelines mismos
  2. DAGs con 100+ tareas que tardan 10 minutos solo en parsearse
  3. Dependency spaghetti: 20 DAGs con ExternalTaskSensors cruzados
  4. "Nobody touches the Airflow config" → tribal knowledge
  5. El scheduler es un SPOF y nadie sabe cómo hacer failover

Señales de que te falta orquestación:

  1. "El pipeline de ayer no corrió y nadie se enteró"
  2. Backfill manual con scripts ad-hoc que a veces duplican datos
  3. Documentación en Confluence sobre el orden de ejecución de los scripts
  4. cron + emails como sistema de alertas
  5. "Funciona si lo ejecutas en orden — no lo ejecutes fuera de orden"

El punto óptimo:
  Orquestar lo que necesita orquestación.
  Un script que corre una vez al mes y tarda 5 minutos
  no necesita Airflow — necesita un cron y un healthcheck.
  Un pipeline con 15 dependencias que corre diariamente
  y del que dependen 3 equipos — eso SÍ necesita un orquestador.
```

**Preguntas:**

1. ¿Cuántos DAGs puede manejar un Airflow deployment antes de degradarse?

2. ¿Dagster escala mejor que Airflow para muchos assets?

3. ¿"Serverless orchestration" (ej: AWS Step Functions)
   es una alternativa a Airflow para pipelines simples?

4. ¿Temporal (orquestador de workflows general) compite con Airflow
   para data engineering?

5. ¿El futuro de la orquestación es event-driven (Datasets, assets)
   en lugar de schedule-driven (cron)?

---

### Ejercicio 17.7.5 — El ecosistema fit: orquestación en el stack de 2024

**Tipo: Analizar**

```
Estado del ecosistema en 2024:

  Airflow:
  - Estándar de facto. 10 años de madurez.
  - Managed: MWAA (AWS), Cloud Composer (GCP), Astronomer.
  - Limitaciones: UI anticuada, parseo lento, XCom limitado.
  - Futuro: Airflow 3.x (mejor UI, mejor scheduler, assets mejorados).

  Dagster:
  - El challenger más serio. Modelo de assets es el futuro.
  - Managed: Dagster Cloud.
  - Limitaciones: ecosistema más pequeño, documentación en evolución.
  - Futuro: más integraciones, Dagster+ (enterprise features).

  Prefect:
  - La opción "ligera" y Pythonic.
  - Managed: Prefect Cloud.
  - Limitaciones: modelo de negocio basado en cloud, features free limitadas.
  - Futuro: Prefect 3.x (simplificación, más open-source).

  Otros:
  - Mage: orientado a data engineering, UI visual, joven.
  - Kestra: declarativo (YAML), multi-lenguaje, open-source.
  - Temporal: orquestador general (no específico de datos), potente.
  - AWS Step Functions: serverless, integrado con AWS, limitado fuera de AWS.

Recomendación para un equipo nuevo en 2024:
  ¿Ya tienes Airflow?        → Quédate, mejora, actualiza a 2.8+.
  ¿Empezando desde cero?     → Evalúa Dagster seriamente.
  ¿Pipeline simple/scripts?  → Prefect o incluso cron + alertas.
  ¿100% AWS?                 → Step Functions + MWAA para lo complejo.
```

**Preguntas:**

1. ¿Airflow será reemplazado en 5 años?
   ¿Por quién?

2. ¿El modelo de assets de Dagster se convertirá en el estándar?

3. ¿Un equipo de 3 data engineers necesita Airflow
   o es overkill?

4. ¿Managed Airflow (MWAA, Composer) elimina la complejidad operativa?

5. ¿Para el sistema de e-commerce del libro, ¿cuál es la recomendación final?

---

## Resumen del capítulo

**Orquestación: las tres capas**

```
Capa 1: Scheduling
  Cuándo ejecutar: cron expression, data_interval, event-driven.
  → Airflow schedule, Dagster schedule, Prefect cron, Datasets/Assets

Capa 2: Dependency management
  En qué orden: DAG de tareas, grafo de assets, dependencias de datos.
  → Airflow operators, Dagster deps, Prefect task dependencies
  → ExternalTaskSensor, Datasets, asset dependencies

Capa 3: Operación
  Qué hacer si falla: reintentos, alertas, SLAs, backfill.
  → Retries, callbacks, SLA miss, quality gates, circuit breakers
  → Esto es lo que diferencia a un orquestador de un cron.
```

**El principio de la orquestación:**

```
Un pipeline de datos tiene valor solo si ejecuta
de forma confiable, predecible, y observable.

  Confiable:   si falla, se recupera automáticamente
               o alerta antes de que el negocio se entere.

  Predecible:  ejecuta a la misma hora, produce los mismos resultados
               para los mismos inputs, es idempotente.

  Observable:  puedes ver qué corrió, cuándo, cuánto tardó,
               y qué datos produjo — sin leer código.

La orquestación no añade valor de negocio por sí misma.
Pero sin ella, el valor de negocio de tus pipelines
se degrada silenciosamente hasta que un VP pregunta
por qué el dashboard muestra $0.
```

**Conexión con el Cap.18 (Observabilidad):**

> La orquestación te dice si el pipeline ejecutó.
> La observabilidad te dice si el pipeline produjo datos correctos.
> Un pipeline que ejecuta exitosamente pero produce métricas incorrectas
> es más peligroso que uno que falla — porque nadie se entera.
> El Cap.18 explora cómo monitorear no solo la ejecución,
> sino la calidad, el volumen, y la frescura de los datos
> que tus pipelines producen.

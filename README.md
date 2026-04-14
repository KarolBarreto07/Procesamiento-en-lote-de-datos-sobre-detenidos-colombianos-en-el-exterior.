# 📊🧑‍⚖️Procesamiento-en-lote-de-datos-sobre-detenidos-colombianos-en-el-exterior.
![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![PySpark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange?logo=apachespark)
![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-yellow?logo=apachehadoop)
![Big Data](https://img.shields.io/badge/Big%20Data-Processing-green)

## Uiversidad Nacional Abierta y a Distancia (UNAD)
## Big Data
## Descripción 📑

Este repositorio contiene el desarrollo de una solución de **procesamiento en lote (batch processing)** orientada al análisis de datos relacionados con detenidos colombianos en el exterior.
El proyecto utiliza **Apache Spark con Python (PySpark)** una herramienta de Big Data que permite el procesamiento distribuido de grandes volúmenes de información de manera eficiente y escalable, atraves de este desarrollo se realizara un **Análisis Exploratorio de Datos (EDA)**, técnicas de limpieza, transformación y análisis sobre un conjunto de datos estructurado, con el objetivo de extraer información relevante y patrones significativos relacionados con la situación de los detenidos colombianos en distintos países.

El objetivo principal es aplicar técnicas de Big Data para la limpieza, transformación y análisis de datos, permitiendo identificar patrones relevantes asociados a:
* Países con mayor número de detenidos
* Tipos de delitos más frecuentes
* Distribución por género y grupo etario
* Situación jurídica de los detenidos
* Tendencias temporales de registros

## Tecnologias usadas 👩🏻‍💻
Apache Spark, PySpark, Hadoop, Python

## A continuacion se describira paso a paso el proceso de desarrollo y ejecución 🤓
1. Se realiza la importación de las librerías de PySpark, específicamente SparkSession y functions, con el fin de permitir la creación de sesiones de Spark y el uso de funciones avanzadas para el procesamiento de datos.
Esto permite trabajar con datos distribuidos y aplicar operaciones como agregaciones, filtrados y transformaciones.
```python
# =================================================
# 1. IMPORTAR LIBRERÍAS
# =================================================
```python
from pyspark.sql import SparkSession, functions as F
```

2. Se crea una sesión de Spark utilizando SparkSession.builder, lo cual permite inicializar el motor de procesamiento distribuido. Esta sesión es necesaria para poder leer datos, transformarlos y ejecutar operaciones de análisis en paralelo.
```python
# =================================================
# 2. CREAR SESIÓN SPARK
# =================================================
spark = SparkSession.builder \
    .appName("Colombianos_Detenidos_Exterior") \
    .getOrCreate()
```

3. Se realiza la carga del archivo CSV desde el sistema de archivos HDFS mediante Spark DataFrame. Esto permite leer grandes volúmenes de datos de forma distribuida.
```python
# =================================================
# 3. CARGAR DATASET
# =================================================
file_path = "hdfs://localhost:9000/Tarea3.ProcesamientoDatos/e97j-vuf7.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

df.printSchema()
print("===DATOS ORIGINALES===")
df.show(15)
```

Se realiza la preparación del dataset para su análisis:
* Se renombran columnas para facilitar su uso.
* Se eliminan valores nulos en columnas importantes.
* Se eliminan registros duplicados.
Esto garantiza la calidad de los datos antes del análisis.
```python
#=================================================
# 4. LIMPIEZA DE DATOS Y TRANFORMACION DE DATOS
#=================================================

df_limpieza = (
    df
    .withColumnRenamed("PAIS PRISIÓN", "pais_prision")
    .withColumnRenamed("GÉNERO", "genero")
    .withColumnRenamed("SITUACIÓN JURÍDICA", "situacion_juridica")
    .withColumnRenamed("FECHA PUBLICACIÓN", "fecha_publicacion")
    .withColumnRenamed("EXTRADITADO Y O REPATRIADO", "estado_retorno")
    .withColumnRenamed("GRUPO EDAD", "grupo_edad")
    .withColumnRenamed("CANTIDAD", "cantidad")
    .withColumnRenamed("DELITO", "delito")
    .dropna(subset=["pais_prision", "delito", "genero", "situacion_juridica"])
    .dropDuplicates()
)

print("=== DATOS LIMPIOS ===")
df_limpieza.printSchema()
df_limpieza.show(15)

print("Total registros originales:", df.count())
print("Total registros limpios:", df_limpieza.count())
```
5. Se aplican operaciones con DataFrames como:
* groupBy() para agrupar datos
* count() para contar registros
* orderBy() para ordenar resultados
* filter() para aplicar condiciones

Estas operaciones permiten identificar patrones como:
- Países con más detenidos
- Delitos más frecuentes
- Distribución por género y edad
- Situación jurídica de los detenidos
```python
print("\n========================================")
print("   ANALISIS_EXPLORATORIO_DE_DATOS       ")
print("========================================")

# 1. País con más detenidos
print("\n1. Pais con mas detenidos" )
df_limpieza.groupBy("pais_prision") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# 2. Delitos más frecuentes
print("\n2. Delitos mas frecuentes")
df_limpieza.groupBy("delito") \
     .count() \
    .orderBy("count", ascending=False) \
    .show()

# 3. Distribución por género
print("\n3. Distribucion por genero")
df_limpieza.groupBy("genero") \
    .count() \
    .show()

# 4. Edad más común
print("\n4. Edad mas comun")
df_limpieza.groupBy("grupo_edad") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# 5. Situación jurídica
print("\n5. Sitiacion juridica")
df_limpieza.groupBy("situacion_juridica") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# 6. Número de detenidos masculinos por país
print("\n6. Número de detenidos masculinos por país")
df_limpieza.filter(df_limpieza.genero == "MASCULINO") \
    .groupBy("pais_prision") \
    .agg(F.count("*").alias("total_masculino")) \
    .orderBy("total_masculino", ascending=False) \
    .show()


# 7. Países con pocos casos
print("\n7. Paises con pocos casos")
df_limpieza.groupBy("pais_prision") \
    .count() \
    .filter("count < 10") \
    .show()

# 8. Porcentaje por género
print("\n8. Porcentaje por género")
total = df_limpieza.count()

df_limpieza.groupBy("genero") \
    .count() \
    .withColumn("porcentaje", (F.col("count") / total) * 100) \
    .show()

```

6. Se utilizan RDDs de Spark para realizar procesamiento de bajo nivel:
* map() para transformar los datos
* reduceByKey() para agregaciones
* filter() para filtrado de registros
Esto permite calcular métricas como:
- Promedio de detenidos por país
- Total de registros por año
```python
# =================================================
# 6. CONSULTAS CON RDD
# =================================================
print("\n--- CONSULTA RDD: Promedio de detenidos por país ---")

rdd_promedio = df_limpieza.rdd \
    .map(lambda x: (x["pais_prision"], (x["cantidad"], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]) \
    .sortBy(lambda x: -x[1])

for pais, promedio in rdd_promedio.take(10):
    print(f"{pais}: {round(promedio, 2)}")

print("\n--- CONSULTA RDD: Detenidos por año ---")


rdd_anio = df_limpieza.rdd \
    .filter(lambda x: x["fecha_publicacion"] is not None) \
    .map(lambda x: (x["fecha_publicacion"].year, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

for anio, total in rdd_anio.take(10):
    print(f"{anio}: {total}")
print("\n--- CONSULTA RDD: Total de registros ---")

total_registros = df_limpieza.rdd.count()

print(f"Total de registros: {total_registros}")
```











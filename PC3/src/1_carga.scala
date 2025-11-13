// 1_carga.scala
// Script para cargar el CSV en spark-shell

import org.apache.spark.sql.DataFrame

println("=== FASE 1: Cargando Datos (Usando sesión existente) ===")

// 1. Configurar el nivel de log en la sesión 'spark' que YA existe
spark.sparkContext.setLogLevel("WARN")

// 2. Ruta al archivo (usa el nombre SIN tilde, como sale en el error)
val dataPath = "data/Accidentes_de_transito.csv"

// 3. Cargar el DataFrame (toda la expresión encadenada a partir de spark.read)
val dfRaw: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(dataPath)

println("--- Esquema del DataFrame Original ---")
dfRaw.printSchema()
println("Datos cargados. Variable 'dfRaw' lista.")
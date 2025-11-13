// 2_Limpieza.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import spark.implicits._

println("\n=== FASE 2: Limpieza y Transformación de Datos ===")

// Tarea 1: Convertir campo FECHA (int) a DateType
val dfConFecha = dfRaw.withColumn(
  "fecha_accidente",
  to_date(col("FECHA").cast("string"), "yyyyMMdd")
)

// Tarea 2: Estandarizar campo DEPARTAMENTO
val dfEstandarizado = dfConFecha.withColumn(
  "departamento_std",
  upper(trim(col("DEPARTAMENTO")))
)

// Tarea 3: Crear/renombrar columnas con los nombres que necesitas
val dfPaso1 = dfEstandarizado.withColumn("modalidad", col("MODALIDAD"))
val dfPaso2 = dfPaso1.withColumn("km", col("KILOMETRO"))
// Aseguramos que heridos y fallecidos sean INT
val dfPaso3 = dfPaso2.withColumn("nro_fallecidos", coalesce(col("FALLECIDOS"), lit(0)))
val dfPaso4 = dfPaso3.withColumn("nro_heridos", coalesce(col("HERIDOS"), lit(0)))
val dfPaso5 = dfPaso4.withColumn("nro_fallecidos",
  coalesce(col("FALLECIDOS").cast("int"), lit(0))).withColumn("nro_heridos",
  coalesce(col("HERIDOS").cast("int"), lit(0)))

val dfPaso6 = dfPaso5.withColumn("codigo_via", col("CODIGO_VIA"))
val dfEnriquecido = dfPaso6.withColumn("hora", coalesce(substring(col("HORA").cast("string"), 1, 2).cast("int"), lit(0)))

println("--- Columnas de dfEnriquecido ---")
dfEnriquecido.columns.foreach(println)

// Tarea 4: Seleccionar solo las columnas que usaremos
val dfLimpio: DataFrame = dfEnriquecido.select(
  col("fecha_accidente"),
  col("departamento_std").alias("departamento"),
  col("modalidad"),
  col("codigo_via"),
  col("km"),
  col("nro_fallecidos"),
  col("nro_heridos"),
  col("hora")
)

println("--- Esquema del DataFrame Limpio y Transformado ---")
dfLimpio.printSchema()
println("--- Muestra de los Datos Limpios ---")
dfLimpio.show(10, false)
println("Datos limpios. Variable 'dfLimpio' lista.")

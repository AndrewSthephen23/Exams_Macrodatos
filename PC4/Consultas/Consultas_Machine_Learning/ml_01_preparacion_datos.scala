// ============================================================
// MACHINE LEARNING - PARTE 1: PREPARACION DE DATOS
// ============================================================
// Requisito: Preparar features para los modelos de ML
// Objetivo: Unir tablas y preparar columnas para clasificacion y regresion
// ============================================================

import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

println("\n>>> PREPARACION DE DATOS PARA MACHINE LEARNING")

// ------------------------------------------------------------
// 1. UNIR LAS DOS TABLAS
// ------------------------------------------------------------
println("\n[1] Uniendo tablas institucion y docentes...")

val dfCompleto = dfInstitucion.
  join(dfDocentes, "cod_mod").
  select(
    col("cod_mod"),
    col("nivel_educativo"),
    col("area_geografica"),
    col("docentes_hombres"),
    col("docentes_mujeres"),
    col("condicion_nombrados"),
    col("condicion_contratados"),
    col("total_docentes")
  ).
  na.drop() // Eliminar filas con valores nulos

println(s"Total de registros: ${dfCompleto.count()}")
dfCompleto.show(5, false)

// ------------------------------------------------------------
// 2. INDEXAR COLUMNAS CATEGORICAS (texto a numero)
// ------------------------------------------------------------
println("\n[2] Convirtiendo texto a numeros (StringIndexer)...")

// Convertir nivel_educativo a numero (para clasificacion)
val indexerNivel = new StringIndexer().
  setInputCol("nivel_educativo").
  setOutputCol("label_nivel")

// Convertir area_geografica a numero (para usar como feature)
val indexerArea = new StringIndexer().
  setInputCol("area_geografica").
  setOutputCol("area_index")

// Aplicar transformaciones
val dfIndexado = indexerArea.fit(dfCompleto).transform(dfCompleto)
val dfFinal = indexerNivel.fit(dfIndexado).transform(dfIndexado)

println("Datos con columnas indexadas:")
dfFinal.select("nivel_educativo", "label_nivel", "area_geografica", "area_index").show(5, false)

// ------------------------------------------------------------
// 3. CREAR VECTOR DE FEATURES
// ------------------------------------------------------------
println("\n[3] Creando vector de features (VectorAssembler)...")

// Columnas numericas que usaremos como features
val columnasFeatures = Array(
  "area_index",
  "docentes_hombres",
  "docentes_mujeres",
  "condicion_nombrados",
  "condicion_contratados"
)

val assembler = new VectorAssembler().
  setInputCols(columnasFeatures).
  setOutputCol("features")

val dfPreparado = assembler.transform(dfFinal)

println("Datos preparados para ML:")
dfPreparado.select("features", "label_nivel", "total_docentes").show(5, false)

// ------------------------------------------------------------
// 4. DIVIDIR EN ENTRENAMIENTO Y PRUEBA
// ------------------------------------------------------------
println("\n[4] Dividiendo datos: 70% entrenamiento, 30% prueba...")

val Array(datosEntrenamiento, datosPrueba) = dfPreparado.randomSplit(Array(0.7, 0.3), seed = 12345)

println(s"Datos de entrenamiento: ${datosEntrenamiento.count()}")
println(s"Datos de prueba: ${datosPrueba.count()}")

println("\n>>> DATOS LISTOS PARA ENTRENAR MODELOS")

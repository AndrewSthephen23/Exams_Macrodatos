import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

println("\n INICIANDO MODELO ML 2: RANDOM FOREST...")

// ------------------------------------------------------------
// 4. RANDOM FOREST
// Requisito: "Implementar Random Forest con campos decimales"
// Objetivo: Clasificar el tipo de GRUPO (Publicas/Privadas/Municipal)
//           basado en PROMEDIO_CONSUMO y SUMINISTROS
// ------------------------------------------------------------
println("\n>>> [4] Clasificacion de Grupo Empresarial con Random Forest")

// Paso 1: Preparar la data completa (Join)
val dfFull = dfConsumo.
  join(dfEmpresas, "COD_EMPRESA").
  join(dfTarifas, "COD_TARIFA")

// Paso 2: Seleccionar columnas relevantes y filtrar nulos
val dfML = dfFull.
  select("GRUPO", "PROMEDIO_CONSUMO", "SUMINISTROS").
  filter("PROMEDIO_CONSUMO > 0 AND SUMINISTROS > 0").
  filter("GRUPO IS NOT NULL")

// Paso 3: Convertir columna GRUPO (String) a indice numerico (label)
val indexer = new StringIndexer().
  setInputCol("GRUPO").
  setOutputCol("label").
  setHandleInvalid("skip")

val dfIndexed = indexer.fit(dfML).transform(dfML)

// Paso 4: Crear vector de features con campos decimales
val assembler = new VectorAssembler().
  setInputCols(Array("PROMEDIO_CONSUMO", "SUMINISTROS")).
  setOutputCol("features")

val dfFeatures = assembler.transform(dfIndexed)

// Paso 5: Dividir datos en entrenamiento (80%) y prueba (20%)
val Array(trainData, testData) = dfFeatures.randomSplit(Array(0.8, 0.2), seed = 12345)

// Iniciamos medicion de tiempo
val t0 = System.nanoTime()

// Paso 6: Configurar Random Forest
val randomForest = new RandomForestClassifier().
  setLabelCol("label").
  setFeaturesCol("features").
  setNumTrees(10).
  setMaxDepth(5).
  setSeed(12345)

// Paso 7: Entrenar el modelo
println("\n--- Entrenando Random Forest ---")
val modelo = randomForest.fit(trainData)

// Paso 8: Realizar predicciones
val predicciones = modelo.transform(testData)

// Paso 9: Mostrar resultados de ejemplo
println("\n--- Predicciones de Ejemplo ---")
predicciones.select("GRUPO", "label", "features", "prediction").show(10, false)

// Paso 10: Evaluar metricas del modelo
val evaluator = new MulticlassClassificationEvaluator().
  setLabelCol("label").
  setPredictionCol("prediction")

// Calcular todas las metricas
val accuracy = evaluator.setMetricName("accuracy").evaluate(predicciones)
val recall = evaluator.setMetricName("weightedRecall").evaluate(predicciones)
val f1Score = evaluator.setMetricName("f1").evaluate(predicciones)
val logLoss = evaluator.setMetricName("logLoss").evaluate(predicciones)

// Fin de medicion
val t1 = System.nanoTime()

println(f"\n--- Metricas del Modelo Random Forest ---")
println(f"Accuracy:  ${accuracy * 100}%.2f%%")
println(f"Recall:    ${recall * 100}%.2f%%")
println(f"F1-Score:  ${f1Score * 100}%.2f%%")
println(f"Log Loss:  ${logLoss}%.4f")

// Paso 11: Mostrar importancia de features (caracteristica de Random Forest)
println("\n--- Importancia de Features ---")
println(f"Feature 0 (PROMEDIO_CONSUMO): ${modelo.featureImportances(0)}%.4f")
println(f"Feature 1 (SUMINISTROS): ${modelo.featureImportances(1)}%.4f")

println(f"\n--- Informacion de Ejecucion ---")
println(f"Tiempo de ejecucion: ${(t1 - t0) / 1e9d}%.4f segundos")
println(f"Registros de entrenamiento: ${trainData.count()}")
println(f"Registros de prueba: ${testData.count()}")

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

println("\n INICIANDO MODELO ML 1: MULTILAYER PERCEPTRON...")

// ------------------------------------------------------------
// 3. MULTILAYER PERCEPTRON
// Requisito: "Implementar Multilayer Perceptron con campos decimales"
// Objetivo: Clasificar el tipo de USO (Residencial/Comercial/Industrial)
//           basado en PROMEDIO_CONSUMO y SUMINISTROS
// ------------------------------------------------------------
println("\n>>> [3] Clasificacion de Uso con Perceptron Multicapa")

// Paso 1: Preparar la data completa (Join)
val dfFull = dfConsumo.
  join(dfEmpresas, "COD_EMPRESA").
  join(dfTarifas, "COD_TARIFA")

// Paso 2: Seleccionar columnas relevantes y filtrar nulos
val dfML = dfFull.
  select("USO", "PROMEDIO_CONSUMO", "SUMINISTROS").
  filter("PROMEDIO_CONSUMO > 0 AND SUMINISTROS > 0").
  filter("USO IS NOT NULL")

// Paso 3: Convertir columna USO (String) a indice numerico (label)
val indexer = new StringIndexer().
  setInputCol("USO").
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

// Paso 6: Configurar Perceptron Multicapa
// Arquitectura: [2 entradas, 5 neuronas ocultas, 4 clases de salida]
val numClases = dfIndexed.select("label").distinct().count().toInt
val layers = Array[Int](2, 5, 4, numClases)

val perceptron = new MultilayerPerceptronClassifier().
  setLayers(layers).
  setBlockSize(128).
  setSeed(12345).
  setMaxIter(100)

// Paso 7: Entrenar el modelo
println("\n--- Entrenando Perceptron Multicapa ---")
val modelo = perceptron.fit(trainData)

// Paso 8: Realizar predicciones
val predicciones = modelo.transform(testData)

// Paso 9: Mostrar resultados de ejemplo
println("\n--- Predicciones de Ejemplo ---")
predicciones.select("USO", "label", "features", "prediction").show(10, false)

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

println(f"\n--- Metricas del Modelo Perceptron Multicapa ---")
println(f"Accuracy:  ${accuracy * 100}%.2f%%")
println(f"Recall:    ${recall * 100}%.2f%%")
println(f"F1-Score:  ${f1Score * 100}%.2f%%")
println(f"Log Loss:  ${logLoss}%.4f")
println(f"\n--- Informacion de Ejecucion ---")
println(f"Tiempo de ejecucion: ${(t1 - t0) / 1e9d}%.4f segundos")
println(f"Registros de entrenamiento: ${trainData.count()}")
println(f"Registros de prueba: ${testData.count()}")

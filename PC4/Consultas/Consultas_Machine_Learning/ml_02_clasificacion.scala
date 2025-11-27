// ============================================================
// MACHINE LEARNING - PARTE 2: MODELO DE CLASIFICACION
// ============================================================
// Requisito: Crear modelo de clasificacion (Random Forest)
// Objetivo: Predecir el nivel educativo (Inicial/Primaria/Secundaria)
// ============================================================

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

println("\n>>> MODELO DE CLASIFICACION: RANDOM FOREST")
println("Objetivo: Predecir nivel educativo (Inicial/Primaria/Secundaria)")

// ------------------------------------------------------------
// 1. CREAR Y ENTRENAR EL MODELO
// ------------------------------------------------------------
println("\n[1] Entrenando modelo Random Forest...")

val randomForest = new RandomForestClassifier().
  setLabelCol("label_nivel").
  setFeaturesCol("features").
  setNumTrees(10). // Numero de arboles
  setMaxDepth(5)   // Profundidad maxima

val modeloClasificacion = randomForest.fit(datosEntrenamiento)

println("Modelo entrenado correctamente")

// ------------------------------------------------------------
// 2. HACER PREDICCIONES
// ------------------------------------------------------------
println("\n[2] Realizando predicciones en datos de prueba...")

val predicciones = modeloClasificacion.transform(datosPrueba)

println("Predicciones vs Valores Reales:")
predicciones.select("nivel_educativo", "label_nivel", "prediction").show(10, false)

// ------------------------------------------------------------
// 3. EVALUAR EL MODELO - CALCULAR METRICAS
// ------------------------------------------------------------
println("\n[3] Calculando metricas del modelo...")

// Accuracy
val evaluadorAccuracy = new MulticlassClassificationEvaluator().
  setLabelCol("label_nivel").
  setPredictionCol("prediction").
  setMetricName("accuracy")
val accuracy = evaluadorAccuracy.evaluate(predicciones)

// F1-Score
val evaluadorF1 = new MulticlassClassificationEvaluator().
  setLabelCol("label_nivel").
  setPredictionCol("prediction").
  setMetricName("f1")
val f1Score = evaluadorF1.evaluate(predicciones)

// Recall (Weighted)
val evaluadorRecall = new MulticlassClassificationEvaluator().
  setLabelCol("label_nivel").
  setPredictionCol("prediction").
  setMetricName("weightedRecall")
val recall = evaluadorRecall.evaluate(predicciones)

// ------------------------------------------------------------
// 4. TABLA DE METRICAS
// ------------------------------------------------------------
println("\n============================================")
println("   TABLA DE METRICAS - CLASIFICACION")
println("   Modelo: Random Forest")
println("============================================")
println(f"| Metrica    | Valor              |")
println(f"|------------|---------------------|")
println(f"| Accuracy   | ${accuracy}%.4f             |")
println(f"| Recall     | ${recall}%.4f             |")
println(f"| F1-Score   | ${f1Score}%.4f             |")
println("============================================")

// ------------------------------------------------------------
// 5. IMPORTANCIA DE FEATURES
// ------------------------------------------------------------
println("\n[5] Importancia de cada feature:")
println("Features: area_index, docentes_hombres, docentes_mujeres, condicion_nombrados, condicion_contratados")
println(s"Importancia: ${modeloClasificacion.featureImportances}")

println("\n>>> CLASIFICACION COMPLETADA")

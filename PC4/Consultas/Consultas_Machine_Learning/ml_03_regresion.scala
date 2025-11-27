// ============================================================
// MACHINE LEARNING - PARTE 3: MODELO DE REGRESION
// ============================================================
// Requisito: Crear modelo de regresion (Regresion Lineal)
// Objetivo: Predecir la cantidad de docentes nombrados
// ============================================================

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

println("\n>>> MODELO DE REGRESION: REGRESION LINEAL")
println("Objetivo: Predecir la cantidad de docentes nombrados")

// ------------------------------------------------------------
// 1. PREPARAR FEATURES PARA REGRESION
// ------------------------------------------------------------
println("\n[1] Preparando features para regresion...")

// Usamos features para predecir docentes nombrados
val columnasRegresion = Array(
  "area_index",
  "label_nivel",
  "total_docentes"
)

val assemblerRegresion = new VectorAssembler().
  setInputCols(columnasRegresion).
  setOutputCol("features_regresion")

val dfRegresion = assemblerRegresion.transform(dfFinal)

// Dividir datos
val Array(trainRegresion, testRegresion) = dfRegresion.randomSplit(Array(0.7, 0.3), seed = 12345)

println(s"Datos entrenamiento: ${trainRegresion.count()}")
println(s"Datos prueba: ${testRegresion.count()}")

// ------------------------------------------------------------
// 2. CREAR Y ENTRENAR EL MODELO
// ------------------------------------------------------------
println("\n[2] Entrenando modelo de Regresion Lineal...")

val regresionLineal = new LinearRegression().
  setLabelCol("condicion_nombrados").
  setFeaturesCol("features_regresion").
  setMaxIter(10)

val modeloRegresion = regresionLineal.fit(trainRegresion)

println("Modelo entrenado correctamente")
println(s"Coeficientes: ${modeloRegresion.coefficients}")
println(s"Intercepto: ${modeloRegresion.intercept}")

// ------------------------------------------------------------
// 3. HACER PREDICCIONES
// ------------------------------------------------------------
println("\n[3] Realizando predicciones...")

val prediccionesReg = modeloRegresion.transform(testRegresion)

println("Predicciones vs Valores Reales:")
prediccionesReg.select("condicion_nombrados", "prediction", "total_docentes", "area_geografica").show(10, false)

// ------------------------------------------------------------
// 4. EVALUAR EL MODELO - CALCULAR METRICAS
// ------------------------------------------------------------
println("\n[4] Calculando metricas del modelo...")

// RMSE (Root Mean Squared Error)
val evaluadorRMSE = new RegressionEvaluator().
  setLabelCol("condicion_nombrados").
  setPredictionCol("prediction").
  setMetricName("rmse")
val rmse = evaluadorRMSE.evaluate(prediccionesReg)

// MAE (Mean Absolute Error)
val evaluadorMAE = new RegressionEvaluator().
  setLabelCol("condicion_nombrados").
  setPredictionCol("prediction").
  setMetricName("mae")
val mae = evaluadorMAE.evaluate(prediccionesReg)

// R2 (Coeficiente de determinacion)
val evaluadorR2 = new RegressionEvaluator().
  setLabelCol("condicion_nombrados").
  setPredictionCol("prediction").
  setMetricName("r2")
val r2 = evaluadorR2.evaluate(prediccionesReg)

// ------------------------------------------------------------
// 5. TABLA DE METRICAS
// ------------------------------------------------------------
println("\n============================================")
println("   TABLA DE METRICAS - REGRESION")
println("   Modelo: Regresion Lineal")
println("============================================")
println(f"| Metrica    | Valor              |")
println(f"|------------|---------------------|")
println(f"| RMSE       | ${rmse}%.4f             |")
println(f"| MAE        | ${mae}%.4f             |")
println(f"| R2         | ${r2}%.4f             |")
println("============================================")

// Resumen del modelo
println("\n[5] Resumen del entrenamiento:")
val resumen = modeloRegresion.summary
println(s"Iteraciones: ${resumen.totalIterations}")
println(s"RMSE entrenamiento: ${resumen.rootMeanSquaredError}")

println("\n>>> REGRESION COMPLETADA")

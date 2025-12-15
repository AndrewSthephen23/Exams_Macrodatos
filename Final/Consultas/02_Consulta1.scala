import org.apache.spark.sql.functions._

println("\n INICIANDO CONSULTAS SPARK SQL (SCALA)...")

// ------------------------------------------------------------
// 1. GROUP BY + AGGREGATION FUNCTIONS
// Requisito: "Usar Sql Spark con group by y funciones de org.apache.spark.sql.functions"
// Objetivo: Analizar el consumo Total y Promedio agrupado por GRUPO (Público/Privado) y USO (Residencial/Comercial).
// ------------------------------------------------------------
println("\n>>> [1] Análisis de Consumo por Sector y Uso (Group By + Agg)")

// Paso previo: Preparar la data unida (JOIN)
// Usamos las variables dfConsumo, dfEmpresas y dfTarifas que se cargo.
val dfFull = dfConsumo.
  join(dfEmpresas, "COD_EMPRESA").
  join(dfTarifas, "COD_TARIFA")

// Iniciamos medición de tiempo
val t0 = System.nanoTime()

// Lógica de la Consulta
val consulta1 = dfFull.groupBy("GRUPO", "USO").
  agg(
    sum("PROMEDIO_CONSUMO").alias("Consumo_Total_KW"),
    avg("PROMEDIO_CONSUMO").alias("Consumo_Promedio_KW"),
    max("SUMINISTROS").alias("Max_Suministros"),
    count("COD_EMPRESA").alias("Total_Registros")
  ).
  orderBy(desc("Consumo_Total_KW"))

// Mostramos resultado
consulta1.show(false)

// Fin de medición
val t1 = System.nanoTime()
println(f"Tiempo de ejecución: ${(t1 - t0) / 1e9d}%.4f segundos")
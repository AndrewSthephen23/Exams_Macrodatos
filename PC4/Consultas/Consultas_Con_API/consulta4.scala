// ============================================================
// CONSULTAS NATIVAS (SCALA DATAFRAME API)
// ============================================================
import org.apache.spark.sql.functions._

// ------------------------------------------------------------
// 4. PROMEDIO
// Requisito: "Una consulta que incluya el promedio de una columna"
// Objetivo: Promedio de docentes nombrados vs contratados a nivel nacional.
// ------------------------------------------------------------
println("\n>>> [4] Promedios de Condición Laboral Docente")

val consulta4 = dfDocentes.
  select(
    avg("condicion_nombrados").alias("promedio_nombrados"),
    avg("condicion_contratados").alias("promedio_contratados"),
    avg("total_docentes").alias("promedio_total_escuela")
  )

consulta4.show()
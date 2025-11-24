// ============================================================
// CONSULTAS NATIVAS (SCALA DATAFRAME API)
// ============================================================
import org.apache.spark.sql.functions._


// ------------------------------------------------------------
// 2. ORDENAMIENTO
// Requisito: "Mostrar información ordenada"
// Objetivo: Listar colegios ordenados por Departamento (A-Z) y Distrito.
// ------------------------------------------------------------
println("\n>>> [2] Listado ordenado por Departamento y Distrito")

val consulta2 = dfInstitucion.
  select("nombre_colegio", "departamento", "distrito").
  orderBy(col("departamento").asc, col("distrito").asc)

consulta2.show(5, false)
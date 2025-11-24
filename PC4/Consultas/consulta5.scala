// ============================================================
// CONSULTAS NATIVAS (SCALA DATAFRAME API)
// ============================================================
import org.apache.spark.sql.functions._

// ------------------------------------------------------------
// 5. JOIN
// Requisito: "Consulta que incluya el comando join"
// Objetivo: Unir Institución con Docentes para ver "Colegios Rurales con más de 20 docentes"
// ------------------------------------------------------------
println("\n>>> [5] JOIN: Colegios Rurales grandes (más de 20 docentes)")

val consulta5 = dfInstitucion.
  join(dfDocentes, "cod_mod"). // Join automático por la clave común
  filter(col("area_geografica") === "2.Rural").
  filter(col("total_docentes") > 20).
  select("nombre_colegio", "departamento", "total_docentes", "docentes_hombres", "docentes_mujeres")

consulta5.show(5, false)
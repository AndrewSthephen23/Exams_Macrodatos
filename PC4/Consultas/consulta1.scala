// ============================================================
// CONSULTAS NATIVAS (SCALA DATAFRAME API)
// ============================================================
import org.apache.spark.sql.functions._

println("\n INICIANDO CONSULTAS SPARK SQL (SCALA)...")

// ------------------------------------------------------------
// 1. SELECT + FILTER
// Requisito: "Mostrar columnas específicas... Utilizar filter"
// Objetivo: Ver colegios de gestión Publica en zonas RURALES.
// ------------------------------------------------------------
println("\n>>> [1] Colegios Privados en Zona Rural (Select + Filter)")

val consulta1 = dfInstitucion.
  select("cod_mod", "nombre_colegio", "departamento", "area_geografica", "tipo_gestion").
  filter(col("area_geografica") === "2.Rural").
  filter(col("tipo_gestion").contains("1.Pública"))

consulta1.show(5, false)
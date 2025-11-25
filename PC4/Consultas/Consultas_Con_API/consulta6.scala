// ============================================================
// CONSULTAS NATIVAS (SCALA DATAFRAME API)
// ============================================================
import org.apache.spark.sql.functions._

// ------------------------------------------------------------
// 6. FUNCIONES ESPECIALES
// Requisito: "Consulta utilizando las funciones de org.apache.spark.sql.functions"
// Objetivo: Crear categoría 'Ratio_Mujeres' usando 'when/otherwise'
// ------------------------------------------------------------
println("\n>>> [6] Funciones: Categorización por Ratio de Mujeres")

// Evitamos división por cero asegurando que total_docentes > 0
val consulta6 = dfDocentes.
  filter(col("total_docentes") > 0).
  withColumn("ratio_mujeres", col("docentes_mujeres") / col("total_docentes")).
  withColumn("categoria_genero", when(col("ratio_mujeres") > 0.7, "Mayoría Femenina").
                                 when(col("ratio_mujeres") < 0.3, "Mayoría Masculina").
                                 otherwise("Mixto Equilibrado")).
  select("cod_mod", "docentes_mujeres", "total_docentes", "ratio_mujeres", "categoria_genero")

// Mostramos los resultados por categoria
println("\nMayoría Masculina")
consulta6.filter(col("categoria_genero") === "Mayoría Masculina").show(20, false)
println("\nMayoría Femenina")
consulta6.filter(col("categoria_genero") === "Mayoría Femenina").show(20, false)
println("\nMixto Equilibrado")
consulta6.filter(col("categoria_genero") === "Mixto Equilibrado").show(20, false)
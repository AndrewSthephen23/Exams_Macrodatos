// ============================================================
// CONSULTAS NATIVAS (SCALA DATAFRAME API)
// ============================================================
import org.apache.spark.sql.functions._


// ------------------------------------------------------------
// 3. AGREGACIÓN (GROUP BY + COUNT)
// Requisito: "Utilizar el comando groupBy y count"
// Objetivo: Conteo de colegios por Nivel Educativo.
// ------------------------------------------------------------
println("\n>>> [3] Cantidad de Colegios por Nivel Educativo")

val consulta3 = dfInstitucion.
  groupBy("nivel_educativo").
  count().
  orderBy(col("count").desc)

consulta3.show(false)

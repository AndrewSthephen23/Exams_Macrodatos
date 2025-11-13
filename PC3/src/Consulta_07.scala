// Consulta 7: Estadísticas descriptivas de un campo numérico
// Pregunta: ¿Cuáles son el promedio, la mediana, la desviación estándar,
//           el mayor y el menor del campo 'nro_heridos'?

import org.apache.spark.sql.functions._

println("\n=== FASE 3: Consulta 7 (Estadísticas Descriptivas) ===")

// 1. Usar el DataFrame 'dfLimpio' directamente para cálculos estadísticos
println("Calculando estadísticas descriptivas para 'nro_heridos':")

// 2. Calcular estadísticas básicas usando DataFrame API
val stats = dfLimpio.select(
    avg("nro_heridos").alias("promedio"),
    stddev("nro_heridos").alias("desviacion_estandar"),
    max("nro_heridos").alias("maximo"),
    min("nro_heridos").alias("minimo")
)

// 3. Obtener los valores
val statsRow = stats.first()
val promedio = statsRow.getDouble(0)
val desviacion = statsRow.getDouble(1)
val maximo = statsRow.getInt(2)
val minimo = statsRow.getInt(3)

// 4. Calcular la MEDIANA usando approxQuantile
// approxQuantile retorna un array con el percentil 50 (mediana)
val mediana = dfLimpio.stat.approxQuantile("nro_heridos", Array(0.5), 0.01)(0)

// 5. Mostrar Resultados en Consola
println("--- Mostrando resultados de la Consulta 7: ---")
println("Estadísticas Descriptivas de 'nro_heridos':")
println(f"- Promedio (Media Aritmética): $promedio%.2f")
println(f"- Mediana (Percentil 50):      $mediana%.2f")
println(f"- Desviación Estándar:         $desviacion%.2f")
println(f"- Máximo:                      $maximo")
println(f"- Mínimo:                      $minimo")

// 6. Crear un resumen en formato texto para guardar
val resumen = s"""Estadísticas Descriptivas de nro_heridos
========================================
Promedio (Media):      ${promedio}
Mediana:               ${mediana}
Desviación Estándar:   ${desviacion}
Máximo:                ${maximo}
Mínimo:                ${minimo}
"""

// 7. Guardar los resultados
// Crear un RDD con el resumen para guardarlo como texto
val rddResumen = spark.sparkContext.parallelize(Seq(resumen))
val outputPath7 = "output/07_estadisticas_heridos"
rddResumen.saveAsTextFile(outputPath7)

println(s"Resultados guardados en la carpeta: $outputPath7")

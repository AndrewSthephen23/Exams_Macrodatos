// 3_Consulta_04.scala
// Consulta 4: Filtrado de fechas a partir de una muestra aleatoria
// Pregunta: ¿Cuáles son los accidentes ocurridos en Julio, 
//           de una muestra aleatoria del 10% del total?

import java.sql.Date // Necesario para manejar el tipo Date

println("\n=== FASE 3: Consulta 4 (Muestra y Filtro Fecha) ===")

// 1. Usar el RDD 'rddLimpio' que ya existe en la shell
val rddLimpio = dfLimpio.rdd

// 2. Fase de MUESTREO
// Tomamos una muestra aleatoria del 10% de los datos, sin reemplazo
// .sample(withReplacement, fraction, seed)
val rddMuestra = rddLimpio.sample(false, 0.1)

// 3. Fase de FILTRADO (equivale a un Map)
// Filtramos la muestra por el campo de fecha
// Indices en dfLimpio: 0=fecha_accidente (es un objeto java.sql.Date)
// getMonth() es 0-indexado (Enero=0, Julio=6)
val rddFiltrado = rddMuestra.filter(row => {
    val fecha = row.getDate(0) // Obtenemos el objeto Date
    fecha != null && fecha.getMonth() == 6 // 6 = Julio
})

// 4. Mostrar Resultados en Consola 
println("--- Mostrando resultados de la Consulta 4: ---")
// Mostramos los resultados (son filas completas, no agregaciones)
rddFiltrado.collect().foreach(println)

// 5. Guardar el resultado en un archivo de texto
val outputPath4 = "output/04_muestra_accidentes_julio"
rddFiltrado.saveAsTextFile(outputPath4)

println(s"Resultados guardados en la carpeta: $outputPath4")
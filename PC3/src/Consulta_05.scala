// 3_Consulta_05.scala
// Consulta 5: Buscar texto en un campo y agrupar por otro
// Pregunta: ¿En qué modalidad (tipo) aparecen más los accidentes
//           en vías que contienen el texto "PE" (vías principales)?

println("\n=== FASE 3: Consulta 5 (Búsqueda de Texto + Agrupación) ===")

// 1. Usar el RDD 'rddLimpio' que ya existe en la shell
val rddLimpio = dfLimpio.rdd

// 2. Fase de FILTRADO (busqueda de texto en codigo_via)
// Buscamos vías que contengan "PE" en su código (vias principales)
// Indices en dfLimpio: 3=codigo_via
val rddFiltrado = rddLimpio.filter(row => {
    val codigoVia = row.getString(3)
    codigoVia != null && codigoVia.toUpperCase().contains("PE")
})

// 3. Fase MAP
// Creamos tuplas (Key, Value)
// Key: modalidad -> String
// Value: 1 (para contar)
// Indices en dfLimpio: 2=modalidad
val rddMapeado5 = rddFiltrado.map(row =>
    (row.getString(2), 1)
)

// 4. Fase REDUCE
// Agrupamos por modalidad y sumamos los Values (conteo)
val rddAgregado5 = rddMapeado5.reduceByKey(_ + _)

// 5. Ordenar por conteo descendente para ver cuál aparece más
val rddOrdenado5 = rddAgregado5.sortBy(_._2, ascending=false)

// 6. Mostrar Resultados en Consola
println("--- Mostrando resultados de la Consulta 5: ---")
println("Modalidades en vias con código 'PE' (ordenadas por frecuencia):")
rddOrdenado5.collect().foreach(println)

// 7. Guardar el resultado en un archivo de texto
val outputPath5 = "output/05_modalidad_vias_pe"
rddOrdenado5.saveAsTextFile(outputPath5)

println(s"Resultados guardados en la carpeta: $outputPath5")

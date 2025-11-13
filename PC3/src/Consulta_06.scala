// Consulta 6: Agrupar por tipo y encontrar max/min de campo numérico
// Pregunta: ¿Cuál es el máximo y mínimo de 'nro_fallecidos'
//           agrupados por 'departamento'?

println("\n=== FASE 3: Consulta 6 (Max/Min por Tipo) ===")

// 1. Usar el RDD 'rddLimpio' que ya existe en la shell
val rddLimpio = dfLimpio.rdd

// 2. Fase MAP
// Creamos tuplas (Key, Value)
// Key: departamento -> String
// Value: nro_fallecidos -> Int
// Indices en dfLimpio: 1=departamento, 5=nro_fallecidos
val rddMapeado6 = rddLimpio.map(row =>
    (row.getString(1), row.getInt(5))
)

// 3. Fase REDUCE con aggregateByKey
// Usamos aggregateByKey para calcular max y min simultáneamente
// Acumulador: (max, min) como tupla de Ints
val rddAgregado6 = rddMapeado6.aggregateByKey((Int.MinValue, Int.MaxValue))(
    // Función de combinación secuencial (dentro de una partición)
    // Actualiza el acumulador (max, min) con cada nuevo valor
    (acc, value) => (math.max(acc._1, value), math.min(acc._2, value)),
    // Función de combinación entre particiones
    // Combina dos acumuladores (max1, min1) y (max2, min2)
    (acc1, acc2) => (math.max(acc1._1, acc2._1), math.min(acc1._2, acc2._2))
)

// 4. Formatear los resultados para mejor legibilidad
val rddFormateado6 = rddAgregado6.map { case (dpto, (maxFall, minFall)) =>
    s"$dpto -> Max: $maxFall, Min: $minFall"
}

// 5. Ordenar por departamento alfabéticamente
val rddOrdenado6 = rddFormateado6.sortBy(line => line.split(" ->")(0))

// 6. Mostrar Resultados en Consola
println("--- Mostrando resultados de la Consulta 6: ---")
println("Máximo y Mínimo de fallecidos por departamento:")
rddOrdenado6.collect().foreach(println)

// 7. Guardar el resultado en un archivo de texto
val outputPath6 = "output/06_max_min_fallecidos_por_dpto"
rddOrdenado6.saveAsTextFile(outputPath6)

println(s"Resultados guardados en la carpeta: $outputPath6")

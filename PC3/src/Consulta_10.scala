// Consulta 10: Consulta con decimales (3 MapReduce anidados)
// Pregunta: ¿Cuál es el índice de severidad promedio
//           ((fallecidos*3 + heridos) / accidentes) por hora del día?

println("\n=== FASE 3: Consulta 10 (3 MapReduce Anidados - Índice Severidad) ===")

// 1. Obtener RDD
val rddLimpio = dfLimpio.rdd

// MAPREDUCE 1: Calcular índice de severidad por cada accidente
// MAP 1: (hora, indice_severidad) donde indice = fallecidos*3 + heridos
println("MAP 1: Calculando índice de severidad por accidente...")
val rddMap1_10 = rddLimpio.map(row => {
    val hora = row.getInt(7)
    val fallecidos = row.getInt(5).toDouble
    val heridos = row.getInt(6).toDouble
    // Índice de severidad: fallecidos pesan 3 veces más que heridos
    val indiceSeveridad = (fallecidos * 3.0) + heridos
    (hora, indiceSeveridad)
})

// MAPREDUCE 2: Sumar índices de severidad y contar accidentes por hora
// REDUCE 2: Agregación por hora
println("REDUCE 2: Sumando índices y contando accidentes por hora...")
val rddReduce2_10 = rddMap1_10.aggregateByKey((0.0, 0))(
    // Función secuencial: acumular índice (Double) y contar (Int)
    (acc, indice) => (acc._1 + indice, acc._2 + 1),
    // Función de combinación entre particiones
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

// MAPREDUCE 3: Calcular promedio de índice de severidad por hora
// MAP 3: Dividir suma_indices / total_accidentes
println("MAP 3: Calculando promedio del índice por hora...")
val rddMap3_10 = rddReduce2_10.map { case (hora, (sumaIndices, totalAccidentes)) =>
    val promedioSeveridad = if (totalAccidentes > 0) sumaIndices / totalAccidentes else 0.0
    (hora, promedioSeveridad)
}

// Ordenar por hora (0-23)
val rddOrdenado10 = rddMap3_10.sortByKey()

// Mostrar resultados (formateados a 2 decimales)
println("--- Mostrando resultados de la Consulta 10: ---")
println("Índice de Severidad Promedio por Hora del Día:")
rddOrdenado10.collect().foreach { case (hora, indice) =>
    println(f"Hora ${hora}%02d:00 - Índice de Severidad: ${indice}%.2f")
}

// Guardar resultados
val outputPath10 = "output/10_indice_severidad_por_hora"
rddOrdenado10.saveAsTextFile(outputPath10)

println(s"Resultados guardados en la carpeta: $outputPath10")
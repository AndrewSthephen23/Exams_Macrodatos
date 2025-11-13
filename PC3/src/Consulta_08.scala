// Consulta 8: Consulta con decimales (3 MapReduce anidados)
// Pregunta: ¿Cuál es la tasa de fatalidad promedio
//           (fallecidos / total_víctimas) por departamento?

println("\n=== FASE 3: Consulta 8 (3 MapReduce Anidados - Tasa Fatalidad) ===")

// 1. Obtener RDD
val rddLimpio = dfLimpio.rdd

// MAPREDUCE 1: Extraer datos y calcular total_victimas por registro
// MAP 1: Extraer (departamento, fallecidos, heridos) y calcular total_victimas
println("MAP 1: Calculando total de víctimas por registro...")
val rddMap1 = rddLimpio.map(row => {
    val dpto = row.getString(1)           // departamento
    val fallecidos = row.getInt(5).toDouble  // convertir a Double
    val heridos = row.getInt(6).toDouble     // convertir a Double
    val totalVictimas = fallecidos + heridos // suma en Double
    (dpto, (fallecidos, totalVictimas))
})

// MAPREDUCE 2: Calcular tasa_fatalidad por cada registro
// MAP 2: Calcular tasa = fallecidos / total_victimas (resultado decimal)
println("MAP 2: Calculando tasa de fatalidad por registro...")
val rddMap2 = rddMap1.map { case (dpto, (fallecidos, totalVictimas)) =>
    // Si total_victimas > 0, calcular tasa; sino, tasa = 0.0
    val tasa = if (totalVictimas > 0.0) fallecidos / totalVictimas else 0.0
    (dpto, (tasa, 1)) // (departamento, (tasa_fatalidad, contador))
}

// MAPREDUCE 3: Calcular promedio de tasa_fatalidad por departamento
// REDUCE 3: Sumar tasas y contar registros por departamento
println("REDUCE 3: Agrupando y calculando promedio por departamento...")
val rddReduce3 = rddMap2.reduceByKey { case ((tasa1, count1), (tasa2, count2)) =>
    (tasa1 + tasa2, count1 + count2) // suma de tasas y contadores
}

// MAP FINAL: Calcular el promedio (suma_tasas / count)
val rddFinal8 = rddReduce3.map { case (dpto, (sumaTasas, count)) =>
    val promedioTasa = sumaTasas / count
    (dpto, promedioTasa)
}

// Ordenar por tasa de fatalidad descendente
val rddOrdenado8 = rddFinal8.sortBy(_._2, ascending=false)

// Mostrar resultados (formateados a 4 decimales)
println("--- Mostrando resultados de la Consulta 8: ---")
println("Tasa de Fatalidad Promedio por Departamento (ordenado desc):")
rddOrdenado8.collect().foreach { case (dpto, tasa) =>
    println(f"$dpto: ${tasa}%.4f (${tasa*100}%.2f%%)")
}

// Guardar resultados
val outputPath8 = "output/08_tasa_fatalidad_por_dpto"
rddOrdenado8.saveAsTextFile(outputPath8)

println(s"Resultados guardados en la carpeta: $outputPath8")

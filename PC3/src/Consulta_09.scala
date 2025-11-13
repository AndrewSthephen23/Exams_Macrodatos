// Consulta 9: Consulta con decimales (3 MapReduce anidados)
// Pregunta: ¿Cuál es el ratio promedio de heridos por accidente
//           (total_heridos / total_accidentes) por modalidad?

println("\n=== FASE 3: Consulta 9 (3 MapReduce Anidados - Ratio Heridos) ===")

// 1. Obtener RDD
val rddLimpio = dfLimpio.rdd

// MAPREDUCE 1: Extraer datos relevantes
// MAP 1: (modalidad, nro_heridos) - convertir a Double
println("MAP 1: Extrayendo modalidad y número de heridos...")
val rddMap1_9 = rddLimpio.map(row => {
    val modalidad = row.getString(2)
    val heridos = row.getInt(6).toDouble // convertir a Double
    (modalidad, heridos)
})

// MAPREDUCE 2: Sumar heridos y contar accidentes por modalidad
// REDUCE 2: Agrupar por modalidad usando aggregateByKey
println("REDUCE 2: Sumando heridos y contando accidentes por modalidad...")
val rddReduce2_9 = rddMap1_9.aggregateByKey((0.0, 0))(
    // Función secuencial: acumular heridos (Double) y contar (Int)
    (acc, heridos) => (acc._1 + heridos, acc._2 + 1),
    // Función de combinación entre particiones
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

// MAPREDUCE 3: Calcular ratio (total_heridos / total_accidentes)
// MAP 3: Dividir para obtener el ratio decimal
println("MAP 3: Calculando ratio heridos/accidente por modalidad...")
val rddMap3_9 = rddReduce2_9.map { case (modalidad, (totalHeridos, totalAccidentes)) =>
    val ratio = if (totalAccidentes > 0) totalHeridos / totalAccidentes else 0.0
    (modalidad, ratio)
}

// Ordenar por ratio descendente
val rddOrdenado9 = rddMap3_9.sortBy(_._2, ascending=false)

// Mostrar resultados (formateados a 2 decimales)
println("--- Mostrando resultados de la Consulta 9: ---")
println("Ratio Promedio de Heridos por Accidente (por Modalidad):")
rddOrdenado9.collect().foreach { case (modalidad, ratio) =>
    println(f"$modalidad: ${ratio}%.2f heridos/accidente")
}

// Guardar resultados
val outputPath9 = "output/09_ratio_heridos_por_modalidad"
rddOrdenado9.saveAsTextFile(outputPath9)

println(s"Resultados guardados en la carpeta: $outputPath9")
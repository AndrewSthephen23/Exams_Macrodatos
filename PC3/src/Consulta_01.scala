// 3_Consulta_01.scala
// Consulta 1:
// Pregunta: ¿Cuál es el total de 'nro_fallecidos' (campo 1) 
//           agrupados por 'departamento' (campo 2) y 'modalidad' (campo 3)?

println("\n=== FASE 3: Consulta 1 (3 campos) ===")

// 1. Convertir el DataFrame 'dfLimpio' a un RDD
// El RDD contendrá objetos 'Row'
val rddLimpio = dfLimpio.rdd

// 2. Fase MAP
// Creamos tuplas (Key, Value)
// Key: (departamento, modalidad) -> (String, String)
// Value: nro_fallecidos -> Int
// Indices en dfLimpio: 1=departamento, 2=modalidad, 5=nro_fallecidos
val rddMapeado = rddLimpio.map(row => 
    ( (row.getString(1), row.getString(2)), row.getInt(5) )
)

// 3. Fase REDUCE
// Agrupamos por la Key (departamento, modalidad) y sumamos los Values (nro_fallecidos)
val rddAgregado = rddMapeado.reduceByKey(_ + _)

// 4. Mostrar Resultados en Consola
println("--- Mostrando resultados de la Consulta 1: ---")
rddAgregado.collect().foreach(println)

// 5. Guardar el resultado en un archivo de texto
val outputPath = "output/01_fallecidos_por_dpto_modalidad"
rddAgregado.saveAsTextFile(outputPath)

println(s"Resultados guardados en la carpeta: $outputPath")
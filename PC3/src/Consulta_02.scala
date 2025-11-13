// 3_Consulta_02.scala
// Consulta 2:
// Pregunta: ¿Cuál es el total de 'nro_heridos' (campo 1) 
//           agrupados por 'departamento' (campo 2) y 'modalidad' (campo 3)?

println("\n=== FASE 3: Consulta 2 (3 campos) ===")

// 1. Convertir el DataFrame 'dfLimpio' a un RDD
val rddLimpio = dfLimpio.rdd

// 2. Fase MAP
// Creamos tuplas (Key, Value)
// Key: (departamento, modalidad) -> (String, String)
// Value: nro_heridos -> Int
// Indices en dfLimpio: 1=departamento, 2=modalidad, 6=nro_heridos
val rddMapeado2 = rddLimpio.map(row => 
    ( (row.getString(1), row.getString(2)), row.getInt(6) )
)

// 3. Fase REDUCE
// Agrupamos por la Key (departamento, modalidad) y sumamos los Values (nro_heridos)
val rddAgregado2 = rddMapeado2.reduceByKey(_ + _)

// 4. Mostrar Resultados en Consola
println("--- Mostrando resultados de la Consulta 2: ---")
rddAgregado2.collect().foreach(println)

// 5. Guardar el resultado en un archivo de texto
val outputPath2 = "output/02_heridos_por_dpto_modalidad"
rddAgregado2.saveAsTextFile(outputPath2)

println(s"Resultados guardados en la carpeta: $outputPath2")
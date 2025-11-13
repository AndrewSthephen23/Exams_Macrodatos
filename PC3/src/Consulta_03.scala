// 3_Consulta_03.scala
// Consulta 3:
// Pregunta: ¿Cuál es el conteo total de accidentes (campo 1) 
//           agrupados por 'departamento' (campo 2) y 'modalidad' (campo 3)?

println("\n=== FASE 3: Consulta 3 (3 campos) ===")

// 1. Usar el RDD 'rddLimpio' que ya existe en la shell
val rddLimpio = dfLimpio.rdd

// 2. Fase MAP
// Creamos tuplas (Key, Value)
// Key: (departamento, modalidad) -> (String, String)
// Value: 1 (para poder contar/sumar las ocurrencias)
// Indices en dfLimpio: 1=departamento, 2=modalidad
val rddMapeado3 = rddLimpio.map(row => 
    ( (row.getString(1), row.getString(2)), 1 )
)

// 3. Fase REDUCE
// Agrupamos por la Key (departamento, modalidad) y sumamos los Values (los '1')
val rddAgregado3 = rddMapeado3.reduceByKey(_ + _)

// 4. Mostrar Resultados en Consola
println("--- Mostrando resultados de la Consulta 3: ---")
rddAgregado3.collect().foreach(println)

// 5. Guardar el resultado en un archivo de texto
val outputPath3 = "output/03_conteo_por_dpto_modalidad"
rddAgregado3.saveAsTextFile(outputPath3)

println(s"Resultados guardados en la carpeta: $outputPath3")
// 1. Configuración de Logs
spark.sparkContext.setLogLevel("ERROR")

// Comando para saber la direccion de HDFS -> hdfs getconf -confKey fs.defaultFS
val namenode = "hdfs://localhost:9000" 

println(s">>> INICIANDO CARGA DESDE $namenode/input_final/...")

// 2. Carga de datos con la corrección "multiline"
// Esto le dice a Spark que lea el archivo como un bloque completo y no línea por línea.
val dfEmpresas = spark.read.option("multiline", "true").json(s"$namenode/input_final/dim_empresas.json")
val dfTarifas = spark.read.option("multiline", "true").json(s"$namenode/input_final/dim_tarifas.json")
val dfConsumo = spark.read.option("multiline", "true").json(s"$namenode/input_final/fact_consumo.json")

// 3. Verificación de Esquemas y Datos
println("\n--- ESQUEMA EMPRESAS ---")
dfEmpresas.printSchema()
dfEmpresas.show(5)

println("\n--- ESQUEMA TARIFAS ---")
dfTarifas.printSchema()
dfTarifas.show(5)

println("\n--- ESQUEMA CONSUMO (HECHOS) ---")
dfConsumo.printSchema()
dfConsumo.show(5)

// 4. Prueba rápida con SQL
dfConsumo.createOrReplaceTempView("consumo_temp")

println("\n--- PRUEBA SQL RÁPIDA (Conteo Total) ---")
spark.sql("SELECT count(*) as total_registros_consumo FROM consumo_temp").show()

println("\n--- PRUEBA SQL RÁPIDA (Top 3 Consumo) ---")
spark.sql("SELECT * FROM consumo_temp ORDER BY PROMEDIO_CONSUMO DESC LIMIT 3").show()
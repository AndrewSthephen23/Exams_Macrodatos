println("\n INICIANDO CONSULTA 2 (SPARK SQL + VISTAS)...")

// ------------------------------------------------------------
// 2. VISTAS TEMPORALES + SQL PURO
// Requisito: "Usar vistas temporales, OrderBy y operadores de selección"
// Objetivo: Encontrar el Top 10 de registros con mayor consumo en el sector 'Residencial', mostrando su empresa y tarifa.
// ------------------------------------------------------------
println("\n>>> [2] Top 10 Consumo Residencial (SQL Views + OrderBy)")

// Paso 1: Preparar la data completa (Join)
// Asumimos que dfConsumo, dfEmpresas y dfTarifas ya están cargados en memoria.
val dfFull = dfConsumo.
  join(dfEmpresas, "COD_EMPRESA").
  join(dfTarifas, "COD_TARIFA")

// Paso 2: Crear la Vista Temporal
// Esto permite usar sentencias SQL estándar ("SELECT ...") sobre el DataFrame
dfFull.createOrReplaceTempView("v_energia_unificada")

// Iniciamos medición de tiempo
val t0 = System.nanoTime()

// Paso 3: Ejecutar la Consulta SQL
// Usamos:
// - WHERE: Selección (Filtro por uso Residencial y exclusión de nulos/ceros)
// - ORDER BY: Ordenamiento (De mayor a menor consumo)
// - LIMIT: Restricción de salida
val consulta2 = spark.sql("""
  SELECT 
    RAZON_SOCIAL as Empresa,
    ATARIFA as Tarifa,
    GRUPO as Sector,
    PROMEDIO_CONSUMO as Consumo_KWh
  FROM v_energia_unificada
  WHERE USO = 'Residencial' 
    AND PROMEDIO_CONSUMO > 0
  ORDER BY PROMEDIO_CONSUMO DESC
  LIMIT 10
""")

// Mostramos resultado
consulta2.show(false)

// Fin de medición
val t1 = System.nanoTime()
println(f"Tiempo de ejecución: ${(t1 - t0) / 1e9d}%.4f segundos")
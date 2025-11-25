// ============================================================
// CONSULTAS CON VISTAS TEMPORALES (SPARK SQL PURO)
// ============================================================

println("\n INICIANDO CONSULTAS SQL PURO...")

// ------------------------------------------------------------
// PASO 0: CREACIÓN DE VISTAS TEMPORALES
// Esto permite usar los DataFrames como tablas en SQL
// ------------------------------------------------------------
dfInstitucion.createOrReplaceTempView("v_institucion")
dfDocentes.createOrReplaceTempView("v_docentes")

println(" Vistas temporales 'v_institucion' y 'v_docentes' creadas.")


// ------------------------------------------------------------
// 1. SELECCIÓN UTILIZANDO JOIN
// Requisito: "1 Selección utilizando join"
// Objetivo: Listar colegios con déficit de docentes nombrados (más contratados que nombrados).
// ------------------------------------------------------------
println("\n>>> [SQL-1] JOIN: Colegios donde predominan docentes contratados")

val sqlJoin = spark.sql("""
  SELECT 
    i.nombre_colegio,
    i.departamento,
    i.tipo_gestion,
    d.condicion_nombrados,
    d.condicion_contratados
  FROM v_institucion i
  INNER JOIN v_docentes d ON i.cod_mod = d.cod_mod
  WHERE d.condicion_contratados > d.condicion_nombrados
  LIMIT 10
""")

sqlJoin.show(false)
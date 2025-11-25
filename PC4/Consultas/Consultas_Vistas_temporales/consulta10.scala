// ------------------------------------------------------------
// 4. ORDER BY + SELECCIÓN (Consulta A)
// Requisito: "2 consultas usando OrderBy combinado con otros operadores" (1/2)
// Objetivo: Top 10 colegios más grandes (por personal) en la capital (Lima).
// ------------------------------------------------------------
println("\n>>> [SQL-4] Top 10 Colegios más grandes en LIMA")

val sqlOrder1 = spark.sql("""
  SELECT 
    i.nombre_colegio,
    i.distrito,
    d.total_docentes
  FROM v_institucion i
  JOIN v_docentes d ON i.cod_mod = d.cod_mod
  WHERE i.departamento = 'LIMA' 
  ORDER BY d.total_docentes DESC
  LIMIT 10
""")

sqlOrder1.show(false)
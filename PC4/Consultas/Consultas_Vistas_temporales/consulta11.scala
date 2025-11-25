// ------------------------------------------------------------
// 5. ORDER BY + SELECCIÓN (Consulta B)
// Requisito: "2 consultas usando OrderBy combinado con otros operadores" (2/2)
// Objetivo: Listar colegios de nivel Inicial con pocos docentes (posibles unidocentes), ordenados por provincia.
// ------------------------------------------------------------
println("\n>>> [SQL-5] Colegios Iniciales pequeños (posibles Unidocentes)")

val sqlOrder2 = spark.sql("""
  SELECT 
    i.provincia,
    i.distrito,
    i.nombre_colegio,
    d.total_docentes
  FROM v_institucion i
  JOIN v_docentes d ON i.cod_mod = d.cod_mod
  WHERE i.nivel_educativo = '1.Inicial' 
    AND d.total_docentes <= 2
    AND d.total_docentes > 0
  ORDER BY i.provincia ASC, d.total_docentes ASC
  LIMIT 10
""")

sqlOrder2.show(false)
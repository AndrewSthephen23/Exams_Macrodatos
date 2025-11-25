// ------------------------------------------------------------
// 2. GROUP BY + COUNT (Consulta A)
// Requisito: "2 consultas usando GroupBy usando count" (1/2)
// Objetivo: Cantidad de Colegios por Departamento (Ranking geográfico).
// ------------------------------------------------------------
println("\n>>> [SQL-2] Cantidad de Colegios por Departamento")

val sqlGroup1 = spark.sql("""
  SELECT 
    departamento, 
    COUNT(*) as total_colegios
  FROM v_institucion
  GROUP BY departamento
  ORDER BY total_colegios DESC
""")

sqlGroup1.show(10, false) // Mostramos top 10
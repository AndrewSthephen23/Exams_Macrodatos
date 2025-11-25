// ------------------------------------------------------------
// 3. GROUP BY + COUNT (Consulta B)
// Requisito: "2 consultas usando GroupBy usando count" (2/2)
// Objetivo: Distribución de colegios según Gestión y Área (Tabla cruzada).
// ------------------------------------------------------------
println("\n>>> [SQL-3] Distribución por Gestión y Área Geográfica")

val sqlGroup2 = spark.sql("""
  SELECT 
    tipo_gestion,
    area_geografica,
    COUNT(*) as cantidad
  FROM v_institucion
  GROUP BY tipo_gestion, area_geografica
  ORDER BY tipo_gestion
""")

sqlGroup2.show(false)
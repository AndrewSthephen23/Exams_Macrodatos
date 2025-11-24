// ============================================================
// FASE 2 (Parte 2): CONEXIÓN JDBC
// ============================================================

// 1. CREDENCIALES (Actualizadas según tu log)
// Nota: Usamos 'bd_practica4' y password 'kali'
val jdbcUrl = "jdbc:postgresql://localhost:5432/bd_practica4"
val dbUser = "postgres"
val dbPassword = "tu contraseña"

println("📡 Conectando a 'bd_practica4' con usuario 'postgres'...")

// 2. CARGAR TABLA: tb_institucion

val dfInstitucion = spark.read.format("jdbc").
  option("url", jdbcUrl).
  option("dbtable", "tb_institucion").
  option("user", dbUser).
  option("password", dbPassword).
  option("driver", "org.postgresql.Driver").
  load()

// 3. CARGAR TABLA: tb_plana_docente
val dfDocentes = spark.read.format("jdbc").
  option("url", jdbcUrl).
  option("dbtable", "tb_plana_docente").
  option("user", dbUser).
  option("password", dbPassword).
  option("driver", "org.postgresql.Driver").
  load()

// 4. VERIFICACIÓN
println("------------------------------------------------")
println(s"✅ Tabla Institución: ${dfInstitucion.count()} registros")
println(s"✅ Tabla Docentes:    ${dfDocentes.count()} registros")
println("------------------------------------------------")

println("--- Esquema: tb_institucion ---")
dfInstitucion.printSchema()

println("--- Muestra: tb_institucion ---")
dfInstitucion.show(5, false)

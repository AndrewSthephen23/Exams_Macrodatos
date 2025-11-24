import pandas as pd
from sqlalchemy import create_engine, text  # <--- Importamos 'text'

# ==========================================
# CONFIGURACIÓN
# ==========================================
# Nombre del archivo CSV origen
CSV_FILE = 'Docentes2024_0.csv'

# Credenciales de PostgreSQL
DB_USER = 'postgres'
DB_PASS = 'kali'  # Tu contraseña
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'bd_practica4'

# Cadena de conexión
db_connection_str = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
db_connection = create_engine(db_connection_str)

def run_etl():
    print(" Iniciando proceso ETL...")

    # ---------------------------------------------------------
    # 1. EXTRACCIÓN
    # ---------------------------------------------------------
    print(f"   Leyendo archivo {CSV_FILE}...")
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8')
    except UnicodeDecodeError:
        print(" Codificación UTF-8 falló, intentando con Latin-1...")
        df = pd.read_csv(CSV_FILE, encoding='latin-1')
    
    print(f" Archivo leído. Filas encontradas: {len(df)}")

    # ---------------------------------------------------------
    # 2. TRANSFORMACIÓN
    # ---------------------------------------------------------
    print(" Procesando y normalizando tablas...")

    # --- TABLA 1: tb_institucion ---
    cols_institucion = {
        'cod_mod': 'cod_mod',
        'cen_edu': 'nombre_colegio',
        'nivel': 'nivel_educativo',
        'gestion': 'tipo_gestion',
        'dpto': 'departamento',
        'prov': 'provincia',
        'dist': 'distrito',
        'area': 'area_geografica'
    }
    
    df_institucion = df[list(cols_institucion.keys())].copy()
    df_institucion.rename(columns=cols_institucion, inplace=True)
    df_institucion.drop_duplicates(subset=['cod_mod'], inplace=True)

    # --- TABLA 2: tb_plana_docente ---
    cols_docentes = {
        'cod_mod': 'cod_mod',
        'Docentes_hombres': 'docentes_hombres',
        'Docentes_mujeres': 'docentes_mujeres',
        'Docentes_nombrados': 'condicion_nombrados',
        'Docentes_contratados': 'condicion_contratados',
        'Docentes_total': 'total_docentes',
        'Docentes_25_menos_años': 'edad_menos_25',
        'Docentes_2635_años': 'edad_26_35',
        'Docentes_3645_años': 'edad_36_45',
        'Docentes_4655_años': 'edad_46_55',
        'Docentes_5665_años': 'edad_56_65',
        'Docentes_66_a_mas_años': 'edad_mas_66'
    }

    df_docentes = df[list(cols_docentes.keys())].copy()
    df_docentes.rename(columns=cols_docentes, inplace=True)

    # ---------------------------------------------------------
    # 3. CARGA
    # ---------------------------------------------------------
    print("Cargando datos a PostgreSQL...")

    try:
        # Cargar tb_institucion
        df_institucion.to_sql('tb_institucion', db_connection, if_exists='replace', index=False)
        print(f" Tabla 'tb_institucion' creada exitosamente ({len(df_institucion)} registros).")

        # Cargar tb_plana_docente
        df_docentes.to_sql('tb_plana_docente', db_connection, if_exists='replace', index=False)
        print(f" Tabla 'tb_plana_docente' creada exitosamente ({len(df_docentes)} registros).")

        # Configurar PK usando text()
        with db_connection.connect() as con:
            print(" Configurando Primary Keys...")
            # Usamos text() para envolver el comando SQL crudo
            con.execute(text('ALTER TABLE tb_institucion ADD PRIMARY KEY (cod_mod);'))
            con.commit() # Confirmamos la transacción
            print(" Clave primaria configurada.")

    except Exception as e:
        print(f"   ❌ Error al escribir en la Base de Datos: {e}")

    print("Fin del proceso ETL.")

if __name__ == '__main__':
    run_etl()
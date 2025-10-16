#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

# Uso:
#   ./bench_single.sh <jar> <hdfs_input> <hdfs_output> [job_label]
# Ejemplos:
#   ./bench_single.sh Consulta1.jar /input_parcial /output_parcial/C1 C1
#   DRIVER_CLASS=tu.paquete.Main ./bench_single.sh ConsultaX.jar /in /out CX

if [ $# -lt 3 ]; then
  echo "Uso: $0 <jar> <hdfs_input> <hdfs_output> [job_label]"
  exit 1
fi

JAR_FILE="$1"
INPUT_PATH="$2"
OUTPUT_PATH="$3"
JOB_LABEL="${4:-$(basename "$JAR_FILE")}"

# === Directorio de resultados (por defecto: ./Resultados en el cwd) ===
RESULTS_DIR="${RESULTS_DIR:-$(pwd)/Resultados}"
mkdir -p "$RESULTS_DIR"

# CSV (personalizable con CSV_FILE env)
CSV_FILE="${CSV_FILE:-$RESULTS_DIR/mediciones_single.csv}"

# Log de esta corrida
LOG="$RESULTS_DIR/run_${JOB_LABEL}_$(date +%Y%m%d_%H%M%S).log"

echo "[bench] RESULTS_DIR=$RESULTS_DIR"
echo "[bench] CSV_FILE=$CSV_FILE"
echo "[bench] LOG=$LOG"

# Cabecera CSV si no existe
[ -f "$CSV_FILE" ] || echo "ts,job,elapsed_s,cpu_avg_pct,mem_avg_mb,app_id,stdout_log" > "$CSV_FILE"

# Limpiar output HDFS (si existe)
hdfs dfs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

# Limpieza extra opcional (p.ej. temporales de C4) pasando rutas separadas por espacio:
#   EXTRA_CLEAN="/user/sh1nj1/temp_crecimiento_1_suma /user/sh1nj1/temp_crecimiento_2_join" ./bench_single.sh ...
if [ -n "${EXTRA_CLEAN:-}" ]; then
  echo "[bench] EXTRA_CLEAN: $EXTRA_CLEAN"
  for p in $EXTRA_CLEAN; do
    hdfs dfs -rm -r -f "$p" >/dev/null 2>&1 || true
  done
fi

# ---- Función: sumar %CPU y RSS (KB) de TODAS las JVM Hadoop locales ----
sum_local_java() {
  # Capturamos PIDs sin romper por pipefail si no hay procesos
  mapfile -t p_lines < <(pgrep -fa java 2>/dev/null | egrep -i 'hadoop|mapred|yarn|Hadoop|MRAppMaster|YarnChild' || true)
  if [ ${#p_lines[@]} -eq 0 ]; then
    return 0
  fi
  for L in "${p_lines[@]}"; do
    pid=$(awk '{print $1}' <<< "$L")
    # pcpu=float, rss=KB (entero)
    ps -p "$pid" -o pcpu=,rss= --no-headers
  done
}

# ---- Ejecutar el job y medir ----
START=$(date +%s.%N)

# Si tu JAR define Main-Class en MANIFEST, basta con:
if [ -n "${DRIVER_CLASS:-}" ]; then
  ( hadoop jar "$JAR_FILE" "$DRIVER_CLASS" "$INPUT_PATH" "$OUTPUT_PATH" ) 2>&1 | tee "$LOG" &
else
  ( hadoop jar "$JAR_FILE" "$INPUT_PATH" "$OUTPUT_PATH" ) 2>&1 | tee "$LOG" &
fi
LAUNCH_PID=$!

CPU_SUM=0
RSS_SUM_KB=0
COUNT=0

# Muestreo a 1 Hz mientras el lanzador siga vivo
while kill -0 "$LAUNCH_PID" 2>/dev/null; do
  SAMPLE="$(sum_local_java || true)"
  if [ -n "$SAMPLE" ]; then
    cpu_iter=0
    rss_iter=0
    while read -r line; do
      [ -z "$line" ] && continue
      c=$(echo "$line" | awk '{print $1}')
      r=$(echo "$line" | awk '{print $2}')
      # valida números (c = float, r = entero)
      if echo "$c" | grep -Eq '^[0-9]+([.][0-9]+)?$' && echo "$r" | grep -Eq '^[0-9]+$'; then
        cpu_iter=$(awk -v a="$cpu_iter" -v b="$c" 'BEGIN{printf "%.2f", a+b}')
        rss_iter=$(awk -v a="$rss_iter" -v b="$r" 'BEGIN{printf "%.0f", a+b}')
      fi
    done <<< "$SAMPLE"
    CPU_SUM=$(awk -v a="$CPU_SUM" -v b="$cpu_iter" 'BEGIN{printf "%.2f", a+b}')
    RSS_SUM_KB=$(awk -v a="$RSS_SUM_KB" -v b="$rss_iter" 'BEGIN{printf "%.0f", a+b}')
    COUNT=$((COUNT+1))
  fi
  sleep 1
done

END=$(date +%s.%N)
ELAPSED=$(awk "BEGIN{printf \"%.2f\", $END-$START}")

# Promedios (si hubo al menos 1 muestra)
if [ $COUNT -gt 0 ]; then
  CPU_AVG=$(awk -v a="$CPU_SUM" -v n="$COUNT" 'BEGIN{printf "%.2f", a/n}')
  RSS_AVG_KB=$(awk -v a="$RSS_SUM_KB" -v n="$COUNT" 'BEGIN{printf "%.0f", a/n}')
else
  CPU_AVG="0.00"
  RSS_AVG_KB="0"
fi

# RSS promedio a MB
MEM_AVG_MB=$(awk -v kb="$RSS_AVG_KB" 'BEGIN{printf "%.2f", kb/1024}')

# applicationId (si YARN lo muestra en el log)
APP_ID=$(grep -Eo 'application_[0-9_]+' "$LOG" | tail -n1 || true)

# ---- Guardar resultados en CSV (siempre agrega fila) ----
echo "$(date +%F\ %T),$JOB_LABEL,$ELAPSED,$CPU_AVG,$MEM_AVG_MB,${APP_ID:-NA},$(basename "$LOG")" >> "$CSV_FILE"

# ---- Resumen ----
echo "OK single-node:"
echo "  Job:        $JOB_LABEL"
echo "  Elapsed:    $ELAPSED s"
echo "  CPU avg:    $CPU_AVG % (suma JVM locales)"
echo "  Mem avg:    $MEM_AVG_MB MB (RSS prom.)"
echo "  AppId:      ${APP_ID:-NA}"
echo "  CSV:        $CSV_FILE"
echo "  Log:        $LOG"


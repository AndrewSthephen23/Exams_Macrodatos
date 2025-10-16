#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

# Uso:
#   ./bench_cluster.sh <jar> <hdfs_input> <hdfs_output> [job_label]
# Ejemplos:
#   ./bench_cluster.sh Consulta1.jar /input_parcial /output_parcial/C1 C1
#   DRIVER_CLASS=tu.paquete.Main ./bench_cluster.sh ConsultaX.jar /in /out CX
#
# Opcional:
#   EXTRA_CLEAN="/ruta/hdfs/tmp1 /ruta/hdfs/tmp2" ./bench_cluster.sh ...

if [ $# -lt 3 ]; then
  echo "Uso: $0 <jar> <hdfs_input> <hdfs_output> [job_label]"
  exit 1
fi

JAR_FILE="$1"
INPUT_PATH="$2"
OUTPUT_PATH="$3"
JOB_LABEL="${4:-$(basename "$JAR_FILE")}"

# === Directorio de resultados (por defecto: ./ResultadosCluster en el cwd) ===
RESULTS_DIR="${RESULTS_DIR:-$(pwd)/ResultadosCluster}"
mkdir -p "$RESULTS_DIR"

# CSV (personalizable con CSV_FILE env)
# Columnas: ts,job,elapsed_s,app_id,final_status,tracking_url,vcoreSeconds,memorySeconds,mem_avg_mb,stdout_log
CSV_FILE="${CSV_FILE:-$RESULTS_DIR/mediciones_cluster.csv}"

# Log de esta corrida
LOG="$RESULTS_DIR/run_${JOB_LABEL}_$(date +%Y%m%d_%H%M%S).log"

echo "[bench-cluster] RESULTS_DIR=$RESULTS_DIR"
echo "[bench-cluster] CSV_FILE=$CSV_FILE"
echo "[bench-cluster] LOG=$LOG"

# Cabecera CSV si no existe
[ -f "$CSV_FILE" ] || echo "ts,job,elapsed_s,app_id,final_status,tracking_url,vcoreSeconds,memorySeconds,mem_avg_mb,stdout_log" > "$CSV_FILE"

# Limpiar output HDFS (si existe)
hdfs dfs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

# Limpieza extra opcional (temporales intermedios)
if [ -n "${EXTRA_CLEAN:-}" ]; then
  echo "[bench-cluster] EXTRA_CLEAN: $EXTRA_CLEAN"
  for p in $EXTRA_CLEAN; do
    hdfs dfs -rm -r -f "$p" >/dev/null 2>&1 || true
  done
fi

# ---- Ejecutar el job y medir wall clock ----
START=$(date +%s.%N)
if [ -n "${DRIVER_CLASS:-}" ]; then
  ( hadoop jar "$JAR_FILE" "$DRIVER_CLASS" "$INPUT_PATH" "$OUTPUT_PATH" ) 2>&1 | tee "$LOG"
else
  ( hadoop jar "$JAR_FILE" "$INPUT_PATH" "$OUTPUT_PATH" ) 2>&1 | tee "$LOG"
fi
END=$(date +%s.%N)
ELAPSED=$(awk "BEGIN{printf \"%.2f\", $END-$START}")

# ---- Extraer applicationId desde el log ----
APP_ID=$(grep -Eo 'application_[0-9_]+' "$LOG" | tail -n1 || true)

FINAL="NA"
TRACK="NA"
VCORE_S=""
MEM_S=""
MEM_AVG_MB=""

# ---- Intento 1: yarn application -status (si está disponible) ----
if [ -n "${APP_ID:-}" ]; then
  STATUS=$(yarn application -status "$APP_ID" 2>/dev/null || true)
  if [ -n "$STATUS" ]; then
    FINAL=$(echo "$STATUS" | awk -F: '/Final-State/ {sub(/^ /,"",$2); gsub(/\r/,"",$2); print $2}')
    TRACK=$(echo "$STATUS" | awk -F: '/Tracking-URL/ {sub(/^ /,"",$2); gsub(/\r/,"",$2); print $2}')
    # Algunas distros imprimen VCoreSeconds/MemorySeconds; otras no.
    VCORE_S=$(echo "$STATUS" | awk -F: '/VCoreSeconds/ {gsub(/ /,""); gsub(/\r/,""); print $2}')
    MEM_S=$(echo "$STATUS" | awk -F: '/MemorySeconds/ {gsub(/ /,""); gsub(/\r/,""); print $2}')
  fi
fi

# ---- Intento 2 (fallback): leer counters del log si no hay métricas YARN ----
# Ejemplos de líneas:
#   Total vcore-milliseconds taken by all map tasks=9875
#   Total vcore-milliseconds taken by all reduce tasks=3026
#   Total megabyte-milliseconds taken by all map tasks=10112000
#   Total megabyte-milliseconds taken by all reduce tasks=3098624
if [ -z "${VCORE_S:-}" ] || [ -z "${MEM_S:-}" ]; then
  vcm_map=$(grep -Eo 'Total vcore-milliseconds taken by all map tasks=[0-9]+' "$LOG" | awk -F= '{print $2}' | tail -n1)
  vcm_red=$(grep -Eo 'Total vcore-milliseconds taken by all reduce tasks=[0-9]+' "$LOG" | awk -F= '{print $2}' | tail -n1)
  mbm_map=$(grep -Eo 'Total megabyte-milliseconds taken by all map tasks=[0-9]+' "$LOG" | awk -F= '{print $2}' | tail -n1)
  mbm_red=$(grep -Eo 'Total megabyte-milliseconds taken by all reduce tasks=[0-9]+' "$LOG" | awk -F= '{print $2}' | tail -n1)

  vcm_map=${vcm_map:-0}; vcm_red=${vcm_red:-0}
  mbm_map=${mbm_map:-0}; mbm_red=${mbm_red:-0}

  # vcore-milliseconds → segundos
  vcore_ms_total=$(awk -v a="$vcm_map" -v b="$vcm_red" 'BEGIN{print a+b}')
  if echo "$vcore_ms_total" | grep -Eq '^[0-9]+$'; then
    VCORE_S=$(awk -v ms="$vcore_ms_total" 'BEGIN{printf "%.0f", ms/1000}')
  fi

  # megabyte-milliseconds → segundos·MB
  mb_ms_total=$(awk -v a="$mbm_map" -v b="$mbm_red" 'BEGIN{print a+b}')
  if echo "$mb_ms_total" | grep -Eq '^[0-9]+$'; then
    MEM_S=$(awk -v ms="$mb_ms_total" 'BEGIN{printf "%.0f", ms/1000}')
  fi
fi

# ---- Memoria promedio (MB) estimada por YARN ----
# si tenemos MemorySeconds (MB*seg) y elapsed (seg): mem_avg_mb = MEM_S / ELAPSED
if [ -n "${MEM_S:-}" ] && echo "$MEM_S" | grep -Eq '^[0-9]+$' && echo "$ELAPSED" | grep -Eq '^[0-9]+([.][0-9]+)?$'; then
  # Evita división por cero
  if awk "BEGIN {exit !($ELAPSED > 0)}"; then
    MEM_AVG_MB=$(awk -v ms="$MEM_S" -v t="$ELAPSED" 'BEGIN{printf "%.2f", ms/t}')
  else
    MEM_AVG_MB=""
  fi
fi

# ---- Append SIEMPRE una fila al CSV ----
echo "$(date +%F\ %T),$JOB_LABEL,$ELAPSED,${APP_ID:-NA},${FINAL:-NA},${TRACK:-NA},${VCORE_S:-},${MEM_S:-},${MEM_AVG_MB:-},$(basename "$LOG")" >> "$CSV_FILE"

# ---- Resumen ----
echo "OK cluster:"
echo "  Job:         $JOB_LABEL"
echo "  Elapsed:     $ELAPSED s"
echo "  AppId:       ${APP_ID:-NA}"
echo "  FinalState:  ${FINAL:-NA}"
echo "  Tracking:    ${TRACK:-NA}"
echo "  VCoreSeconds:${VCORE_S:-}"
echo "  MemorySeconds:${MEM_S:-}"
echo "  Mem avg MB:  ${MEM_AVG_MB:-}"
echo "  CSV:         $CSV_FILE"
echo "  Log:         $LOG"

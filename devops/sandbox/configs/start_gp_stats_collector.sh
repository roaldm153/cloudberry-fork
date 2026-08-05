#!/bin/bash
set -euo pipefail

if [ -f "/usr/local/cloudberry-db/cloudberry-env.sh" ]; then
  . /usr/local/cloudberry-db/cloudberry-env.sh
fi
export COORDINATOR_DATA_DIRECTORY="${COORDINATOR_DATA_DIRECTORY:-/data0/database/coordinator/gpseg-1}"
export USER=gpadmin

HN="$(hostname -s)"
if [[ "${HN}" != "cdw" ]]; then
    echo "[$(date)] ${HN}: не координатор, пропускаем настройку gp_stats_collector"
    exit 0
fi

echo "[$(date)] Applying gp_stats_collector configuration..."

# preload-параметр — требует полного рестарта
gpconfig -c shared_preload_libraries -v gp_stats_collector
gpstop -ar
sleep 10

psql -U gpadmin -d postgres -c "create extension if not exists gp_stats_collector;"
sleep 5

gpconfig -c gpsc.uds_path -v "'/tmp/yagpcc_agent_uds.sock'" --skipvalidation
gpconfig -c gpsc.enable   -v on                       --skipvalidation

gpconfig -s gpsc.enable
gpconfig -s gpsc.uds_path
grep gpsc /data0/database/coordinator/gpseg-1/postgresql.conf

gpstop -ar
sleep 10
psql -U gpadmin -d postgres -c "select name, setting, source from pg_settings where name like 'gpsc%';"

echo "[$(date)] gp_stats_collector configured successfully."

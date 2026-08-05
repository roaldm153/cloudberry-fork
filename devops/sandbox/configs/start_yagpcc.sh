#!/usr/bin/env bash
set -euo pipefail

RUNDIR=/home/gpadmin/yagpcc-run
LOGFILE=/home/gpadmin/yagpcc.log
HN="$(hostname -s)"

case "${HN}" in
  cdw)
    cp /etc/yagpcc/yagpcc_master.yaml "${RUNDIR}/yagpcc.yaml"
    ;;
  sdw1|sdw2)
    cp /etc/yagpcc/yagpcc_segment.yaml "${RUNDIR}/yagpcc.yaml"
    ;;
  scdw)
    echo "scdw: yagpcc не запускается до промоушена standby" >> "${LOGFILE}"
    exit 0
    ;;
  *)
    echo "Неизвестный hostname ${HN}, yagpcc не запускается" >> "${LOGFILE}"
    exit 0
    ;;
esac

cd "${RUNDIR}"
nohup /usr/local/cloudberry-db/bin/yagpcc --config-path "${RUNDIR}" \
    >> "${LOGFILE}" 2>&1 &
echo $! > "${RUNDIR}/yagpcc.pid"

socat TCP4-LISTEN:1441,fork,reuseaddr TCP6:[::1]:1441 &

# Both services are backgrounded above; return success so the CMD chain
# continues to start_gp_stats_collector.sh instead of blocking here.
exit 0

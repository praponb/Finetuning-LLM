SELECT id, service, host, log_ts, level, status_code, k, v FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, v;

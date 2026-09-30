SELECT id, host, log_ts, status_code, k, v FROM app_logs LATERAL VIEW EXPLODE(context) li AS k, v;

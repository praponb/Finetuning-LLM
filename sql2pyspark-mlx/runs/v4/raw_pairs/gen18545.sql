SELECT id, host, log_ts, level, k, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS k, value;

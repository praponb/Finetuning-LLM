SELECT id, service, host, log_ts, status_code, k, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value;

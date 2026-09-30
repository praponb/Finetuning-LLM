SELECT id, service, host, log_ts, level, k, attr_value FROM app_logs LATERAL VIEW EXPLODE(context) e AS k, attr_value;

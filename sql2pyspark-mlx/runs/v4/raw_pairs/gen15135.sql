SELECT id, host, log_ts, level, status_code, attr_key, value FROM app_logs LATERAL VIEW EXPLODE(context) t AS attr_key, value

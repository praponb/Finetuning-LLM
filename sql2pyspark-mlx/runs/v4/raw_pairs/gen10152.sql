SELECT id, log_ts, attr_key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value;

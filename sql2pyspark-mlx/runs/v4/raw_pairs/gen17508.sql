SELECT id, host, level, status_code, attr_key, attr_value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, attr_value

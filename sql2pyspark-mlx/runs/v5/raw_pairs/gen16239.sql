SELECT id, host, status_code, attr_key, value FROM app_logs LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
